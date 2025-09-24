import asyncio
import json
import mimetypes
import os
import shutil
import time
import tempfile
from typing import AsyncGenerator, Dict, List, Optional
from uuid import uuid4

import aiosqlite
from fastapi import (APIRouter, BackgroundTasks, Depends, File, HTTPException, Header,
                     Query, Request, Response, UploadFile)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from ConfigMedia import get_config
from EncryptionManager import decrypt_to_memory, decrypt
from Searcher import ReverseSearcher, get_searcher
from TaskManager import task_registry
from processing_status import status_manager
from server import rate_limiter_dependency
from src.reactions.ReactionRoutes import ReactionSummaryResponse
import logging
from src.db_utils import AsyncDBContext

# --- Project-specific Imports (Updated for new schema) ---
from src.media.media_repository import (update_instance_visibility_roles, manage_role_for_instances_by_tags, 
                                        get_cards_with_cursor, get_duplicates)
from src.models import FileVisibilityUpdateRequest, RoleBasicResponse, TagVisibilityUpdateRequest, UserProfile
from src.media.folder_manager import create_folder, delete_folder_if_empty, move_folder
from src.media.HelperMedia import (
    delete_instance_and_content_if_orphan, 
    get_instance_details,
)
from src.media.thumbnails import (
    create_image_thumbnail, create_pdf_thumbnail, create_text_thumbnail, create_video_thumbnail
)
from src.roles.permissions import Permission # type: ignore
from src.roles.rbac_manager import require_permission_from_profile
from src.users.auth_dependency import get_current_active_user
from src.websocket.HelperWebsocket import get_websocket_manager
from src.media.utilsVideo import stream_transcoded_video

# --- Global Instances and Config ---
config = get_config()
logger = logging.getLogger("MediaRoutes")
router = APIRouter(tags=["Media"])

# ==============================================================================
# 1. Pydantic Models (Updated for new schema)
# ==============================================================================

class FolderItem(BaseModel):
    """A single folder for UI display."""
    id: int
    name: str
    path: str

class FolderCreateRequest(BaseModel):
    """Request to create a new folder."""
    name: str = Field(..., min_length=1, description="The name of the new folder.")
    parent_id: Optional[int] = Field(None, description="The ID of the parent folder. If null, creates a root folder in the first watch directory.")

class FolderMoveRequest(BaseModel):
    """Request to move a folder."""
    new_parent_id: Optional[int] = Field(None, description="The ID of the new parent folder. If null, moves the folder to the root of the 'DEFAULT_CREATE_FOLDER'.")

class CardItem(BaseModel):
    """A single file instance card for UI display."""
    id: int # This is the instance_id
    display_name: str # This is the file_name from the instance
    file_type: str
    is_encrypted: bool
    file_hash: Optional[str] = None
    duration_seconds: Optional[float] = None
    original_created_at: Optional[str] = None

class CardListingResponse(BaseModel):
    """A flexible response for listing cards, which can include folders."""
    folders: Optional[List[FolderItem]] = None
    items: List[CardItem]
    next_cursor: Optional[str] = None

class DuplicatesResponse(BaseModel):
    hash_duplicates: List[List[CardItem]]
    similarity_duplicates: List[List[CardItem]]

class SceneResponse(BaseModel):
    id: int
    content_id: int # Belongs to content, not instance
    start_timecode: str
    end_timecode: str
    transcript: Optional[str] = None
    tags: List[str]

class UploaderResponse(BaseModel):
    id: int
    username: str

class InstanceDetailsResponse(BaseModel):
    # Instance specific fields
    instance_id: int = Field(..., alias='id') # The ID of this specific file copy
    file_name: str
    stored_path: str
    ingest_source: str
    original_created_at: Optional[str]
    original_modified_at: Optional[str]
    uploader: Optional[UploaderResponse] = None
    visibility_roles: List[RoleBasicResponse]
    folder_path: Optional[str] = None
    folder_id: Optional[int] = None
    folder_name: Optional[str] = None

    # Content specific fields
    content_id: int
    file_hash: str
    file_type: str
    extension: str
    phash: Optional[str] = None
    is_encrypted: bool
    is_webready: bool
    metadata: dict
    transcript: Optional[str] = None
    duration_seconds: Optional[float] = None
    size_in_bytes: Optional[int] = None
    tags: List[str]
    meta_tags: List[str]
    scenes: List[SceneResponse]
    reactions: List[ReactionSummaryResponse]

    class Config:
        validate_by_name = True

class GlobalVisibilityUpdateRequest(BaseModel):
    role_id: int

class ConversionRequest(BaseModel):
    """Request model for triggering a media conversion."""
    video_codec: Optional[str] = Field(None, description="Target video codec (e.g., 'hevc', 'av1').")
    image_format: Optional[str] = Field(None, description="Target image format (e.g., 'jpeg', 'png', 'webp').")


# ==============================================================================
# 2. Dependencies
# ==============================================================================

async def get_media_password(x_media_password: Optional[str] = Header(None)) -> Optional[str]:
    """Dependency to extract the optional media password from the header."""
    return x_media_password

async def _serve_ready_media(details: dict, password: Optional[str]):
    """
    Contains the logic to serve a media file that is already web-ready,
    handling transcoded versions and encryption.
    This is extracted from the `get_instance_media_file` endpoint to be reused.
    """
    # Determine the correct path to serve from. A transcoded path takes precedence.
    if transcoded_path := details.get('transcoded_path'):
        if os.path.exists(transcoded_path):
            # Transcoded files are always web-ready mp4
            return FileResponse(transcoded_path, media_type="video/mp4", headers={'Accept-Ranges': 'bytes'})
        else:
            logger.warning(f"Transcoded path {transcoded_path} for content {details['content_id']} not found on disk. Signaling caller to handle.")
            # Signal to the caller that the expected file is missing.
            # The caller (e.g., /stream endpoint) can then decide to live-transcode.
            return None

    stored_path = details['stored_path']
    if not os.path.exists(stored_path):
        # Instead of raising a 404, return None to let the caller handle the missing file.
        # This allows endpoints like /stream to fall back to live transcoding.
        logger.warning(f"Stored path {stored_path} for content {details['content_id']} not found on disk. Signaling caller to handle.")
        return None

    # For encrypted files, we decrypt to memory and serve as a Response object
    if details['is_encrypted']:
        if not password:
            raise HTTPException(status_code=401, detail="Password required.", headers={"WWW-Authenticate": "X-Media-Password"})

        decrypted_content = decrypt_to_memory(stored_path, password)
        if decrypted_content is None:
            raise HTTPException(status_code=401, detail="Invalid password.")

        media_type, _ = mimetypes.guess_type(details['file_name'])
        # Be more explicit for video types if mimetype guessing fails
        if not media_type and details.get('file_type') == 'video':
            media_type = 'video/mp4'
        return Response(content=decrypted_content, media_type=media_type or "application/octet-stream")

    # For non-encrypted, web-ready files, serve directly from disk
    media_type, _ = mimetypes.guess_type(stored_path)
    if not media_type and details.get('file_type') == 'video':
        media_type = 'video/mp4'
    return FileResponse(stored_path, media_type=media_type or "application/octet-stream", headers={'Accept-Ranges': 'bytes'})

def _broadcast_file_not_found(instance_id: int):
    """
    Schedules a broadcast for a file_not_found error to clients viewing the instance.
    This is a fire-and-forget task.
    """
    manager = get_websocket_manager()
    room_name = f"instance-viewers:{instance_id}"
    message = {
        "type": "file_error",
        "payload": {
            "file_id": instance_id,
            "error": "file_not_found",
            "message": "The requested media file was not found on disk. It may have been moved or deleted."
        }
    }
    asyncio.create_task(manager.broadcast_to_room(room_name, message))

# ==============================================================================
# 3. API Routes: Search and Discovery
# ==============================================================================

@router.get("/api/cards", response_model=CardListingResponse)
@require_permission_from_profile(Permission.FILE_SEARCH)
async def search_files_with_cursor(
    current_user: UserProfile = Depends(get_current_active_user),
    search: Optional[str] = Query(None),
    scene_search: Optional[str] = Query(None, description="Search for text within video scene transcripts."),
    filename: Optional[str] = Query(None),
    tags: Optional[str] = Query(None),
    exclude_tags: Optional[str] = Query(None),
    meta_tags: Optional[str] = Query(None),
    exclude_meta_tags: Optional[str] = Query(None),
    file_type: Optional[List[str]] = Query(None),
    is_encrypted: Optional[bool] = Query(None),
    duration_min: Optional[int] = Query(None, gt=0),
    duration_max: Optional[int] = Query(None, gt=0),
    folder_id: Optional[int] = Query(None),
    mode: str = Query("expanded", description="View mode for listing files and folders.", pattern="^(directory|expanded|recursive)$"),
    page_size: int = Query(24, gt=0, le=100),
    cursor: Optional[str] = Query(None)
):
    """
    Search for file instances with advanced filters and cursor-based pagination.
    Supports different view modes:
    - `directory`: Lists sub-folders and direct files within the given `folder_id`.
    - `expanded`: (Default) Shows a flat list of all files matching the search, ignoring folder structure.
    - `recursive`: Shows a flat list of all files within the given `folder_id` and all its sub-folders.
    """
    # This helper function must be updated internally to query the new schema
    results = await get_cards_with_cursor(
        user_id=current_user.id, search=search, scene_search=scene_search,
        filename=filename, tags=tags, exclude_tags=exclude_tags,
        meta_tags=meta_tags, exclude_meta_tags=exclude_meta_tags,
        file_type=file_type, is_encrypted=is_encrypted,
        duration_min=duration_min, duration_max=duration_max, folder_id=folder_id,
        mode=mode,
        page_size=page_size, cursor=cursor
    )
    return results

@router.post("/api/search/image", status_code=200)
@require_permission_from_profile(Permission.FILE_SEARCH)
async def search_by_image(
    current_user: UserProfile = Depends(get_current_active_user),
    file: UploadFile = File(...),
    top_k: int = Query(5, gt=0, le=50),
    searcher: ReverseSearcher = Depends(get_searcher)
):
    """Accepts an image upload and performs a reverse image search."""
    # This logic remains the same as it operates on the search index
    # The search index should return instance_ids
    temp_dir = tempfile.mkdtemp()
    try:
        temp_file_path = os.path.join(temp_dir, file.filename)
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        search_output = await searcher.search(image_path=temp_file_path, top_k=top_k)
        if search_output.get("status") == "error":
            raise HTTPException(status_code=400, detail=search_output.get("message"))
        return search_output
    finally:
        if os.path.exists(temp_dir): shutil.rmtree(temp_dir)

@router.get("/api/duplicates", response_model=DuplicatesResponse)
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_duplicates_route(
    current_user: UserProfile = Depends(get_current_active_user),
    duplicate_type: str = Query("all", pattern="^(all|hash|similarity)$"),
    file_type: Optional[List[str]] = Query(None),
):
    """Finds and returns groups of duplicate or similar file instances."""
    # This helper function must be updated internally to query the new schema
    hash_duplicates, similarity_duplicates = await get_duplicates(
        user_id=current_user.id, duplicate_type=duplicate_type, file_type=file_type
    )
    return {"hash_duplicates": hash_duplicates, "similarity_duplicates": similarity_duplicates}

# ==============================================================================
# 4. API Routes: File Management and Metadata
# ==============================================================================

@router.post("/api/folders", response_model=FolderItem, status_code=201)
@require_permission_from_profile(Permission.FILE_EDIT)
async def create_folder_route(
    request: FolderCreateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Creates a new folder. If a `parent_id` is provided, the folder is created
    inside the parent. If not, it's created in the primary watch directory.
    Requires 'file:edit' permission.
    """
    try:
        new_folder = await create_folder(name=request.name, parent_id=request.parent_id)
        return new_folder
    except ValueError as e:
        # Name conflict, invalid name, or parent not found
        raise HTTPException(status_code=409, detail=str(e))
    except (RuntimeError, IOError) as e:
        # Config error or filesystem error
        logger.error(f"Folder creation failed due to system error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/api/folders/{folder_id}/move", response_model=FolderItem)
@require_permission_from_profile(Permission.FILE_EDIT)
async def move_folder_route(
    folder_id: int,
    request: FolderMoveRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Moves a folder to a new parent directory.
    The destination must be within the 'DEFAULT_CREATE_FOLDER' or its subdirectories.
    This operation physically moves the folder and all its contents on the filesystem
    and updates all corresponding database records.
    Requires 'file:edit' permission.
    """
    try:
        moved_folder = await move_folder(folder_id=folder_id, new_parent_id=request.new_parent_id)
        return moved_folder
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except (RuntimeError, IOError, aiosqlite.Error) as e:
        logger.error(f"Folder move failed for ID {folder_id} due to system error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/api/folders/{folder_id}", status_code=204)
@require_permission_from_profile(Permission.FILE_EDIT)
async def delete_folder_route(
    folder_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Deletes a folder, but only if it is completely empty (no sub-folders or files).
    Requires 'file:edit' permission.
    """
    try:
        success = await delete_folder_if_empty(folder_id=folder_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Folder with ID {folder_id} not found or already deleted.")
        # On success, FastAPI automatically returns a 204 No Content response.
        return
    except ValueError as e:
        # Covers "not found", "not empty", and "protected directory" cases.
        raise HTTPException(status_code=409, detail=str(e))
    except (IOError, aiosqlite.Error) as e:
        logger.error(f"Folder deletion failed for ID {folder_id} due to system error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/instance/{instance_id}", response_model=InstanceDetailsResponse)
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_instance_by_id(instance_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """Retrieve the full details for a single file instance."""
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if details is None:
        raise HTTPException(status_code=404, detail=f"No instance found with ID: {instance_id} or access denied.")
    # The 'metadata' field is a JSON string in DB, parse it for the response model
    details['metadata'] = json.loads(details['metadata']) if isinstance(details.get('metadata'), str) else details.get('metadata', {})
    return details

@router.delete("/api/instance/{instance_id}", status_code=200)
@require_permission_from_profile(Permission.FILE_DELETE)
async def delete_instance_by_id(instance_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """
    Deletes a file instance. If it's the last instance for its content, the content
    and physical files (stored file, thumbnail) are also deleted.
    """
    success = await delete_instance_and_content_if_orphan(instance_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete instance. Check logs for details.")
    return {"status": "success", "message": f"Instance {instance_id} has been deleted."}

@router.post("/api/files/upload", status_code=202)
@require_permission_from_profile(Permission.FILE_ADD)
async def upload_file_for_processing(request: Request, file: UploadFile = File(...), current_user: UserProfile = Depends(get_current_active_user)):
    """Accepts a file upload and queues it for processing."""
    scanner_app_state = request.app.state.scanner_app_state
    if not scanner_app_state or 'intake_queue' not in scanner_app_state:
        raise HTTPException(status_code=503, detail="Scanner service is not available.")

    upload_dir = os.path.join(config.get("FILESTORE_DIR"), "_incoming")
    os.makedirs(upload_dir, exist_ok=True)
    
    temp_filename = str(uuid4())
    file_path = os.path.join(upload_dir, temp_filename)

    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    ingest_info = (file_path, "upload", current_user.id, file.filename)
    await scanner_app_state['intake_queue'].put(ingest_info)

    return {"status": "queued", "detail": f"File '{file.filename}' has been queued."}

# ==============================================================================
# 5. API Routes: Visibility Management
# ==============================================================================

@router.put("/api/instance/{instance_id}/visibility", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def set_instance_visibility(instance_id: int, request: FileVisibilityUpdateRequest, current_user: UserProfile = Depends(get_current_active_user)):
    """Sets (overwrites) the visibility roles for a specific file instance."""
    # This repository function must now operate on `instance_visibility_roles`
    await update_instance_visibility_roles(instance_id, request.role_ids)
    return {"status": "success", "message": f"Visibility for instance {instance_id} updated."}

@router.post("/api/instances/visibility/global", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def add_role_globally(request: GlobalVisibilityUpdateRequest, current_user: UserProfile = Depends(get_current_active_user)):
    """Adds a role to the visibility list of ALL file instances."""
    query = """
        INSERT OR IGNORE INTO instance_visibility_roles (instance_id, role_id)
        SELECT id, ? FROM file_instances
    """
    async with AsyncDBContext() as db:
        cursor = await db.execute(query, (request.role_id,))
        await db.commit()
    
    return {"status": "success", "message": f"Role {request.role_id} added globally to {cursor.rowcount} instances."}


@router.delete("/api/instances/visibility/global", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def remove_role_globally(request: GlobalVisibilityUpdateRequest, current_user: UserProfile = Depends(get_current_active_user)):
    """Removes a role from the visibility list of ALL file instances."""
    query = "DELETE FROM instance_visibility_roles WHERE role_id = ?"
    async with AsyncDBContext() as db:
        cursor = await db.execute(query, (request.role_id,))
        await db.commit()

    return {"status": "success", "message": f"Role {request.role_id} removed globally from {cursor.rowcount} instances."}


@router.post("/api/instances/visibility/by-tag", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def add_role_to_instances_by_tag(request: TagVisibilityUpdateRequest, current_user: UserProfile = Depends(get_current_active_user)):
    """Adds a single role to the visibility list of all instances whose content matches the given tags."""
    # This repository function must be updated to find instances via content tags
    await manage_role_for_instances_by_tags(
        tags=request.tags, role_id=request.role_id, tag_type=request.tag_type, action='add'
    )
    return {"status": "success", "message": f"Role added to instances matching tags."}

@router.delete("/api/instances/visibility/by-tag", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def remove_role_from_instances_by_tag(request: TagVisibilityUpdateRequest, current_user: UserProfile = Depends(get_current_active_user)):
    """Removes a single role from the visibility list of all instances whose content matches the given tags."""
    await manage_role_for_instances_by_tags(
        tags=request.tags, role_id=request.role_id, tag_type=request.tag_type, action='remove'
    )
    return {"status": "success", "message": f"Role removed from instances matching tags."}

# ==============================================================================
# 6. API Routes: Media and Thumbnail Serving
# ==============================================================================

@router.get("/api/instance/{instance_id}/thumbnail")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_instance_thumbnail(instance_id: int, current_user: UserProfile = Depends(get_current_active_user), password: Optional[str] = Depends(get_media_password)):
    """Serves a pre-generated thumbnail for a file instance."""
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details:
        raise HTTPException(status_code=404, detail="Instance not found or access denied.")

    if details['is_encrypted'] and not password:
        raise HTTPException(status_code=401, detail="Password required for encrypted content.", headers={"WWW-Authenticate": "X-Media-Password"})

    # The thumbnail is named by content hash, not instance id.
    thumb_path = os.path.join(config['THUMBNAILS_DIR'], f"{details['file_hash']}.{config['THUMBNAIL_FORMAT'].lower()}")
    if not os.path.exists(thumb_path):
        logger.info(f"Thumbnail for instance {instance_id} (hash: {details['file_hash'][:10]}) not found. Generating on-the-fly.")
        
        source_path = details['stored_path']
        temp_decrypted_path = None
        
        try:
            if details['is_encrypted']:
                # Decrypt to a temporary file
                temp_fd, temp_decrypted_path = tempfile.mkstemp(suffix=details.get('extension'))
                os.close(temp_fd)
                
                # Use asyncio.to_thread for the blocking decrypt function
                success = await asyncio.to_thread(decrypt, source_path, temp_decrypted_path, password)
                if not success:
                    raise HTTPException(status_code=401, detail="Invalid password or corrupted file.")
                
                source_path_for_thumb = temp_decrypted_path
            else:
                source_path_for_thumb = source_path

            # Map file type to thumbnail creation function
            thumb_creators = {
                "image": create_image_thumbnail,
                "video": create_video_thumbnail,
                "pdf": create_pdf_thumbnail,
                "text": create_text_thumbnail,
            }
            creator_func = thumb_creators.get(details['file_type'])
            
            if not creator_func:
                raise HTTPException(status_code=404, detail=f"Cannot generate thumbnail for file type '{details['file_type']}'.")

            created = await asyncio.get_running_loop().run_in_executor(None, creator_func, source_path_for_thumb, thumb_path)

            if not created:
                raise HTTPException(status_code=500, detail="Failed to generate thumbnail on-the-fly.")
        finally:
            if temp_decrypted_path and os.path.exists(temp_decrypted_path):
                os.remove(temp_decrypted_path)
    
    return FileResponse(thumb_path)

@router.get("/api/scene/{scene_id}/thumbnail")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_scene_thumbnail(
    scene_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """
    Serves a thumbnail for a video scene. If the thumbnail doesn't exist,
    it is generated on-the-fly and cached.
    """
    from src.db_utils import AsyncDBContext, execute_db_query
    from src.media.thumbnails import create_video_frame_thumbnail
    from EncryptionManager import decrypt

    # New query to get all required data and check permissions in one go.
    query = """
        SELECT 
            fi.stored_path,
            fc.is_encrypted,
            fc.extension,
            s.start_timecode
        FROM scenes s
        JOIN file_content fc ON s.content_id = fc.id
        JOIN file_instances fi ON fc.id = fi.content_id
        JOIN instance_visibility_roles ivr ON fi.id = ivr.instance_id
        JOIN user_roles ur ON ivr.role_id = ur.role_id
        WHERE s.id = ? AND ur.user_id = ?
        LIMIT 1
    """
    async with AsyncDBContext() as db_conn:
        scene_info = await execute_db_query(db_conn, query, (scene_id, current_user.id), fetch_one=True)

    if not scene_info:
        raise HTTPException(status_code=404, detail="Scene not found or access denied.")

    if scene_info['is_encrypted'] and not password:
        raise HTTPException(status_code=401, detail="Password required for encrypted content.", headers={"WWW-Authenticate": "X-Media-Password"})

    thumb_format = config.get('THUMBNAIL_FORMAT', 'WEBP').lower()
    thumb_path = os.path.join(config['THUMBNAILS_DIR'], f"scene_{scene_id}.{thumb_format}")
    
    # If thumbnail already exists, serve it.
    if os.path.exists(thumb_path):
        return FileResponse(thumb_path)

    # --- Thumbnail Generation Logic ---
    logger.info(f"Generating thumbnail on-the-fly for scene {scene_id}.")
    
    source_video_path = scene_info['stored_path']
    temp_decrypted_path = None

    try:
        if scene_info['is_encrypted']:
            temp_fd, temp_decrypted_path = tempfile.mkstemp(suffix=scene_info.get('extension'))
            os.close(temp_fd)
            
            success = await asyncio.to_thread(decrypt, source_video_path, temp_decrypted_path, password)
            if not success:
                raise HTTPException(status_code=401, detail="Invalid password or corrupted file.")
            
            source_video_path = temp_decrypted_path

        # Run synchronous thumbnail creation in a thread pool executor
        created = await asyncio.get_running_loop().run_in_executor(
            None, create_video_frame_thumbnail, source_video_path, thumb_path, scene_info['start_timecode']
        )

        if not created:
            raise HTTPException(status_code=500, detail="Failed to generate scene thumbnail.")

        return FileResponse(thumb_path)
    finally:
        if temp_decrypted_path and os.path.exists(temp_decrypted_path):
            os.remove(temp_decrypted_path)

@router.get("/api/instance/{instance_id}/media", summary="Serve a web-ready media file")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_instance_media_file(
    instance_id: int,
    request: Request,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """
    Serves the media file directly from disk. This endpoint should be used for all
    non-video files. For videos, it delegates to the streaming logic to robustly
    handle all cases (ready, not ready, missing files).
    """
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details:
        raise HTTPException(status_code=404, detail="Instance not found or access denied.")

    # For videos, always delegate to the streaming endpoint's logic.
    # This consolidates video serving, handles missing files gracefully by falling
    # back to live transcoding, and triggers background caching.
    if details['file_type'] == 'video':
        return await stream_instance_media_file(instance_id, request, current_user, password)

    # For all other file types (images, audio, etc.), serve them directly.
    response = await _serve_ready_media(details, password)
    if response is None:
        _broadcast_file_not_found(instance_id)
        # This case is now only reachable for non-video files if their path is missing.
        raise HTTPException(status_code=404, detail="The requested media file was not found on disk.")
    return response


@router.get("/api/instance/{instance_id}/download", summary="Download the raw media file", dependencies=[Depends(rate_limiter_dependency(times=1, seconds=60))])
@require_permission_from_profile(Permission.FILE_VIEW)
async def download_instance_media_file(
    instance_id: int, 
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """
    Serves the media file as an attachment, forcing a download.
    Handles encrypted files if the correct password is provided.
    """
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details:
        raise HTTPException(status_code=404, detail="Instance not found or access denied.")

    stored_path = details['stored_path']
    if not os.path.exists(stored_path):
        _broadcast_file_not_found(instance_id)
        raise HTTPException(status_code=404, detail="Media file not found on disk.")

    filename = details['file_name']
    media_type, _ = mimetypes.guess_type(filename)

    if details['is_encrypted']:
        if not password:
            raise HTTPException(status_code=401, detail="Password required.", headers={"WWW-Authenticate": "X-Media-Password"})
        
        decrypted_content = decrypt_to_memory(stored_path, password)
        if decrypted_content is None:
            raise HTTPException(status_code=401, detail="Invalid password.")
        
        headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
        return Response(content=decrypted_content, media_type=media_type or "application/octet-stream", headers=headers)

    return FileResponse(stored_path, media_type=media_type or "application/octet-stream", filename=filename)

@router.get("/api/instance/{instance_id}/stream", summary="Live-transcode and stream a video")
@require_permission_from_profile(Permission.FILE_VIEW)
async def stream_instance_media_file(
    instance_id: int,
    request: Request,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """
    Serves non-web-ready videos by transcoding them on-the-fly.
    If the video is already web-ready, it serves it directly, behaving like the /media endpoint.
    Handles encrypted files by decrypting to a temporary file before streaming.
    """
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details or not os.path.exists(details['stored_path']):
        _broadcast_file_not_found(instance_id)
        raise HTTPException(status_code=404, detail="Instance not found or access denied.")
    
    if details['file_type'] != 'video':
        raise HTTPException(status_code=400, detail="Streaming is only supported for video files.")

    # Check if the video is already in a servable format (original is web-ready or a transcoded version exists)
    is_ready = details.get('is_webready', False)
    has_transcoded_path_in_db = details.get('transcoded_path')

    if is_ready or has_transcoded_path_in_db:
        logger.debug(f"Video instance {instance_id} is potentially web-ready. Attempting to serve directly.")
        ready_media_response = await _serve_ready_media(details, password)

        # If _serve_ready_media returns a valid response, serve it.
        if ready_media_response:
            return ready_media_response

        # If it returns None, it means a transcoded file was expected but missing.
        # We log this and fall through to the live transcoding logic.
        logger.warning(f"Web-ready file for instance {instance_id} was not found on disk. Falling back to live transcoding.")

        # The file was supposed to be ready, but it's not on disk. Mark it as not ready in the DB.
        # This prevents future requests from hitting this broken path.
        from src.db_utils import execute_db_query
        async with AsyncDBContext() as db:
            # If a transcoded path was the problem, nullify it and mark as not web-ready.
            if has_transcoded_path_in_db:
                logger.info(f"Nullifying missing transcoded_path for content ID {details['content_id']}.")
                await execute_db_query(
                    db, "UPDATE file_content SET transcoded_path = NULL, is_webready = 0 WHERE id = ?", (details['content_id'],)
                )
            # If the original file was the problem, just mark it as not web-ready.
            elif is_ready:
                logger.info(f"Marking content ID {details['content_id']} as not web-ready due to missing original file.")
                await execute_db_query(
                    db, "UPDATE file_content SET is_webready = 0 WHERE id = ?", (details['content_id'],)
                )

    # --- Auto-cache by scheduling a background conversion ---
    from src.media.video_cache_task import schedule_webready_transcode
    scanner_app_state = request.app.state.scanner_app_state
    if not scanner_app_state:
        logger.warning(f"Scanner service not available. Skipping background conversion for instance {instance_id}.")
    else:
        logger.info(f"Triggering background conversion for instance ID {instance_id} from /stream request.")
        # This runs in the background. The scheduler handles deduplication at the content level.
        asyncio.create_task(
            schedule_webready_transcode(
                instance_id=instance_id,
                content_id=details['content_id'],
                source_path=details['stored_path']
            )
        )
    
    # If not ready, or if the ready file was missing, proceed with on-the-fly transcoding.
    logger.debug(f"Video instance {instance_id} is not web-ready or file is missing. Live-transcoding from /stream endpoint.")
    streamer = stream_transcoded_video(
        source_path=details['stored_path'], 
        password=password if details['is_encrypted'] else None,
        details=details
    )
    return StreamingResponse(streamer, media_type="video/mp4")

@router.get("/api/instance/{instance_id}/view", summary="Stream a viewable file, like text or PDF")
@require_permission_from_profile(Permission.FILE_VIEW)
async def view_instance_file(
    instance_id: int,
    background_tasks: BackgroundTasks,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """
    Streams large, viewable files like text or PDFs, handling decryption efficiently.

    For non-encrypted files, it streams directly from disk. For encrypted files,
    it decrypts to a temporary file and streams from there, cleaning up afterwards.
    This avoids loading large encrypted files entirely into memory.
    """
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details:
        raise HTTPException(status_code=404, detail="Instance not found or access denied.")
    if details['file_type'] not in ['text', 'pdf']:
        raise HTTPException(
            status_code=400,
            detail=f"This endpoint is for streaming viewable files (e.g., text, pdf). Use the /media or /stream endpoint for type '{details['file_type']}'."
        )
    stored_path = details['stored_path']
    if not os.path.exists(stored_path):
        _broadcast_file_not_found(instance_id)
        raise HTTPException(status_code=404, detail="Media file not found on disk.")

    media_type, _ = mimetypes.guess_type(details['file_name'])
    if not media_type or media_type == "application/octet-stream":
        media_type = "text/plain" if details['file_type'] == 'text' else "application/pdf"

    if not details['is_encrypted']:
        return FileResponse(stored_path, media_type=media_type, headers={'Accept-Ranges': 'bytes'})

    if not password:
        raise HTTPException(status_code=401, detail="Password required for encrypted file.", headers={"WWW-Authenticate": "X-Media-Password"})

    from EncryptionManager import decrypt
    temp_fd, temp_path = tempfile.mkstemp(suffix=details.get('extension'))
    os.close(temp_fd)

    try:
        success = await asyncio.to_thread(decrypt, stored_path, temp_path, password)
        if not success:
            raise HTTPException(status_code=401, detail="Invalid password or corrupted file.")

        background_tasks.add_task(os.remove, temp_path)
        return FileResponse(temp_path, media_type=media_type, headers={'Accept-Ranges': 'bytes'})
    except Exception as e:
        if os.path.exists(temp_path): os.remove(temp_path)
        if isinstance(e, HTTPException): raise e
        logger.error(f"Unexpected error during file view for instance {instance_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error during file streaming.")

# ==============================================================================
# 7. API Routes: Subtitles
# ==============================================================================

@router.get("/api/instance/{instance_id}/subtitles", summary="List available subtitle tracks")
@require_permission_from_profile(Permission.FILE_VIEW)
async def list_subtitles(
    instance_id: int,
    background_tasks: BackgroundTasks,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """Inspects a media file for subtitles. Requires password for encrypted files."""
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details or not os.path.exists(details['stored_path']):
        raise HTTPException(status_code=404, detail="Instance not found or access denied.")

    source_path = details['stored_path']

    if details['is_encrypted']:
        if not password:
            raise HTTPException(status_code=401, detail="Password required to scan for subtitles.", headers={"WWW-Authenticate": "X-Media-Password"})

        from EncryptionManager import decrypt
        temp_fd, temp_path = tempfile.mkstemp(suffix=details.get('extension'))
        os.close(temp_fd)

        try:
            success = await asyncio.to_thread(decrypt, source_path, temp_path, password)
            if not success:
                raise HTTPException(status_code=401, detail="Invalid password or corrupted file.")

            source_path = temp_path
            background_tasks.add_task(os.remove, temp_path)
        except Exception as e:
            if os.path.exists(temp_path): os.remove(temp_path)
            if isinstance(e, HTTPException): raise e
            logger.error(f"Unexpected error during subtitle list for instance {instance_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error during subtitle processing.")

    from src.media.utilsVideo import get_subtitle_streams
    subtitle_list = await get_subtitle_streams(source_path)
    return JSONResponse(content=subtitle_list)

async def _vtt_generator(source_path: str, stream_index: int) -> AsyncGenerator[bytes, None]:
    """An async generator that streams a subtitle track as VTT using ffmpeg."""
    command = ['ffmpeg', '-i', source_path, '-map', f'0:{stream_index}', '-f', 'webvtt', '-y', 'pipe:1']
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL
    )
    try:
        while chunk := await proc.stdout.read(1024):
            yield chunk
        await proc.wait()
    finally:
        if proc.returncode is None:
            proc.kill()

@router.get("/api/instance/{instance_id}/subtitle/{stream_index}", summary="Serve a subtitle track as VTT")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_subtitle_vtt(
    instance_id: int,
    stream_index: int,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """Streams a subtitle track converted to VTT format."""
    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details or not os.path.exists(details['stored_path']):
        raise HTTPException(status_code=404, detail="Instance not found or access denied.")

    source_path = details['stored_path']
    temp_path_to_clean = None

    # This wrapper ensures the temp file is cleaned up even if the client disconnects
    async def cleanup_wrapper(generator, path_to_clean):
        try:
            async for chunk in generator:
                yield chunk
        finally:
            if path_to_clean and os.path.exists(path_to_clean):
                await asyncio.to_thread(os.remove, path_to_clean)

    if details['is_encrypted']:
        if not password:
            raise HTTPException(status_code=401, detail="A password is required.")

        from EncryptionManager import decrypt
        temp_fd, temp_path = tempfile.mkstemp(suffix=details.get('extension'))
        os.close(temp_fd)

        try:
            success = await asyncio.to_thread(decrypt, source_path, temp_path, password)
            if not success:
                raise HTTPException(status_code=401, detail="Invalid password or corrupted file.")

            source_path = temp_path
            temp_path_to_clean = temp_path
        except Exception as e:
            if os.path.exists(temp_path): os.remove(temp_path)
            if isinstance(e, HTTPException): raise e
            logger.error(f"Unexpected error during subtitle stream for instance {instance_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error during subtitle streaming.")

    vtt_streamer = _vtt_generator(source_path, stream_index)
    return StreamingResponse(cleanup_wrapper(vtt_streamer, temp_path_to_clean), media_type="text/vtt")


# ==============================================================================
# 8. API Routes: Conversion and System Status
# ==============================================================================

@router.post("/api/instance/{instance_id}/convert", status_code=202)
@require_permission_from_profile(Permission.FILE_EDIT)
async def request_media_conversion(
    instance_id: int,
    request: Request,
    conversion_request: ConversionRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Triggers a background task to convert a media file to a different format or codec.
    The resulting file will be ingested as a new instance.
    """
    from src.media.video_convert_task import schedule_media_conversion

    current_status = status_manager.get_status(instance_id)
    if current_status and current_status.get("ingest_source") == 'conversion' and current_status.get("status") not in ["completed", "error"]:
        raise HTTPException(status_code=409, detail="A conversion for this file is already in progress or queued.")

    details = await get_instance_details(instance_id, user_id=current_user.id)
    if not details:
        raise HTTPException(status_code=404, detail="File not found or access denied.")

    file_type = details['file_type']
    if file_type not in ['video', 'image']:
        raise HTTPException(status_code=400, detail=f"Conversion is not supported for file type '{file_type}'.")

    if file_type == 'video' and not conversion_request.video_codec:
        raise HTTPException(status_code=400, detail="A 'video_codec' must be specified for video conversion.")
    if file_type == 'image' and not conversion_request.image_format:
        raise HTTPException(status_code=400, detail="An 'image_format' must be specified for image conversion.")

    scanner_app_state = request.app.state.scanner_app_state
    if not scanner_app_state:
        raise HTTPException(status_code=503, detail="Scanner service is not available to handle the converted file.")

    await schedule_media_conversion(
        instance_id=instance_id,
        source_path=details['stored_path'],
        original_filename=details['file_name'],
        file_type=file_type,
        conversion_options=conversion_request.dict(),
        scanner_app_state=scanner_app_state,
        user_id=current_user.id,
        task_type='conversion'
    )
    
    return {"status": "success", "message": "Media conversion has been scheduled."}

@router.get("/api/instance/{instance_id}/status")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_instance_status(instance_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """
    Checks the processing status of a file instance (e.g., conversion, re-tagging).
    If a task is active, returns its status. Otherwise, returns a 'ready' state.
    """
    # status_manager is the single source of truth for active tasks.
    task_status = status_manager.get_status(instance_id)
    if task_status:
        return task_status
    # If no task is active for this instance, it's ready for a new one.
    return {"status": "ready", "progress": 100, "message": "Ready for new tasks."}

@router.get("/api/scanner/status")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_scanner_status(request: Request, current_user: UserProfile = Depends(get_current_active_user)):
    """
    Provides a detailed status of the background scanner service for the UI. This now includes
    ingestion tasks, conversion tasks, and re-tagging tasks.
    """
    app_state = getattr(request.app.state, 'scanner_app_state', None)

    if not app_state or not app_state.get("config"):
        raise HTTPException(status_code=503, detail="Scanner service is not available or still initializing.")

    currently_processing_with_progress = []
    now = time.time()

    # This assumes status_manager has a method to get all keys for active tasks.
    # This is a more robust way to capture all background work, not just file-path-based ingestion.
    # HACK: Accessing a presumed internal attribute because a public method is not available.
    # The ideal fix is to add a `get_all_active_keys()` method to the ProcessingStatusManager class.
    all_active_keys = list(getattr(status_manager, '_statuses', {}).keys())

    for key in all_active_keys:
        progress_info = status_manager.get_status(key)
        if not progress_info: continue

        start_time = progress_info.get('start_time')
        elapsed = (now - start_time) if start_time else 0
        progress = progress_info.get('progress', 0)
        eta = ((elapsed / progress) * (100 - progress)) if progress > 0 and elapsed > 1 else None

        # Use the filename stored in the status, falling back to deriving from the key
        display_name = progress_info.get('filename') or (os.path.basename(key) if isinstance(key, str) else f"Task for Instance {key}")

        currently_processing_with_progress.append({
            "path": display_name,
            "progress": progress,
            "message": progress_info.get('message', 'Waiting...'),
            "elapsed": elapsed,
            "eta": eta,
            "ingest_source": progress_info.get('ingest_source', 'unknown'),
            "priority": progress_info.get('priority', 'NORMAL'),
        })
    
    # Sort by elapsed time, descending, so longest-running tasks are at the top
    currently_processing_with_progress.sort(key=lambda x: x['elapsed'], reverse=True)

    worker_pools_status = {}
    for pool_name, pool in task_registry.get_pools().items():
        worker_pools_status[pool_name] = {
            "workers": pool.num_workers,
            "queue_size": pool.queue.qsize(),
            "active_tasks": pool.get_active_task_count(),
        }

    scanner = app_state.get("scanner")
    watched_dirs = scanner.watch_dirs if scanner and hasattr(scanner, 'watch_dirs') else []

    status_payload = {
        "processing_enabled": app_state.get("processing_enabled_event").is_set(),
        "intake_queue_size": app_state.get('intake_queue').qsize(),
        "currently_processing_count": len(currently_processing_with_progress),
        "currently_processing": currently_processing_with_progress,
        "files_processed_session": app_state.get('files_processed_session', 0),
        "watched_directories": list(watched_dirs),
        "worker_pools": worker_pools_status,
        "config": {
            "clip_model": app_state.get("config", {}).get("MODEL_REVERSE_IMAGE", "N/A"),
            "tagger_model": app_state.get("config", {}).get("MODEL_TAGGER", "N/A"),
            "index_rebuild_interval_minutes": app_state.get("config", {}).get("INDEX_REBUILD_INTERVAL_MINUTES", 0),
            "model_idle_unload_seconds": app_state.get("config", {}).get("MODEL_IDLE_UNLOAD_SECONDS", 0),
        },
    }
    return JSONResponse(content=status_payload)


# ==============================================================================
# 9. Legacy Routes (Removed)
# ==============================================================================

# The legacy media serving endpoints (`/media/old`, `/media/v1-autocache`, `/media/v2-stream`)
# have been removed. Their functionality has been consolidated into the single, more robust
# `/api/instance/{instance_id}/media` endpoint. This new endpoint handles decryption,
# on-the-fly streaming for incompatible videos, and serving of static files correctly.