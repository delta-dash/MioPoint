# mediaroutes.py
import asyncio
import mimetypes
import os
import shutil
import tempfile
from typing import AsyncGenerator, Dict, List, Optional
from uuid import uuid4

from fastapi import (APIRouter, BackgroundTasks, Depends, File, HTTPException, Header,
                     Query, Request, Response, UploadFile)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from ClipManager import get_clip_manager
from ConfigMedia import get_config
from EncryptionManager import decrypt_to_memory
from ImageTagger import get_tagger
from Searcher import ReverseSearcher, get_searcher
import logging

# --- Project-specific Imports ---
from src.media.media_repository import add_role_to_all_files, manage_role_for_files_by_tags, remove_role_from_all_files, update_file_visibility_roles, update_visibility_for_files_by_tags
from src.models import FileVisibilityUpdateRequest, RoleBasicResponse, TagVisibilityUpdateRequest, UserProfile
from src.media.HelperMedia import (delete_file_async, get_cards_with_cursor,
                                     get_file_details, get_thumbnail_by_id, get_visibility_roles_for_file)
from src.media.video_convert_task import schedule_video_conversion
from src.roles.permissions import Permission
from src.roles.rbac_manager import require_permission_from_profile
from src.users.auth_dependency import get_current_active_user, get_current_user_or_guest
from utilsEncryption import decrypt_file_content  # Used by older media endpoints
from utilsVideo import convert_and_update_video, get_subtitle_streams, is_video_codec_compatible, stream_transcoded_video

# --- Global Instances and Config ---
conversion_progress_cache = {}
tagging_progress_cache = {}
config = get_config()
logger = logging.getLogger("MediaRoutes")
logger.setLevel(logging.DEBUG)
router = APIRouter(tags=["Media"])

class GlobalRoleUpdateRequest(BaseModel):
    role_id: int

class TagRoleUpdateRequest(BaseModel):
    tags: List[str]
    role_id: int
    tag_type: str = Field("tags", pattern="^(tags|meta_tags)$")

class VisibilityRoleResponse(BaseModel):
    id: int
    name: str
    rank: int

class SceneResponse(BaseModel):
    id: int
    file_id: int
    start_timecode: str
    end_timecode: str
    transcript: Optional[str] = None
    tags: List[str]

class RoleInUploader(BaseModel):
    id: int
    name: str
    rank: int

class UploaderResponse(BaseModel):
    id: int
    username: str
    roles: List[RoleInUploader]

class FileDetailsResponse(BaseModel):
    id: int
    file_name: str
    file_hash: str
    file_type: str
    extension: str
    stored_path: str
    phash: Optional[str] = None
    is_encrypted: bool
    metadata: str  # Kept as a JSON string, client can parse
    transcript: Optional[str] = None
    discovered_at: str
    processed_at: str
    ingest_source: str
    transcoded_path: Optional[str] = None
    duration_seconds: Optional[float] = None
    original_created_at: str
    original_modified_at: str
    size_in_bytes: Optional[int] = None
    uploader: Optional[UploaderResponse] = None
    tags: List[str]
    meta_tags: List[str]
    scenes: List[SceneResponse]
    visibility_roles: List[VisibilityRoleResponse]


# --- Pydantic Models ---

class CardItem(BaseModel):
    """Defines the structure of a single file item (card) in the API response."""
    id: int
    display_name: str
    file_type: str
    is_encrypted: bool
    file_hash: Optional[str] = None
    duration_seconds: Optional[float] = None
    original_created_at: Optional[str] = None

class PaginatedCardResponse(BaseModel):
    """Defines the structure of the paginated card response."""
    items: List[CardItem]
    next_cursor: Optional[str] = Field(None, description="Cursor for the next page. Null if no more results.")

class TagFileRequest(BaseModel):
    """Request body for tagging a file."""
    tags: List[str]


# --- Dependencies ---

async def get_media_password(x_media_password: Optional[str] = Header(None)) -> Optional[str]:
    """Dependency to extract the optional media password from the header."""
    return x_media_password


# --- API Routes: Search and Discovery ---

@router.get("/api/cards", response_model=PaginatedCardResponse)
@require_permission_from_profile(Permission.FILE_SEARCH)
async def search_files_with_cursor(
    current_user: UserProfile = Depends(get_current_active_user),
    search: Optional[str] = Query(None, description="Keyword search across paths, transcripts, metadata."),
    filename: Optional[str] = Query(None, description="Search specifically within the file's name."),
    tags: Optional[str] = Query(None, description="Comma-separated list of tags the file MUST have."),
    exclude_tags: Optional[str] = Query(None, description="Comma-separated list of tags the file MUST NOT have."),
    meta_tags: Optional[str] = Query(None, description="Comma-separated list of meta-tags the file MUST have."),
    exclude_meta_tags: Optional[str] = Query(None, description="Comma-separated list of meta-tags to exclude."),
    
    # --- MODIFIED LINE ---
    # Change from Optional[str] to Optional[List[str]] to accept multiple values
    file_type: Optional[List[str]] = Query(None, description="Filter by one or more file types (e.g., 'image', 'video')."),
    
    is_encrypted: Optional[bool] = Query(None, description="Filter by encryption status."),
    duration_min: Optional[int] = Query(None, gt=0, description="Minimum duration in seconds."),
    duration_max: Optional[int] = Query(None, gt=0, description="Maximum duration in seconds."),
    page_size: int = Query(24, gt=0, le=100, description="Items per page."),
    cursor: Optional[str] = Query(None, description="Cursor for the next page.")
):
    """Search for files with advanced filters and stable, cursor-based pagination."""
    results = await get_cards_with_cursor(
        user_id=current_user.id,
        search=search, filename=filename, tags=tags, exclude_tags=exclude_tags,
        meta_tags=meta_tags, exclude_meta_tags=exclude_meta_tags,
        # The file_type argument is now a list
        file_type=file_type, is_encrypted=is_encrypted,
        duration_min=duration_min, duration_max=duration_max,
        page_size=page_size, cursor=cursor
    )
    return results

@router.post("/api/search/image")
@require_permission_from_profile(Permission.FILE_SEARCH)
async def search_by_image(
    current_user: UserProfile = Depends(get_current_active_user),
    file: UploadFile = File(..., description="The image file to search for."),
    top_k: int = 5,
    searcher: ReverseSearcher = Depends(get_searcher)
):
    """Accepts an image upload and performs a reverse image search."""
    temp_dir = tempfile.mkdtemp()
    try:
        temp_file_path = os.path.join(temp_dir, file.filename)
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        search_output = await searcher.search(image_path=temp_file_path, top_k=top_k)

        if search_output.get("status") == "error":
            raise HTTPException(status_code=400, detail=search_output.get("message"))
            
        return JSONResponse(content={
            "search_context": {
                "match_type": search_output.get("match_type", "none"),
                "query_found_in_db": search_output.get("query_found_in_db", False),
                "message": search_output.get("message")
            },
            "results": {
                "total": len(search_output.get("results", [])),
                "items": search_output.get("results", [])
            }
        })
    finally:
        if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
        await file.close()


# --- API Routes: File Management and Metadata ---

@router.get("/api/file/{file_id}", response_model=FileDetailsResponse)
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_file_by_id(file_id: int, current_user: UserProfile = Depends(get_current_user_or_guest)):
    """Retrieve the full details for a single file, respecting user visibility."""
    details = await get_file_details(file_id, user_id=current_user.id)
    if details is None:
        raise HTTPException(status_code=404, detail=f"No file found with ID: {file_id}")
    return details

@router.delete("/api/file/{file_id}", status_code=200)
@require_permission_from_profile(Permission.FILE_DELETE)
async def delete_file_by_id(file_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """Deletes a file and all its associated data. Requires FILE_DELETE permission."""
    success = await delete_file_async(file_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete file. Check logs for details.")
    return {"status": "success", "message": f"File {file_id} has been deleted."}

@router.post("/api/files/upload", status_code=202)
@require_permission_from_profile(Permission.FILE_ADD)
async def upload_file_for_processing(
    request: Request,
    file: UploadFile = File(...),
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Accepts a file upload and submits it to the scanner's intake queue.
    The scanner service will handle duplicate checks after sanitization.
    """
    scanner_state = request.app.state.scanner
    if not scanner_state or 'intake_queue' not in scanner_state:
        raise HTTPException(status_code=503, detail="Scanner service is not available.")

    upload_dir = os.path.join(config.get("WATCH_DIRS")[0], "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    
    safe_filename = f"{uuid4()}_{file.filename}"
    file_path = os.path.join(upload_dir, safe_filename)

    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    ingest_info = (file_path, "upload", current_user.id)
    await scanner_state['intake_queue'].put(ingest_info)

    return {"status": "queued", "detail": f"File '{file.filename}' has been queued for processing."}


# --- API Routes: Visibility Management ---

@router.get("/api/file/{file_id}/visibility", response_model=List[RoleBasicResponse])
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_file_visibility(file_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """Gets the list of roles that can view a specific file."""
    roles = await get_visibility_roles_for_file(file_id, user_id=current_user.id)
    if roles is None:
        raise HTTPException(status_code=404, detail="File not found or access denied.")
    return roles

@router.put("/api/file/{file_id}/visibility", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def set_file_visibility(
    file_id: int,
    request: FileVisibilityUpdateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Sets (overwrites) the list of roles that can view a specific file.
    An empty list defaults to the 'Everyone' role.
    """
    await update_file_visibility_roles(file_id, request.role_ids)
    return {"status": "success", "message": f"Visibility for file {file_id} has been updated."}

@router.put("/api/files/visibility/by-tag", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def set_files_visibility_by_tag(
    request: TagVisibilityUpdateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Performs a bulk update, setting the visibility for all files matching the given tags."""
    await update_visibility_for_files_by_tags(
        tags=request.tags,
        new_role_ids=request.role_ids,
        tag_type=request.tag_type
    )
    return {"status": "success", "message": "File visibility has been updated based on tags."}

@router.post("/api/files/visibility/global", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def add_role_globally(
    request: GlobalRoleUpdateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Adds a single role to the visibility list of ALL files."""
    await add_role_to_all_files(role_id=request.role_id)
    return {"status": "success", "message": f"Role {request.role_id} added to all files."}

@router.delete("/api/files/visibility/global", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def remove_role_globally(
    request: GlobalRoleUpdateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Removes a single role from the visibility list of ALL files. Cannot remove the 'Everyone' role."""
    try:
        await remove_role_from_all_files(role_id=request.role_id)
        return {"status": "success", "message": f"Role {request.role_id} removed from all files."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/api/files/visibility/by-tag", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def add_role_by_tag(
    request: TagRoleUpdateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Adds a single role to all files matching the given tags."""
    await manage_role_for_files_by_tags(
        tags=request.tags,
        role_id=request.role_id,
        tag_type=request.tag_type,
        action='add'
    )
    return {"status": "success", "message": f"Role {request.role_id} added to files matching tags."}

@router.delete("/api/files/visibility/by-tag", status_code=200)
@require_permission_from_profile(Permission.FILE_EDIT)
async def remove_role_by_tag(
    request: TagRoleUpdateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Removes a single role from all files matching the given tags."""
    await manage_role_for_files_by_tags(
        tags=request.tags,
        role_id=request.role_id,
        tag_type=request.tag_type,
        action='remove'
    )
    return {"status": "success", "message": f"Role {request.role_id} removed from files matching tags."}
# --- API Routes: Auto-Tagging ---






# --- API Routes: Media and Thumbnail Serving ---

@router.get("/api/{file_id}/thumbnail")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_media_thumbnail(
    file_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """Serves a pre-generated thumbnail. Requires password via header for encrypted files."""
    details = await get_file_details(file_id, user_id=current_user.id)
    if not details:
        raise HTTPException(status_code=404, detail="File not found or access denied.")

    if details['is_encrypted'] and not password:
        raise HTTPException(
            status_code=401,
            detail="Password required to generate thumbnail for this encrypted file.",
            headers={"WWW-Authenticate": "X-Media-Password"},
        )

    thumbnail_info = await get_thumbnail_by_id(file_id, as_bytes=False, password=password)

    if not thumbnail_info or not (thumbnail_path := thumbnail_info.get('data')) or not os.path.exists(thumbnail_path):
        raise HTTPException(status_code=404, detail="Thumbnail not found or could not be generated.")
    return FileResponse(thumbnail_path)

@router.get("/api/{file_id}/media", summary="Serve media with streaming and caching")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_media_file_combined(
    file_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """
    Serves media content. Requires password for encrypted files.
    Streams incompatible videos on-the-fly while caching them.
    """
    file_details = await get_file_details(file_id, user_id=current_user.id)
    if not file_details or not os.path.exists(file_details['stored_path']):
        raise HTTPException(status_code=404, detail="File not found or access denied.")

    stored_path = file_details.get('stored_path')

    if file_details['is_encrypted']:
        if not password:
            raise HTTPException(
                status_code=401,
                detail="A password is required to access this encrypted file.",
                headers={"WWW-Authenticate": "X-Media-Password"},
            )
        decrypted_content = decrypt_to_memory(stored_path, password)
        if decrypted_content is None:
            raise HTTPException(status_code=401, detail="Invalid password.")
        
        media_type, _ = mimetypes.guess_type(file_details['file_name'])
        return Response(content=decrypted_content, media_type=media_type or "application/octet-stream")

    if (transcoded_path := file_details.get('transcoded_path')) and os.path.exists(transcoded_path):
        return FileResponse(transcoded_path, media_type="video/mp4", headers={'Accept-Ranges': 'bytes'})

    if file_details['file_type'] == 'video':
        if not await is_video_codec_compatible(stored_path):
            await schedule_video_conversion(file_id, conversion_progress_cache)
            return StreamingResponse(stream_transcoded_video(stored_path), media_type="video/mp4")

    media_type, _ = mimetypes.guess_type(stored_path)
    return FileResponse(stored_path, media_type=media_type or "application/octet-stream", headers={'Accept-Ranges': 'bytes'})


# --- API Routes: Subtitles ---

@router.get("/api/{file_id}/subtitles", summary="List available subtitle tracks")
@require_permission_from_profile(Permission.FILE_VIEW)
async def list_subtitles(
    file_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """Inspects a media file for subtitles. Requires password for encrypted files."""
    file_details = await get_file_details(file_id, user_id=current_user.id)
    if not file_details or not os.path.exists(file_details['stored_path']):
        raise HTTPException(status_code=404, detail="File not found or access denied.")
    
    source_path = file_details['stored_path']
    temp_path = None
    try:
        if file_details['is_encrypted']:
            if not password:
                raise HTTPException(status_code=401, detail="Password required to scan for subtitles.", headers={"WWW-Authenticate": "X-Media-Password"})
            
            decrypted_content = decrypt_to_memory(source_path, password)
            if not decrypted_content:
                raise HTTPException(status_code=401, detail="Invalid password.")

            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(source_path)[1]) as temp_file:
                temp_file.write(decrypted_content)
                temp_path = temp_file.name
            source_path = temp_path

        subtitle_list = await get_subtitle_streams(source_path)
        return JSONResponse(content=subtitle_list)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

async def _vtt_generator(source_path: str, stream_index: int) -> AsyncGenerator[bytes, None]:
    """An async generator that streams a subtitle track as VTT using ffmpeg."""
    command = [
        'ffmpeg', '-i', source_path, '-map', f'0:{stream_index}',
        '-f', 'webvtt', '-y', 'pipe:1'
    ]
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL
    )
    try:
        while True:
            chunk = await proc.stdout.read(1024)
            if not chunk:
                break
            yield chunk
        await proc.wait()
    finally:
        if proc.returncode is None:
            proc.kill()

@router.get("/api/{file_id}/subtitle/{stream_index}", summary="Serve a subtitle track as VTT")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_subtitle_vtt(
    file_id: int,
    stream_index: int,
    current_user: UserProfile = Depends(get_current_active_user),
    password: Optional[str] = Depends(get_media_password)
):
    """Streams a subtitle track converted to VTT format. Requires a password for encrypted files."""
    file_details = await get_file_details(file_id, user_id=current_user.id)
    if not file_details or not os.path.exists(file_details['stored_path']):
        raise HTTPException(status_code=404, detail="File not found or access denied.")

    source_path = file_details['stored_path']
    temp_path = None
    try:
        if file_details['is_encrypted']:
            if not password:
                raise HTTPException(status_code=401, detail="A password is required via the X-Media-Password header.")

            decrypted_content = await asyncio.to_thread(decrypt_to_memory, source_path, password)
            if not decrypted_content:
                raise HTTPException(status_code=403, detail="Invalid password.")

            def write_temp_file(content):
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(source_path)[1]) as temp_f:
                    temp_f.write(content)
                    return temp_f.name
            temp_path = await asyncio.to_thread(write_temp_file, decrypted_content)
            source_path = temp_path

        vtt_streamer = _vtt_generator(source_path, stream_index)
        
        async def cleanup_wrapper(generator, path_to_clean):
            try:
                async for chunk in generator:
                    yield chunk
            finally:
                if path_to_clean:
                    await asyncio.to_thread(os.remove, path_to_clean)

        return StreamingResponse(cleanup_wrapper(vtt_streamer, temp_path), media_type="text/vtt")
    except Exception:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        raise


# --- API Routes: Conversion and System Status ---

@router.post("/api/files/{file_id}/convert", status_code=202)
@require_permission_from_profile(Permission.FILE_EDIT)
async def start_video_conversion(file_id: int, background_tasks: BackgroundTasks, current_user: UserProfile = Depends(get_current_active_user)):
    """Triggers a background task to convert a video and track its progress."""
    if file_id in conversion_progress_cache:
        raise HTTPException(status_code=409, detail="Conversion for this file is already in progress.")
    background_tasks.add_task(convert_and_update_video, file_id, conversion_progress_cache)
    return {"status": "success", "message": "Video conversion started."}

@router.get("/api/files/{file_id}/status")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_file_conversion_status(file_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """Checks the conversion status of a file."""
    progress_entry = conversion_progress_cache.get(file_id)
    if progress_entry is not None:
        if isinstance(progress_entry, dict) and progress_entry.get("status") == "error":
            return progress_entry
        return {"status": "converting", "progress": progress_entry}
    
    file_details = await get_file_details(file_id)
    if not file_details:
        raise HTTPException(status_code=404, detail="File not found")

    if file_details.get('transcoded_path') and os.path.exists(file_details['transcoded_path']):
        return {"status": "ready", "progress": 100}
    
    if file_details['file_type'] != 'video' or await is_video_codec_compatible(file_details['stored_path']):
        return {"status": "ready", "progress": 100}
    
    return {"status": "pending_conversion", "progress": 0}

@router.get("/api/scanner/status")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_scanner_status(request: Request, current_user: UserProfile = Depends(get_current_active_user)):
    """Checks the status of the background scanner service."""
    scanner_state = request.app.state.scanner
    if not scanner_state:
        return {"status": "Error", "detail": "Scanner service failed to initialize."}
    return {
        "status": scanner_state.get("status", "Unknown"),
        "executor_running": scanner_state["executor"]._shutdown is False,
        "active_tasks": len(scanner_state.get("tasks", [])),
        "queue_size": scanner_state['intake_queue'].qsize()
    }


# --- Legacy/Alternative Media Serving Routes (for reference or compatibility) ---

@router.get("/api/{file_id}/media/old", summary="Original: Serve media, requires manual convert call")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_media_file_original(
    file_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    decryption_key: Optional[str] = Header(None, alias="X-Decryption-Key", description="The key to decrypt the file if it is encrypted.")
):
    file_details = await get_file_details(file_id, current_user.id)
    if not file_details or not os.path.exists(file_details['stored_path']):
        raise HTTPException(status_code=404, detail="File not found")

    if (transcoded_path := file_details.get('transcoded_path')) and os.path.exists(transcoded_path):
        return FileResponse(transcoded_path, media_type="video/mp4", headers={'Accept-Ranges': 'bytes'})

    if file_details['file_type'] == 'video' and not file_details['is_encrypted']:
        if not await is_video_codec_compatible(file_details['stored_path']):
            return JSONResponse(status_code=409, content={"status": "conversion_required"})
    
    stored_path = file_details['stored_path']
    if not file_details['is_encrypted']:
        return FileResponse(stored_path, headers={'Accept-Ranges': 'bytes'})
    else:
        # Check if the decryption key was provided
        if not decryption_key:
            raise HTTPException(status_code=400, detail="Decryption key required via X-Decryption-Key header for this encrypted file.")
        
        # The key is passed as a string, but the underlying crypto function expects bytes.
        key_bytes = decryption_key.encode('utf-8')
        decrypted_content = decrypt_file_content(stored_path, key_bytes)
        
        # Handle decryption failure (e.g., wrong key)
        if decrypted_content is None:
            raise HTTPException(status_code=403, detail="Invalid decryption key provided.")
            
        return Response(content=decrypted_content, media_type="application/octet-stream")

@router.get("/api/{file_id}/media/v1-autocache", summary="Solution 1: Auto-triggers background conversion")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_media_file_autocache(
    file_id: int,
    background_tasks: BackgroundTasks, 
    current_user: UserProfile = Depends(get_current_active_user),
    decryption_key: Optional[str] = Header(None, alias="X-Decryption-Key", description="The key to decrypt the file if it is encrypted.")
):
    """
    Serves a media file. If an incompatible video is requested for the first time,
    it automatically starts a background conversion task and serves the original file.
    """
    file_details = await get_file_details(file_id)
    if not file_details or not os.path.exists(file_details['stored_path']):
        raise HTTPException(status_code=404, detail="File not found")

    stored_path = file_details.get('stored_path')

    if (transcoded_path := file_details.get('transcoded_path')) and os.path.exists(transcoded_path):
        return FileResponse(transcoded_path, media_type="video/mp4", headers={'Accept-Ranges': 'bytes'})

    if file_details['file_type'] == 'video' and not file_details['is_encrypted']:
        if not await is_video_codec_compatible(stored_path):
            if file_id not in conversion_progress_cache:
                background_tasks.add_task(convert_and_update_video, file_id, conversion_progress_cache)
            
    if not file_details['is_encrypted']:
        return FileResponse(stored_path, headers={'Accept-Ranges': 'bytes'})
    else:
        # Check if the decryption key was provided
        if not decryption_key:
            raise HTTPException(status_code=400, detail="Decryption key required via X-Decryption-Key header for this encrypted file.")

        # The key is passed as a string, but the underlying crypto function expects bytes.
        key_bytes = decryption_key.encode('utf-8')
        decrypted_content = decrypt_file_content(stored_path, key_bytes)

        # Handle decryption failure (e.g., wrong key)
        if decrypted_content is None:
            raise HTTPException(status_code=403, detail="Invalid decryption key provided.")
            
        return Response(content=decrypted_content, media_type="application/octet-stream")

@router.get("/api/{file_id}/media/v2-stream", summary="Solution 2: On-the-fly streaming transcode")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_media_file_stream(
    file_id: int, 
    current_user: UserProfile = Depends(get_current_active_user),
    decryption_key: Optional[str] = Header(None, alias="X-Decryption-Key", description="The key to decrypt the file if it is encrypted.")
):
    """
    Serves a media file. If an incompatible video is requested, it transcodes
    it in real-time and streams the output directly to the client. Does not create a cache file.
    """
    file_details = await get_file_details(file_id)
    if not file_details or not os.path.exists(file_details['stored_path']):
        raise HTTPException(status_code=404, detail="File not found")

    stored_path = file_details.get('stored_path')

    if (transcoded_path := file_details.get('transcoded_path')) and os.path.exists(transcoded_path):
        return FileResponse(transcoded_path, media_type="video/mp4", headers={'Accept-Ranges': 'bytes'})

    if file_details['file_type'] == 'video' and not file_details['is_encrypted']:
        if not await is_video_codec_compatible(stored_path):
            return StreamingResponse(stream_transcoded_video(stored_path), media_type="video/mp4")

    if not file_details['is_encrypted']:
        return FileResponse(stored_path, headers={'Accept-Ranges': 'bytes'})
    else:
        # Check if the decryption key was provided
        if not decryption_key:
            raise HTTPException(status_code=400, detail="Decryption key required via X-Decryption-Key header for this encrypted file.")

        # The key is passed as a string, but the underlying crypto function expects bytes.
        key_bytes = decryption_key.encode('utf-8')
        decrypted_content = decrypt_file_content(stored_path, key_bytes)

        # Handle decryption failure (e.g., wrong key)
        if decrypted_content is None:
            raise HTTPException(status_code=403, detail="Invalid decryption key provided.")
            
        return Response(content=decrypted_content, media_type="application/octet-stream")
