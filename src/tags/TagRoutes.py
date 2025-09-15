#TagRoutes.py
import aiosqlite
from enum import Enum
from typing import List, Union, Optional, Callable, Any
from functools import wraps
import inspect

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status, Path
from pydantic import BaseModel, Field

# --- Assume a logger is configured ---

from ConfigMedia import get_config
import logging
from src.media.actions import retag_file
from src.models import UserProfile

logger = logging.getLogger("TagService")
logger.setLevel(logging.DEBUG)

# --- Service and Helper Imports ---
# get_current_active_user_profile is not used, so it can be removed.
 # For type hinting in decorator
from src.roles.permissions import Permission
from src.roles.rbac_manager import require_permission_from_profile
from src.tags.HelperTags import (
    create_tag,
    edit_tag,
    get_visible_tags_for_user,
    remove_tag,
    get_tag_data,
    assign_tags_to_file,
    unassign_tags_from_file
)
from src.db_utils import get_db_connection
from src.users.auth_dependency import get_current_active_user

from ImageTagger import get_tagger
from ClipManager import get_clip_manager

tagging_progress_cache = {}

# --- API Router Setup ---
router = APIRouter(prefix='/api', tags=["Tags"])

# --- Enums and Pydantic Models ---

class TagType(str, Enum):
    tags = "tags"
    meta_tags = "meta_tags"

class TagCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, description="The unique name for the new tag.")
    parents: Optional[List[Union[int, str]]] = Field(None, description="Optional list of parent tag IDs or names.")
    alias_of: Optional[Union[int, str]] = Field(None, description="Make this tag an alias of another tag.")
    is_protection_tag: bool = Field(False, description="Mark this tag as a protection tag.")

class TagEditRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, description="A new unique name for the tag. Overwrites the existing name.")
    parents: Optional[List[Union[int, str]]] = Field(None, description="A complete list of parent tags. This will overwrite all existing parents.")
    alias_of: Optional[Union[int, str]] = Field(None, description="Set this tag as an alias of another tag. Use `null` or omit to make it a regular tag.")
    is_protection_tag: Optional[bool] = Field(None, description="Update the protection status of the tag.")

    
class TagAssignmentRequest(BaseModel):
    tags: List[Union[int, str]] = Field(..., min_items=1, description="A list of tag IDs or names to assign/unassign.")



# --- API Routes ---

@router.get("/tags/{tag_type}", summary="Get all visible tags")
@require_permission_from_profile(permission=Permission.FILE_VIEW)
async def get_all_tags_route(
    tag_type: TagType,
    current_user: UserProfile = Depends(get_current_active_user),
):
    """
    Retrieves a list of all tags of a given type that are visible to the current user.
    A tag is visible if it's public or if the user has a role granting access.
    Requires 'file:view' permission.
    """
    # Permission check is now handled by the decorator
    visible_tags = await get_visible_tags_for_user(current_user.id, tag_type.value)
    return visible_tags
    
@router.post("/tags/{tag_type}", status_code=status.HTTP_201_CREATED, summary="Create a new tag")
@require_permission_from_profile(permission=Permission.TAG_EDIT)
async def create_new_tag_route(
    tag_type: TagType,
    request: TagCreateRequest,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Creates a new tag or meta_tag. Requires 'tag:edit' permission.
    """
    try:
        new_tag_id = await create_tag(
            tag_name=request.name,
            tag_type=tag_type.value,
            conn=conn,
            parents=request.parents,
            alias_of=request.alias_of,
            is_protection_tag=request.is_protection_tag
        )
        if new_tag_id is None:
            raise HTTPException(status_code=409, detail=f"Failed to create tag '{request.name}'. It may already exist or an invalid parent/alias was provided.")

        return await get_tag_data(tag_type=tag_type.value, conn=conn, tag_id=new_tag_id)
        
    except (aiosqlite.IntegrityError, ValueError) as e:
        raise HTTPException(status_code=409, detail=f"Database conflict or invalid input: {e}")

@router.patch("/tags/{tag_type}/{tag_id}", summary="Edit a tag")
@require_permission_from_profile(permission=Permission.TAG_EDIT)
async def update_tag_route(
    tag_type: TagType,
    tag_id: int,
    request: TagEditRequest,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Updates a tag's properties. Requires 'tag:edit' permission.
    All changes are applied in a single transaction.
    """
    update_kwargs = request.model_dump(exclude_unset=True)
    if not update_kwargs:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")

    try:
        success = await edit_tag(
            tag_id=tag_id,
            tag_type=tag_type.value,
            conn=conn,
            **update_kwargs
        )
        if not success:
            raise HTTPException(status_code=400, detail="Tag edit failed. Check for conflicting operations or a non-existent tag.")
        
        return await get_tag_data(tag_type=tag_type.value, conn=conn, tag_id=tag_id)

    except aiosqlite.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Database conflict: {e}")

@router.get("/tags/{tag_type}/id/{tag_id}", summary="Get detailed tag data by ID")
@require_permission_from_profile(permission=Permission.FILE_VIEW)
async def get_tag_by_id_route(
    tag_type: TagType,
    tag_id: int = Path(..., description="The numeric ID of the tag."),
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Retrieves detailed information about a single tag using its unique numeric ID.
    Requires 'file:view' permission.
    """
    tag_data = await get_tag_data(tag_type=tag_type.value, conn=conn, tag_id=tag_id)

    if tag_data is None:
        raise HTTPException(status_code=404, detail=f"Tag with ID '{tag_id}' not found.")
        
    return tag_data


@router.get("/tags/{tag_type}/name/{tag_name}", summary="Get detailed tag data by Name")
@require_permission_from_profile(permission='file:view')
async def get_tag_by_name_route(
    tag_type: TagType,
    tag_name: str = Path(..., description="The unique name of the tag."),
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Retrieves detailed information about a single tag using its unique name.
    Requires 'file:view' permission.
    """
    tag_data = await get_tag_data(tag_type=tag_type.value, conn=conn, tag_name=tag_name)

    if tag_data is None:
        raise HTTPException(status_code=404, detail=f"Tag with name '{tag_name}' not found.")
        
    return tag_data

@router.delete("/tags/{tag_type}/{tag_identifier}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a tag")
@require_permission_from_profile(permission=Permission.TAG_EDIT)
async def delete_tag_route(
    tag_type: TagType,
    tag_identifier: Union[int, str] = Path(..., description="The ID or name of the tag to delete."),
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Deletes a tag or meta_tag. Requires 'tag:edit' permission.
    """
    success = await remove_tag(tag_identifier, tag_type.value, conn)
    if not success:
        raise HTTPException(status_code=404, detail=f"Tag '{tag_identifier}' not found.")
    return None # Return 204 No Content

# --- File Tagging Routes ---

@router.post("/tags/{tag_type}/file/{file_id}", status_code=status.HTTP_200_OK, summary="Assign tags to a file")
@require_permission_from_profile(permission=Permission.TAG_EDIT)
async def assign_tags_to_file_route(
    file_id: int,
    tag_type: TagType,
    request: TagAssignmentRequest,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Assigns one or more tags to a file. Requires 'tag:edit' permission.
    """
    success = await assign_tags_to_file(file_id, request.tags, tag_type.value, conn)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to assign tags. One or more tags may not exist.")
    return {"message": f"Tags successfully assigned to file {file_id}."}

@router.delete("/tags/{tag_type}/file/{file_id}", status_code=status.HTTP_200_OK, summary="Unassign tags from a file")
@require_permission_from_profile(permission=Permission.TAG_EDIT)
async def unassign_tags_from_file_route(
    file_id: int,
    tag_type: TagType,
    request: TagAssignmentRequest,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Unassigns one or more tags from a file. Requires 'tag:edit' permission.
    """
    await unassign_tags_from_file(file_id, request.tags, tag_type.value, conn)
    return {"message": f"Tags successfully unassigned from file {file_id}."}




@router.post("/files/{file_id}/auto-tag", status_code=202)
@require_permission_from_profile(Permission.TAG_EDIT)
async def trigger_auto_tagging(
    file_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Triggers the automatic tagging process for a specific file."""
    if file_id in tagging_progress_cache and isinstance(tagging_progress_cache[file_id], (int, float)) and tagging_progress_cache[file_id] < 100:
        raise HTTPException(status_code=409, detail="Tagging for this file is already in progress.")

    scanner_state = request.app.state.scanner
    if not scanner_state or 'executor' not in scanner_state:
        raise HTTPException(status_code=503, detail="Scanner service is not available.")

    executor = scanner_state['executor']
    
    # Get models
    config_data = get_config()
    tagger = await get_tagger(config_data["MODEL_TAGGER"])
    clip_manager = get_clip_manager()
    clip_model, clip_preprocess = await clip_manager.load_or_get_model(config_data["MODEL_REVERSE_IMAGE"])

    if not clip_model:
        raise HTTPException(status_code=503, detail="CLIP model is not available.")

    background_tasks.add_task(
        retag_file,
        file_id=file_id,
        executor=executor,
        tagger_model_name=config_data["MODEL_TAGGER"],
        clip_model_name=config_data["MODEL_REVERSE_IMAGE"],
        tagger=tagger,
        clip_model=clip_model,
        clip_preprocess=clip_preprocess,
        progress_cache=tagging_progress_cache
    )

    return {"status": "queued", "message": f"File {file_id} has been queued for auto-tagging."}

@router.get("/files/{file_id}/tagging-status")
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_tagging_status(file_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """Checks the tagging status of a file."""
    progress = tagging_progress_cache.get(file_id)
    if progress is None:
        return {"status": "not_started"}
    if isinstance(progress, dict) and progress.get("status") == "error":
        return progress
    if progress == 100:
        return {"status": "complete", "progress": 100}
    return {"status": "in_progress", "progress": progress}