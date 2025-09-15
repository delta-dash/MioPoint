#TagService.py
import aiosqlite
from typing import List, Union, Optional, Dict

from src.roles.permissions import Permission
from src.tags.HelperTags import (
    create_tag,
    edit_tag,
    get_visible_tags_for_user,
    remove_tag,
    get_tag_data,
    assign_tags_to_file,
    unassign_tags_from_file
)
import logging
from src.roles.rbac_manager import require_permission

logger = logging.getLogger("TagService")
logger.setLevel(logging.DEBUG)

# Note: The `require_permission` decorator is assumed to be async-compatible.
# It must be able to wrap and correctly await an async function.
# The decorator is responsible for raising a PermissionDeniedError if the check fails.

@require_permission(permission='file:view', caller_id_arg='user_id')
async def get_all_visible_tags_securely(
    user_id: int, 
    tag_type: str, 
    conn: Optional[aiosqlite.Connection] = None
) -> List[Dict]:
    """
    Securely and asynchronously retrieves all tags of a given type that are visible to the user.
    Visibility is determined by tag permissions and user roles.
    Requires 'file:view' permission.
    """
    logger.debug(f"User {user_id} is requesting all visible tags of type '{tag_type}'.")
    return await get_visible_tags_for_user(
        user_id=user_id,
        tag_type=tag_type,
        conn=conn # Pass the connection down if it exists
    )

@require_permission(permission='tag:edit', caller_id_arg='user_id')
async def create_tag_securely(
    user_id: int,
    tag_name: str,
    tag_type: str,
    conn: aiosqlite.Connection,
    parents: Optional[List[Union[int, str]]] = None,
    alias_of: Optional[Union[int, str]] = None,
    is_protection_tag: bool = False
) -> Optional[int]:
    """
    Securely and asynchronously creates a new tag after verifying the user has 'tag:edit' permission.
    """
    logger.info(f"User {user_id} is attempting to create tag '{tag_name}'.")
    return await create_tag(
        tag_name=tag_name,
        tag_type=tag_type,
        conn=conn,
        parents=parents,
        alias_of=alias_of,
        is_protection_tag=is_protection_tag
    )

@require_permission(permission='tag:edit', caller_id_arg='user_id')
async def edit_tag_securely(
    user_id: int, 
    tag_id: int, 
    tag_type: str, 
    conn: aiosqlite.Connection, 
    **update_kwargs
) -> bool:
    """
    Securely and asynchronously edits a tag after verifying the user has 'tag:edit' permission.
    """
    logger.info(f"User {user_id} is attempting to edit tag ID {tag_id}.")
    return await edit_tag(
        tag_id=tag_id,
        tag_type=tag_type,
        conn=conn,
        **update_kwargs
    )

@require_permission(permission='tag:edit', caller_id_arg='user_id')
async def remove_tag_securely(
    user_id: int, 
    tag_identifier: Union[int, str], 
    tag_type: str, 
    conn: aiosqlite.Connection
) -> bool:
    """
    Securely and asynchronously removes a tag after verifying the user has 'tag:edit' permission.
    """
    logger.info(f"User {user_id} is attempting to remove tag '{tag_identifier}'.")
    return await remove_tag(
        tag_identifier=tag_identifier,
        tag_type=tag_type,
        conn=conn
    )

@require_permission(permission='tag:edit', caller_id_arg='user_id')
async def assign_tags_to_file_securely(
    user_id: int, 
    file_id: int, 
    tags: List[Union[int, str]], 
    tag_type: str, 
    conn: aiosqlite.Connection
) -> bool:
    """
    Securely and asynchronously assigns tags to a file after verifying user permission.
    """
    logger.info(f"User {user_id} is assigning tags to file {file_id}.")
    return await assign_tags_to_file(
        file_id=file_id,
        tags=tags,
        tag_type=tag_type,
        conn=conn
    )

@require_permission(permission='tag:edit', caller_id_arg='user_id')
async def unassign_tags_from_file_securely(
    user_id: int, 
    file_id: int, 
    tags: List[Union[int, str]], 
    tag_type: str, 
    conn: aiosqlite.Connection
) -> bool:
    """
    Securely and asynchronously unassigns tags from a file after verifying user permission.
    """
    logger.info(f"User {user_id} is unassigning tags from file {file_id}.")
    return await unassign_tags_from_file(
        file_id=file_id,
        tags=tags,
        tag_type=tag_type,
        conn=conn
    )

@require_permission(permission=Permission.TAG_EDIT, caller_id_arg='user_id')
async def get_tag_data_securely(
    user_id: int, 
    tag_identifier: Union[int, str], 
    tag_type: str, 
    conn: aiosqlite.Connection
) -> Optional[Dict]:
    """
    Securely and asynchronously retrieves data for a single tag after verifying 'file:view' permission.
    """
    logger.debug(f"User {user_id} is requesting data for tag '{tag_identifier}'.")
    
    if isinstance(tag_identifier, int):
        return await get_tag_data(tag_type=tag_type, conn=conn, tag_id=tag_identifier)
    else:
        return await get_tag_data(tag_type=tag_type, conn=conn, tag_name=tag_identifier)