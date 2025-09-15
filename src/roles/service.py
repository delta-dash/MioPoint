#service.py
from typing import Set, Optional, List, Dict, Any

import aiosqlite
import logging
# Import the async versions of the RBAC manager functions and context
from src.models import PermissionResponse, RoleBasicResponse, RoleDetailResponse
from src.roles.errors import *
from src.roles.rbac_manager import (
    require_permission,
    assign_role_to_user, remove_role_from_user, create_role, edit_role, delete_role,
    get_permission_by_id, user_has_permission, get_all_roles, get_role_details_raw,
    AsyncDBContext, RBACError
)
from src.roles.permissions import Permission


logger = logging.getLogger("RoleService")
logger.setLevel(logging.DEBUG)

# --- Role Assignment/Removal Functions (Async) ---

# The decorator was updated to handle async functions
@require_permission(permission=Permission.USER_MANAGE_ROLES, caller_id_arg='admin_user_id')
async def assign_role_to_user_securely(
    admin_user_id: int,
    target_user_id: int,
    role_to_assign_id: int,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Securely assigns a role to a user.
    Uses AsyncDBContext internally for database operations.
    """
    logger.info(f"Admin user {admin_user_id} attempting to assign role {role_to_assign_id} to user {target_user_id}.")
    
    try:
        # The context manager handles connection and transaction
        async with AsyncDBContext(external_conn=conn) as db:
            success = await assign_role_to_user(target_user_id, role_to_assign_id, conn=db)
        return success
    except RBACError as e:
        logger.error(f"RBAC Error during role assignment: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected Error during role assignment: {e}", exc_info=True)
        raise RBACError(f"An unexpected error occurred during role assignment: {e}") from e

@require_permission(permission=Permission.USER_MANAGE_ROLES, caller_id_arg='admin_user_id')
async def remove_role_from_user_securely(
    admin_user_id: int,
    target_user_id: int,
    role_to_remove_id: int,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Securely removes a role from a user.
    Uses AsyncDBContext internally for database operations.
    """
    logger.info(f"Admin user {admin_user_id} attempting to remove role {role_to_remove_id} from user {target_user_id}.")
    
    try:
        async with AsyncDBContext(external_conn=conn) as db:
            success = await remove_role_from_user(target_user_id, role_to_remove_id, conn=db)
        return success
    except RBACError as e:
        logger.error(f"RBAC Error during role removal: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected Error during role removal: {e}", exc_info=True)
        raise RBACError(f"An unexpected error occurred during role removal: {e}") from e

# --- Role Management Functions (Async) ---

@require_permission(permission=Permission.USER_MANAGE_ROLES, caller_id_arg='user_id')
async def create_role_securely(
    user_id: int,
    name: str,
    rank: int,
    permissions: Optional[Set[str]] = None, # Changed to Set[str] for better API ergonomics
    conn: Optional[aiosqlite.Connection] = None
) -> int:
    """
    Securely creates a new role.
    If permissions are provided, also requires 'permission:grant'.
    """
    logger.info(f"User {user_id} attempting to create role '{name}' with rank {rank}.")
    
    try:
        async with AsyncDBContext(external_conn=conn) as db:
            permission_ids_to_set: Optional[Set[int]] = None
            
            # Validate permissions if they are provided
            if permissions is not None:
                if not await user_has_permission(user_id, Permission.PERMISSION_GRANT, conn=db):
                    raise PermissionDeniedError("You have permission to create roles, but not to grant permissions.")
                
                # Convert permission names to IDs
                from src.roles.rbac_manager import get_permission_id # Local import to avoid circular dependency if moved
                permission_ids_to_set = set()
                for perm_name in permissions:
                    perm_id = await get_permission_id(perm_name, conn=db)
                    if perm_id is None:
                        raise PermissionNotFoundError(f"Permission '{perm_name}' not found.")
                    permission_ids_to_set.add(perm_id)

            # Create the role
            new_role_id = await create_role(name, rank, conn=db)

            # If role created and permissions were specified, set them
            if new_role_id and permission_ids_to_set is not None:
                await edit_role(
                    role_id=new_role_id,
                    permissions_to_set=permission_ids_to_set,
                    conn=db
                )

            return new_role_id
    except RBACError:
        raise
    except Exception as e:
        logger.error(f"Unexpected Error during role creation: {e}", exc_info=True)
        raise RBACError(f"An unexpected error occurred during role creation: {e}") from e


@require_permission(permission=Permission.USER_MANAGE_ROLES, caller_id_arg='user_id')
async def edit_role_securely(
    user_id: int,
    role_id: int,
    new_name: Optional[str] = None,
    new_rank: Optional[int] = None,
    permissions_to_set: Optional[Set[int]] = None, # CORRECTED: Expect a Set of integers (IDs)
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Securely edits a role.
    Requires 'permission:grant' if permissions are being changed.
    """
    logger.info(f"User {user_id} attempting to edit role ID {role_id}.")

    try:
        async with AsyncDBContext(external_conn=conn) as db:
            # If permissions are being modified, we perform checks.
            if permissions_to_set is not None:
                # 1. Check if the user has permission to grant permissions.
                if not await user_has_permission(user_id, Permission.PERMISSION_GRANT, conn=db):
                    raise PermissionDeniedError("You have permission to manage roles, but not to change their permissions.")

                # 2. VALIDATE that all provided permission IDs actually exist.
                # This prevents errors from invalid data being passed from the client.
                if permissions_to_set: # Only run the query if the set is not empty
                    placeholders = ','.join('?' for _ in permissions_to_set)
                    query = f"SELECT COUNT(id) FROM permissions WHERE id IN ({placeholders})"
                    cursor = await db.execute(query, tuple(permissions_to_set))
                    result = await cursor.fetchone()
                    
                    if not result or result[0] != len(permissions_to_set):
                        # For a more helpful error message, find out which IDs are invalid
                        query_existing = f"SELECT id FROM permissions WHERE id IN ({placeholders})"
                        cursor_existing = await db.execute(query_existing, tuple(permissions_to_set))
                        existing_ids = {row[0] for row in await cursor_existing.fetchall()}
                        invalid_ids = permissions_to_set - existing_ids
                        raise PermissionNotFoundError(f"The following permission IDs do not exist: {', '.join(map(str, invalid_ids))}")

            # Now we can safely call the underlying rbac_manager function with the validated IDs.
            success = await edit_role(
                role_id=role_id,
                new_name=new_name,
                new_rank=new_rank,
                permissions_to_set=permissions_to_set, # Pass the original set of ints
                conn=db
            )
            return success
    except RBACError:
        raise
    except Exception as e:
        logger.error(f"Unexpected Error during role editing: {e}", exc_info=True)
        raise RBACError(f"An unexpected error occurred during role editing: {e}") from e
    


@require_permission(permission=Permission.USER_MANAGE_ROLES, caller_id_arg='user_id')
async def delete_role_securely(
    user_id: int,
    role_id: int,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Securely deletes a role.
    """
    logger.info(f"User {user_id} attempting to delete role ID {role_id}.")
    
    try:
        async with AsyncDBContext(external_conn=conn) as db:
            success = await delete_role(role_id=role_id, conn=db)
        return success
    except RBACError:
        raise
    except Exception as e:
        logger.error(f"Unexpected Error during role deletion: {e}", exc_info=True)
        raise RBACError(f"An unexpected error occurred during role deletion: {e}") from e

# --- Data Retrieval Functions (Async) ---

@require_permission(permission=Permission.USER_MANAGE_ROLES, caller_id_arg='user_id')
async def get_all_roles_securely(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[RoleBasicResponse]:
    """
    Retrieves a list of all roles.
    """
    logger.info("Fetching all roles securely...")
    try:
        async with AsyncDBContext(external_conn=conn) as db:
            rows = await get_all_roles(conn=db)
            # aiosqlite.Row can be unpacked like a dict into the Pydantic model
            return [RoleBasicResponse(**row) for row in rows]
    except Exception as e:
        logger.error(f"Unexpected error in get_all_roles_securely: {e}", exc_info=True)
        raise RBACError(f"An unexpected error occurred while fetching roles: {e}") from e

@require_permission(permission=Permission.USER_MANAGE_ROLES, caller_id_arg='user_id')
async def get_role_details_securely(role_id: int, user_id: int,conn: Optional[aiosqlite.Connection] = None) -> RoleDetailResponse:
    """
    Retrieves the details for a specific role, including its permissions.
    """
    logger.info(f"Fetching details for role ID {role_id} securely...")
    try:
        async with AsyncDBContext(external_conn=conn) as db:
            raw_rows = await get_role_details_raw(role_id=role_id, conn=db)

        if not raw_rows:
            raise RoleNotFoundError(f"Role with ID {role_id} not found.")

        # The first row contains the core role info
        first_row = raw_rows[0]
        
        # Aggregate permissions from all rows
        permissions_list = []
        for row in raw_rows:
            if row['permission_id'] is not None:
                permissions_list.append(PermissionResponse(
                    id=row['permission_id'],
                    name=row['permission_name'],
                    description=row['permission_description']
                ))
        
        return RoleDetailResponse(
            id=first_row['role_id'],
            name=first_row['role_name'],
            rank=first_row['role_rank'],
            permissions=permissions_list
        )
    except RoleNotFoundError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_role_details_securely for role {role_id}: {e}", exc_info=True)
        raise RBACError(f"An unexpected error occurred while fetching role details: {e}") from e