import aiosqlite
from typing import Any, Dict, List, Optional

from fastapi import Request, status, HTTPException

import logging
from src.db_utils import execute_db_query
from src.models import UserProfile

logger = logging.getLogger("ConfigRoutes")
logger.setLevel(logging.INFO)



async def get_current_active_user_profile(request: Request) -> UserProfile:
    """
    The primary dependency for protected routes.
    
    It retrieves the full, fresh UserProfile object that the `smart_auth_middleware`
    has already authenticated, fetched from the DB, and attached to the request state.
    
    It also ensures the user is not a guest and is active.
    """
    # 1. Check if the middleware populated the user object.
    user = getattr(request.state, "user", None)
    if not isinstance(user, UserProfile):
        # This will be triggered for guest users or if the token was invalid.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 2. Check if the authenticated user is active.
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")

    # 3. Check if the user is a guest (if the route should not be accessible to guests)
    if user.is_guest:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action is not available to guest users."
        )
        
    return user

# ==============================================================================
# --- USER PROFILE AGGREGATION ---
# ==============================================================================

async def get_user_profile_with_details_by_id(conn: aiosqlite.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves a user's complete profile asynchronously, including a detailed 
    breakdown of roles and their associated permissions, PLUS a consolidated flat 
    list of all effective permissions.
    
    Args:
        conn: An active aiosqlite database connection.
        user_id: The ID of the user.

    Returns:
        A dictionary containing the complete, nested user profile, or None if the
        user is not found.
    """
    # The SQL query remains the same as it already fetches all necessary data.
    sql_query = """
        SELECT
            u.id, u.username, u.is_guest, u.is_active,
            r.id as role_id, r.name as role_name, r.rank as role_rank,
            p.id as permission_id, p.name as permission_name, p.description as permission_description
        FROM users u
        LEFT JOIN user_roles ur ON u.id = ur.user_id
        LEFT JOIN roles r ON ur.role_id = r.id
        LEFT JOIN role_permissions rp ON r.id = rp.role_id
        LEFT JOIN permissions p ON rp.permission_id = p.id
        WHERE u.id = ?
        ORDER BY r.rank, r.name, p.name;
    """
    
    # Use the async db utility to execute the query and fetch all results
    rows = await execute_db_query(
        conn,
        sql_query,
        params=(user_id,),
        fetch_all=True
    )

    if not rows:
        return None

    # The processing logic is compatible with aiosqlite.Row objects
    first_row = dict(rows[0])
    user_profile = {
        "id": first_row['id'],
        "username": first_row['username'],
        "is_guest": bool(first_row['is_guest']),
        "is_active": bool(first_row['is_active']),
        "roles": [],
        "permissions": []
    }

    roles_map = {}
    permissions_map = {} 

    for row_data in rows:
        row = dict(row_data)
        
        # Part 1: Build the nested roles list
        if row['role_id'] is not None:
            role_id = row['role_id']
            if role_id not in roles_map:
                roles_map[role_id] = {
                    "id": role_id,
                    "name": row['role_name'],
                    "rank": row['role_rank'],
                    "permissions": []
                }
            
            if row['permission_id'] is not None:
                roles_map[role_id]['permissions'].append({
                    "id": row['permission_id'],
                    "name": row['permission_name'],
                    "description": row['permission_description']
                })

        # Part 2: Build the flat, unique permissions list
        if row['permission_id'] is not None:
            perm_id = row['permission_id']
            if perm_id not in permissions_map:
                permissions_map[perm_id] = {
                    "id": perm_id,
                    "name": row['permission_name'],
                    "description": row['permission_description']
                }

    # Convert the maps to lists for the final profile
    user_profile['roles'] = list(roles_map.values())
    user_profile['permissions'] = list(permissions_map.values())
    
    #logger.debug(f"Constructed user_profile: {user_profile}")
    
    return user_profile



