# rbac_manager.py
from functools import wraps
import inspect
from typing import Any, Callable, Dict, List, Optional, Set, Union

import aiosqlite
from ConfigMedia import get_config
import logging
from src.db_utils import AsyncDBContext, execute_db_query
from src.models import UserProfile
from src.roles.permissions import Permission
from src.roles.errors import *

# --- Module-level constants ---
PROTECTED_ROLE_NAMES = ('Admin', 'User', 'Everyone')
EVERYONE_ROLE_RANK = 99999
_PROTECTED_ROLE_IDS: Dict[str, int] = {}

logger = logging.getLogger("RBAC_Manager")
logger.setLevel(logging.DEBUG)
config = get_config()

# --- Helper functions that use the async context ---

def get_admin_role_id() -> int:
    """Synchronously returns the cached Admin role ID. Raises RuntimeError if not initialized."""
    try:
        return _PROTECTED_ROLE_IDS['Admin']
    except KeyError:
        raise RuntimeError("Protected role IDs have not been initialized. Call initialize_protected_role_ids() at startup.")

def get_everyone_role_id() -> int:
    """Synchronously returns the cached Everyone role ID. Raises RuntimeError if not initialized."""
    try:
        return _PROTECTED_ROLE_IDS['Everyone']
    except KeyError:
        raise RuntimeError("Protected role IDs have not been initialized. Call initialize_protected_role_ids() at startup.")
    
async def get_permission_id(name: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[int]:
    query = "SELECT id FROM permissions WHERE name = ?"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (name,), fetch_one=True)
        return result[0] if result else None

async def get_role_id(name: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[int]:
    query = "SELECT id FROM roles WHERE name = ?"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (name,), fetch_one=True)
        return result[0] if result else None

async def get_permission_by_id(permission_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    query = "SELECT id, name, description FROM permissions WHERE id = ?"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (permission_id,), fetch_one=True)
        return dict(result) if result else None

async def get_role_by_id(role_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    query = "SELECT id, name, rank FROM roles WHERE id = ?"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (role_id,), fetch_one=True)
        return dict(result) if result else None
async def get_role_by_name(name: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    query = "SELECT id, name, rank FROM roles WHERE name = ?"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (name,), fetch_one=True)
        return dict(result) if result else None
# --- Initial setup ---

async def setup_roles_database(conn: Optional[aiosqlite.Connection] = None):
    """Creates or updates the RBAC database schema. Always manages its own connection."""
    try:
        async with AsyncDBContext(conn) as db:
            await db.execute("PRAGMA foreign_keys = ON;")
            
            await db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE, password_hash TEXT);")
            await db.execute("CREATE TABLE IF NOT EXISTS roles (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, rank INTEGER NOT NULL DEFAULT 99999);")
            await db.execute("CREATE TABLE IF NOT EXISTS user_roles (user_id INTEGER NOT NULL, role_id INTEGER NOT NULL, FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE, FOREIGN KEY (role_id) REFERENCES roles (id) ON DELETE CASCADE, PRIMARY KEY (user_id, role_id));")
            await db.execute("CREATE TABLE IF NOT EXISTS permissions (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, description TEXT);")
            await db.execute("CREATE TABLE IF NOT EXISTS role_permissions (role_id INTEGER NOT NULL, permission_id INTEGER NOT NULL, FOREIGN KEY (role_id) REFERENCES roles (id) ON DELETE CASCADE, FOREIGN KEY (permission_id) REFERENCES permissions (id) ON DELETE CASCADE, PRIMARY KEY (role_id, permission_id));")
            
            # Using the enum's description attribute if it exists, otherwise a default string.
            default_permissions_data = { p: getattr(p, 'description', f'Allows {p.value.replace(":", " ")}.') for p in Permission }
            for perm, desc in default_permissions_data.items():
                await db.execute("INSERT OR IGNORE INTO permissions (name, description) VALUES (?, ?)", (perm.value, desc))
            
            await db.commit()
            logger.debug("Database schema setup complete.")
    except aiosqlite.Error as e:
        logger.error(f"Database error during setup: {e}", exc_info=True)
        raise RBACError(f"Database setup failed: {e}") from e

async def setup_initial_roles_and_permissions(conn: Optional[aiosqlite.Connection] = None):
    """Sets up 'Admin', 'User', and 'Everyone' roles. Idempotent and async."""
    async def _get_or_create_role_id(name: str, rank: int, db: aiosqlite.Connection) -> int:
        await db.execute("INSERT OR IGNORE INTO roles (name, rank) VALUES (?, ?)", (name, rank))
        res = await db.execute("SELECT id FROM roles WHERE name = ?", (name,))
        row = await res.fetchone()
        if not row: raise RBACError(f"Failed to create or find role '{name}'")
        return row[0]

    async with AsyncDBContext(conn) as db:
        admin_role_id = await _get_or_create_role_id('Admin', 0, db)
        user_role_id = await _get_or_create_role_id('User', 100, db)
        everyone_role_id = await _get_or_create_role_id('Everyone', EVERYONE_ROLE_RANK, db)
        
        all_permissions = await get_all_permissions(conn=db)
        all_perm_ids = {p['id'] for p in all_permissions}
        
        for perm_id in all_perm_ids:
            await db.execute("INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)", (admin_role_id, perm_id))
        
        # Ensure new admin permissions are explicitly granted to Admin role
        admin_specific_permissions = (Permission.ADMIN_CONFIG_VIEW, Permission.ADMIN_CONFIG_EDIT)
        for perm in admin_specific_permissions:
            perm_id = await get_permission_id(perm.value, conn=db)
            if perm_id:
                await db.execute("INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)", (admin_role_id, perm_id))
        
        basic_user_permissions = (Permission.FILE_VIEW, Permission.FILE_SEARCH, Permission.FILE_ADD)
        for perm in basic_user_permissions:
            perm_id = await get_permission_id(perm.value, conn=db)
            if perm_id:
                await db.execute("INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)", (user_role_id, perm_id))

        everyone_permissions = (Permission.FILE_VIEW, Permission.FILE_SEARCH, Permission.REACTION_ADD, Permission.CHAT_CONVERSATION_VIEW_PUBLIC, Permission.CHAT_SEND_MESSAGE, Permission.USER_VIEW_PUBLIC_PROFILE)
        for perm in everyone_permissions:
            perm_id = await get_permission_id(perm.value, conn=db)
            if perm_id:
                await db.execute("INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)", (everyone_role_id, perm_id))
        
        logger.info("Initial 'Admin', 'User', and 'Everyone' roles and permissions have been set up.")
        

async def initialize_protected_role_ids(conn: Optional[aiosqlite.Connection] = None):
    """
    Fetches the IDs for all protected roles and caches them in memory.
    This should be called once when the application starts.
    """
    global _PROTECTED_ROLE_IDS
    async with AsyncDBContext(conn) as db:
        for role_name in PROTECTED_ROLE_NAMES:
            role_id = await get_role_id(role_name, conn=db)
            if role_id is None:
                raise RBACError(f"CRITICAL: Protected role '{role_name}' not found in the database during initialization.")
            _PROTECTED_ROLE_IDS[role_name] = role_id
    logger.info(f"Protected role IDs have been cached: {_PROTECTED_ROLE_IDS}")

# --- Role and Permission Management ---

async def create_role(name: str, rank: int, conn: Optional[aiosqlite.Connection] = None) -> int:
    if name in PROTECTED_ROLE_NAMES:
        raise ProtectedRoleError(f"Cannot create a role with a protected name: '{name}'.")
    query = "INSERT INTO roles (name, rank) VALUES (?, ?)"
    async with AsyncDBContext(conn) as db:
        try:
            return await execute_db_query(db, query, (name, rank), return_last_row_id=True)
        except aiosqlite.IntegrityError:
            raise RoleAlreadyExistsError(f"A role with the name '{name}' already exists.")

async def edit_role(role_id: int, new_name: Optional[str] = None, new_rank: Optional[int] = None, permissions_to_set: Optional[Set[int]] = None, conn: Optional[aiosqlite.Connection] = None) -> bool:
    if new_name is None and new_rank is None and permissions_to_set is None: return True

    async with AsyncDBContext(conn) as db:
        role_data = await get_role_by_id(role_id, conn=db)
        if not role_data: raise RoleNotFoundError(f"Role with ID {role_id} not found.")
        
        current_role_name = role_data['name']
        if new_name is not None and current_role_name in PROTECTED_ROLE_NAMES and new_name != current_role_name:
            raise ProtectedRoleError(f"Cannot rename protected role '{current_role_name}'.")
        
        try:
            updates, params = [], []
            if new_name is not None:
                updates.append("name = ?")
                params.append(new_name)
            if new_rank is not None:
                updates.append("rank = ?")
                params.append(new_rank)
            
            if updates:
                await db.execute(f"UPDATE roles SET {', '.join(updates)} WHERE id = ?", tuple(params + [role_id]))

            if permissions_to_set is not None:
                await db.execute("DELETE FROM role_permissions WHERE role_id = ?", (role_id,))
                if permissions_to_set:
                    await db.executemany("INSERT INTO role_permissions (role_id, permission_id) VALUES (?, ?)", [(role_id, pid) for pid in permissions_to_set])
            
            logger.info(f"Successfully edited role ID {role_id}.")
            return True
        except aiosqlite.IntegrityError:
            raise RoleAlreadyExistsError(f"Failed to edit role ID {role_id}. A role with the name '{new_name}' likely already exists.")

async def delete_role(role_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    async with AsyncDBContext(conn) as db:
        role_data = await get_role_by_id(role_id, conn=db)
        if not role_data: raise RoleNotFoundError(f"Role with ID {role_id} not found.")
        if role_data['name'] in PROTECTED_ROLE_NAMES: raise ProtectedRoleError(f"Cannot delete protected role: '{role_data['name']}'.")
        
        rowcount = await execute_db_query(db, "DELETE FROM roles WHERE id = ?", (role_id,))
        return rowcount > 0

async def grant_permission_to_role(role_id: int, permission_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    async with AsyncDBContext(conn) as db:
        if await get_role_by_id(role_id, conn=db) is None: raise RoleNotFoundError(f"Role ID {role_id} not found.")
        if await get_permission_by_id(permission_id, conn=db) is None: raise PermissionNotFoundError(f"Permission ID {permission_id} not found.")

        query = "INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)"
        rowcount = await execute_db_query(db, query, (role_id, permission_id))
        return rowcount > 0

async def revoke_permission_from_role(role_id: int, permission_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    async with AsyncDBContext(conn) as db:
        query = "DELETE FROM role_permissions WHERE role_id = ? AND permission_id = ?"
        rowcount = await execute_db_query(db, query, (role_id, permission_id))
        return rowcount > 0

# --- User-Role Assignment ---

async def assign_role_to_user(user_id: int, role_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    async with AsyncDBContext(conn) as db:
        if await get_role_by_id(role_id, conn=db) is None: raise RoleNotFoundError(f"Role ID {role_id} not found.")
        
        query = "INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)"
        rowcount = await execute_db_query(db, query, (user_id, role_id))
        return rowcount > 0

async def remove_role_from_user(user_id: int, role_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    async with AsyncDBContext(conn) as db:
        query = "DELETE FROM user_roles WHERE user_id = ? AND role_id = ?"
        rowcount = await execute_db_query(db, query, (user_id, role_id))
        return rowcount > 0

# --- Permission Checking & Querying ---

async def get_user_permissions(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Set[str]:
    query = "SELECT DISTINCT p.name FROM permissions p JOIN role_permissions rp ON p.id = rp.permission_id JOIN user_roles ur ON rp.role_id = ur.role_id WHERE ur.user_id = ?"
    async with AsyncDBContext(conn) as db:
        results = await execute_db_query(db, query, (user_id,), fetch_all=True)
        return {row['name'] for row in results}

async def user_has_permission(user_id: int, permission_name: Union[str, Permission], conn: Optional[aiosqlite.Connection] = None) -> bool:
    permission_str = permission_name.value if isinstance(permission_name, Permission) else permission_name
    query = "SELECT 1 FROM user_roles ur JOIN role_permissions rp ON ur.role_id = rp.role_id JOIN permissions p ON rp.permission_id = p.id WHERE ur.user_id = ? AND p.name = ? LIMIT 1"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (user_id, permission_str), fetch_one=True)
        return result is not None

async def get_user_highest_rank(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> int:
    query = "SELECT MIN(r.rank) FROM roles r JOIN user_roles ur ON r.id = ur.role_id WHERE ur.user_id = ?"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (user_id,), fetch_one=True)
        return result[0] if result and result[0] is not None else EVERYONE_ROLE_RANK + 1

# --- Utility/Helper Queries ---

async def get_role_details_raw(role_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[aiosqlite.Row]:
    query = """
    SELECT r.id AS role_id, r.name AS role_name, r.rank AS role_rank,
           p.id AS permission_id, p.name AS permission_name, p.description AS permission_description
    FROM roles r
    LEFT JOIN role_permissions rp ON r.id = rp.role_id
    LEFT JOIN permissions p ON rp.permission_id = p.id
    WHERE r.id = ? ORDER BY r.rank, r.name, p.name
    """
    async with AsyncDBContext(conn) as db:
        return await execute_db_query(db, query, (role_id,), fetch_all=True)

async def get_all_roles(conn: Optional[aiosqlite.Connection] = None) -> List[aiosqlite.Row]:
    query = "SELECT id, name, rank FROM roles ORDER BY rank, name"
    async with AsyncDBContext(conn) as db:
        return await execute_db_query(db, query, fetch_all=True)

async def get_all_permissions(conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    query = "SELECT id, name, description FROM permissions ORDER BY name"
    async with AsyncDBContext(conn) as db:
        results = await execute_db_query(db, query, fetch_all=True)
        return [dict(row) for row in results]

async def get_roles_for_user(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    query = "SELECT r.id, r.name, r.rank FROM roles r JOIN user_roles ur ON r.id = ur.role_id WHERE ur.user_id = ? ORDER BY r.rank, r.name"
    async with AsyncDBContext(conn) as db:
        results = await execute_db_query(db, query, (user_id,), fetch_all=True)
        return [dict(row) for row in results]

async def check_if_admin_exists(conn: Optional[aiosqlite.Connection] = None) -> bool:
    """
    Checks if at least one user is assigned the 'Admin' role.
    This check is optimized to use the cached Admin role ID.
    
    Returns:
        True if an admin user exists, False otherwise.
    """
    try:
        # Use the fast, cached Admin role ID after initialization
        admin_role_id = get_admin_role_id()
    except RuntimeError as e:
        logger.error(f"Cannot check for admin users because protected roles are not initialized: {e}")
        # If roles aren't initialized, we can't possibly have an admin user yet.
        return False

    query = "SELECT 1 FROM user_roles WHERE role_id = ? LIMIT 1;"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (admin_role_id,), fetch_one=True)
        return result is not None

# --- The 'require_permission' decorator (for async functions) ---

def require_permission(
    permission: Union[str, Permission],
    caller_id_arg: str = 'user_id',
    target_id_arg: Optional[str] = None
):
    """
    An async decorator to enforce permission and rank-based access control.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            caller_user_id = bound_args.arguments.get(caller_id_arg)
            if caller_user_id is None:
                raise ValueError(f"Decorator error: Arg '{caller_id_arg}' not found in '{func.__name__}'.")

            # Check for the required permission
            has_perm = await user_has_permission(caller_user_id, permission)
            if not has_perm:
                perm_str = permission.value if isinstance(permission, Permission) else permission
                logger.warning(f"Permission Denied: User {caller_user_id} lacks '{perm_str}' for '{func.__name__}'.")
                raise PermissionDeniedError(f"You do not have permission to perform this action ('{perm_str}').")

            # Check rank hierarchy if required
            if target_id_arg:
                target_user_id = bound_args.arguments.get(target_id_arg)
                if target_user_id is None:
                    raise ValueError(f"Decorator error: Arg '{target_id_arg}' not found in '{func.__name__}'.")
                
                if caller_user_id != target_user_id:
                    caller_rank = await get_user_highest_rank(caller_user_id)
                    target_rank = await get_user_highest_rank(target_user_id)
                    if caller_rank >= target_rank:
                        logger.warning(f"Permission Denied: User {caller_user_id} (rank {caller_rank}) tried to modify User {target_user_id} (rank {target_rank}).")
                        raise PermissionDeniedError("You cannot modify a user with an equal or higher rank than your own.")

            # If all checks pass, execute the original async function
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def require_permission_by_id(
    permission: Union[str, Permission],
    user_arg: str = 'user_id',
    target_id_arg: Optional[str] = None
):
    """
    Decorator to enforce permissions and rank hierarchy by performing an
    ASYNC DATABASE CHECK.

    It checks if the caller has the required permission and, if a `target_id_arg`
    is provided, ensures the caller has a higher rank (lower rank number)
    than the target.

    Args:
        permission: The permission required to access the endpoint.
        user_arg: The name of the function argument holding the caller's identity.
        target_id_arg: The name of the function argument holding the target's identity.
    """
    perm_str = permission.value if isinstance(permission, Permission) else permission

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # --- 1. Get Caller ID ---
            caller_arg = bound_args.arguments.get(user_arg)
            if caller_arg is None:
                raise ValueError(f"Decorator error: Arg '{user_arg}' not found in '{func.__name__}'.")
            caller_user_id = getattr(caller_arg, 'id', caller_arg)
            if not isinstance(caller_user_id, int):
                raise TypeError(f"Decorator argument '{user_arg}' must resolve to an integer user ID.")

            # --- 2. Check Permission ---
            if not await user_has_permission(caller_user_id, perm_str):
                logger.warning(f"DB Check Denied: User {caller_user_id} lacks '{perm_str}' for '{func.__name__}'.")
                raise PermissionDeniedError(f"You do not have permission to perform this action ('{perm_str}').")

            # --- 3. Check Rank Hierarchy (if applicable) ---
            if target_id_arg:
                target_arg = bound_args.arguments.get(target_id_arg)
                if target_arg is None:
                    raise ValueError(f"Decorator error: Arg '{target_id_arg}' not found in '{func.__name__}'.")
                target_user_id = getattr(target_arg, 'id', target_arg)
                if not isinstance(target_user_id, int):
                    raise TypeError(f"Decorator argument '{target_id_arg}' must resolve to an integer user ID.")

                # A user can always edit themselves, so skip rank check
                if caller_user_id != target_user_id:
                    caller_rank = await get_user_highest_rank(caller_user_id)
                    target_rank = await get_user_highest_rank(target_user_id)
                    if caller_rank >= target_rank:
                        logger.warning(f"Rank Check Denied: User {caller_user_id} (rank {caller_rank}) tried to modify User {target_user_id} (rank {target_rank}).")
                        raise PermissionDeniedError("You cannot modify a user with an equal or higher rank than your own.")

            return await func(*args, **kwargs)
        return wrapper
    return decorator


# --- Decorator 2: Fast, In-Memory Check from a Pre-loaded User Profile ---

def require_permission_from_profile(
    permission: Union[str, Permission],
    profile_arg: str = 'current_user'
):
    """
    Decorator for a FAST, SYNCHRONOUS PERMISSION CHECK on a UserProfile object.

    This is highly efficient as it uses the `permissions` list already loaded
    on the user model, avoiding any new database calls. It does NOT perform
    rank checking. For rank checks, use `require_permission_by_id`.

    Args:
        permission: The permission required to access the endpoint.
        profile_arg: The name of the function argument that holds the UserProfile object.
    """
    perm_str = permission.value if isinstance(permission, Permission) else permission

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            user_profile = bound_args.arguments.get(profile_arg)
            if not isinstance(user_profile, UserProfile) or not hasattr(user_profile, 'permissions'):
                raise TypeError(f"Decorator argument '{profile_arg}' must be a UserProfile object with a 'permissions' attribute.")

            user_permissions = {p.name for p in user_profile.permissions}
            if perm_str not in user_permissions:
                logger.warning(f"Profile Check Denied: User {user_profile.id} lacks '{perm_str}' for '{func.__name__}'.")
                raise PermissionDeniedError(f"You do not have permission to perform this action ('{perm_str}').")

            return await func(*args, **kwargs)
        return wrapper
    return decorator