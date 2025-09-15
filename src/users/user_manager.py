#user_manager.py

"""
User and User-Interaction Helper Module (Async)

This module provides async functions for managing user accounts, authentication,
and IP bans. All database operations use AsyncDBContext for transactional integrity,
allowing them to either manage their own connection or use one provided by the caller.
"""
import datetime
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
import bcrypt
from ConfigMedia import get_config
from src.models import UserCreate
from src.log.HelperLog import log_event_async
import logging
from src.db_utils import AsyncDBContext, execute_db_query
from src.roles.errors import RBACError, RoleNotFoundError
from src.roles.rbac_manager import assign_role_to_user

logger = logging.getLogger("UserManager")
logger.setLevel(logging.DEBUG)
config = get_config()

# ==============================================================================
# --- DATABASE SETUP (Run once at startup) ---
# ==============================================================================

async def setup_user_database(conn: Optional[aiosqlite.Connection] = None):
    """Creates or updates the database schema for users, and banned IPs."""
    async with AsyncDBContext(conn) as db:
        await execute_db_query(db, "PRAGMA foreign_keys = ON;")
        await execute_db_query(db, """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE,
                hashed_password BLOB, is_guest INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                revoke_tokens_before TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
            );
        """)
        await execute_db_query(db, """
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, token TEXT NOT NULL UNIQUE,
                expires_at TIMESTAMP NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_active INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            );
        """)
        await execute_db_query(db, """
            CREATE TABLE IF NOT EXISTS banned_ips (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ip_address TEXT NOT NULL, reason TEXT,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, expires_at TIMESTAMP, is_active INTEGER NOT NULL DEFAULT 1
            );
        """)


    logger.debug("Database schema is verified.")

# ==============================================================================
# --- USER MANAGEMENT HELPERS (Async) ---
# ==============================================================================

async def create_user(usercreate: UserCreate, conn: Optional[aiosqlite.Connection] = None) -> Optional[int]:
    """
    Creates a new non-guest user and assigns the 'Everyone' role.
    """
    username = usercreate.username
    password = usercreate.password
    if not username or not password or username.startswith("guest_"):
        logger.warning("Username is invalid or reserved.")
        return None
    
    lower_username = username.lower()
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    
    try:
        async with AsyncDBContext(conn) as db:
            # Insert the user
            insert_query = "INSERT INTO users (username, hashed_password) VALUES (?, ?)"
            new_user_id = await execute_db_query(db, insert_query, (lower_username, hashed_password), return_last_row_id=True)
            
            # --- RBAC Integration: Assign 'Everyone' role ---
            role_query = "SELECT id FROM roles WHERE name = 'Everyone'"
            everyone_role_row = await execute_db_query(db, role_query, fetch_one=True)
            
            if everyone_role_row:
                everyone_role_id = everyone_role_row['id']
                await assign_role_to_user(new_user_id, everyone_role_id, conn=db)
            else:
                logger.warning(f"Could not find 'Everyone' role to assign to new user {new_user_id}.")

            await log_event_async(
                conn=db, event_type='user_create', actor_type='system', actor_name='api',
                target_type='user', target_id=new_user_id, details={'username': lower_username}
            )
            
            logger.info(f"Successfully created user '{lower_username}' with ID {new_user_id}.")
            return new_user_id
            
    except aiosqlite.IntegrityError:
        logger.warning(f"Failed to create user. Username '{lower_username}' already exists.")
        return None
    except Exception as e:
        logger.error(f"Error creating user '{lower_username}': {e}", exc_info=True)
        return None

async def verify_user(username: str, password: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[int]:
    """Verifies a user's credentials against the database."""
    query = "SELECT id, hashed_password FROM users WHERE username = ? AND is_guest = 0 AND is_active = 1"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (username.lower(),), fetch_one=True)
    
    if result:
        user_id, hashed_password_from_db = result['id'], result['hashed_password']
        if hashed_password_from_db and bcrypt.checkpw(password.encode('utf-8'), hashed_password_from_db):
            logger.debug(f"User '{username}' successfully verified.")
            return user_id
            
    logger.warning(f"Failed verification for user '{username}'.")
    return None

async def get_or_create_guest_user(
    client_ip: str, 
    conn: Optional[aiosqlite.Connection] = None
) -> Optional[Tuple[Dict[str, Any], bool]]: # <--- CHANGE 1: Update the return type hint
    """
    Retrieves an existing guest user or creates a new one.
    
    Returns:
        A tuple containing (user_data_dict, created_boolean) if successful,
        otherwise None. The boolean is True if a new user was created in this call.
    """
    if not client_ip:
        logger.warning("Attempted to get or create a guest user without providing an IP address.")
        return None

    ip_hash = hashlib.sha256(client_ip.encode('utf-8')).hexdigest()
    guest_username = f"guest_{ip_hash}"

    async with AsyncDBContext(conn) as db:
        try:
            # 1. Attempt to find an existing, active guest user.
            query = "SELECT id, username, is_guest, is_active FROM users WHERE username = ? AND is_guest = 1 AND is_active = 1"
            existing_user = await execute_db_query(db, query, (guest_username,), fetch_one=True)

            if existing_user:
                logger.debug(f"Found existing guest user '{guest_username}' with ID {existing_user['id']}.")
                # --- CHANGE 2: Return the user and False (wasn't created now) ---
                return dict(existing_user), False

            # 2. If no active user is found, create a new one.
            logger.info(f"Creating new guest user for IP hash: {guest_username}")
            
            insert_query = "INSERT INTO users (username, is_guest) VALUES (?, 1)"
            new_user_id = await execute_db_query(db, insert_query, (guest_username,), return_last_row_id=True)
            
            if not new_user_id:
                raise Exception(f"Failed to get new user ID after inserting {guest_username}.")

            # ... [Your RBAC and logging logic remains the same] ...
            try:
                role_query = "SELECT id FROM roles WHERE name = 'Everyone'"
                everyone_role_row = await execute_db_query(db, role_query, fetch_one=True)
                if everyone_role_row:
                    await assign_role_to_user(new_user_id, everyone_role_row['id'], conn=db)
            except Exception as e:
                logger.error(f"RBAC error assigning 'Everyone' role to {new_user_id}: {e}", exc_info=True)
            
            await log_event_async(
                conn=db, event_type='guest_user_create', actor_type='system', actor_name='api',
                target_type='user', target_id=new_user_id,
                details={'username': guest_username, 'ip_hash': ip_hash}
            )

            new_user_data = await get_user_by_id(new_user_id, conn=db)
            if not new_user_data:
                # This is a fallback, should not happen in normal operation
                logger.error(f"CRITICAL: Could not fetch newly created guest user {new_user_id}.")
                return None
            
            # --- CHANGE 3: Return the new user and True (was just created) ---
            return new_user_data, True

        except aiosqlite.IntegrityError:
            # Race condition: another request created the user just now. Fetch it.
            logger.warning(f"Race condition on creating '{guest_username}'. Re-fetching user.")
            user = await get_user_by_name(guest_username, conn=db)
            # --- CHANGE 4: Return the user and False (this *request* didn't create it) ---
            return (user, False) if user else None
        
        except Exception as e:
            logger.error(f"An unexpected error occurred while getting or creating guest user '{guest_username}': {e}", exc_info=True)
            return None



async def set_user_active_status(user_id: int, is_active: bool, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Sets the is_active flag for a user (for banning/unbanning)."""
    query = "UPDATE users SET is_active = ? WHERE id = ? AND is_guest = 0"
    async with AsyncDBContext(conn) as db:
        rowcount = await execute_db_query(db, query, (1 if is_active else 0, user_id))
    return rowcount > 0

async def get_user_by_id(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """Retrieves a user's public data by their ID."""
    query = "SELECT id, username, is_guest, is_active FROM users WHERE id = ?"
    async with AsyncDBContext(conn) as db:
        row = await execute_db_query(db, query, (user_id,), fetch_one=True)
    return dict(row) if row else None

async def get_user_by_name(username: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """Retrieves a user's public data by their username."""
    query = "SELECT id, username, is_guest, is_active FROM users WHERE username = ?"
    async with AsyncDBContext(conn) as db:
        row = await execute_db_query(db, query, (username.lower(),), fetch_one=True)
    return dict(row) if row else None

async def ban_user(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Bans a user by setting their 'is_active' status to False."""
    async with AsyncDBContext(conn) as db:
        banned = await set_user_active_status(user_id, is_active=False, conn=db)
        if banned:
            user_data = await get_user_by_id(user_id, conn=db)
            if user_data:
                await log_event_async(
                    conn=db, event_type='user_ban', actor_type='system', actor_name='admin_api',
                    target_type='user', target_id=user_id, details={'username': user_data.get('username', 'N/A')}
                )
                logger.info(f"User ID {user_id} ('{user_data.get('username', 'N/A')}') has been banned.")
        return banned

async def unban_user(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Unbans a user by setting their 'is_active' status to True."""
    async with AsyncDBContext(conn) as db:
        unbanned = await set_user_active_status(user_id, is_active=True, conn=db)
        if unbanned:
            user_data = await get_user_by_id(user_id, conn=db)
            if user_data:
                await log_event_async(
                    conn=db, event_type='user_unban', actor_type='system', actor_name='admin_api',
                    target_type='user', target_id=user_id, details={'username': user_data.get('username', 'N/A')}
                )
                logger.info(f"User ID {user_id} ('{user_data.get('username', 'N/A')}') has been unbanned.")
        return unbanned

# ==============================================================================
# --- IP BANNING HELPERS (Async) ---
# ==============================================================================

async def ban_ip_address(ip_address: str, reason: Optional[str] = None, duration_seconds: Optional[int] = None, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Adds or updates an IP address ban."""
    if not ip_address: return False
    
    expires_at = datetime.datetime.now() + datetime.timedelta(seconds=duration_seconds) if duration_seconds else None
    
    async with AsyncDBContext(conn) as db:
        current_ban = await get_ip_ban_status(ip_address, conn=db)
        if current_ban:
            query = "UPDATE banned_ips SET reason = ?, expires_at = ?, is_active = 1 WHERE ip_address = ?"
            await execute_db_query(db, query, (reason, expires_at, ip_address))
            logger.info(f"Updated ban for IP address '{ip_address}'.")
        else:
            query = "INSERT INTO banned_ips (ip_address, reason, expires_at, is_active) VALUES (?, ?, ?, 1)"
            new_ban_id = await execute_db_query(db, query, (ip_address, reason, expires_at), return_last_row_id=True)
            await log_event_async(
                conn=db, event_type='ip_ban', actor_type='system', actor_name='admin_api',
                target_type='ip_ban', target_id=new_ban_id,
                details={'ip_address': ip_address, 'reason': reason, 'expires_at': str(expires_at)}
            )
            logger.info(f"IP address '{ip_address}' has been banned.")
    return True

async def unban_ip_address(ip_address: str, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Marks all active bans for an IP address as inactive."""
    if not ip_address: return False
    
    query = "UPDATE banned_ips SET is_active = 0 WHERE ip_address = ? AND is_active = 1"
    async with AsyncDBContext(conn) as db:
        rowcount = await execute_db_query(db, query, (ip_address,))
    
    if rowcount > 0:
        logger.info(f"Successfully deactivated {rowcount} active ban(s) for IP address '{ip_address}'.")
        return True
    return False

async def get_ip_ban_status(ip_address: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """Retrieves the active ban status for a given IP address."""
    if not ip_address: return None
    
    query = "SELECT * FROM banned_ips WHERE ip_address = ? AND is_active = 1 AND (expires_at IS NULL OR expires_at > ?) ORDER BY expires_at ASC"
    async with AsyncDBContext(conn) as db:
        row = await execute_db_query(db, query, (ip_address, datetime.datetime.now()), fetch_one=True)
    return dict(row) if row else None

async def is_ip_banned(ip_address: str, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Checks if a given IP address is currently banned, cleaning up expired bans first."""
    if not ip_address: return False
    
    async with AsyncDBContext(conn) as db:
        # Cleanup expired temporary bans
        cleanup_query = "UPDATE banned_ips SET is_active = 0 WHERE is_active = 1 AND expires_at IS NOT NULL AND expires_at <= ?"
        await execute_db_query(db, cleanup_query, (datetime.datetime.now(),))
        
        # Check for any remaining active ban
        status = await get_ip_ban_status(ip_address, conn=db)
    
    return status is not None

async def get_all_user_profiles_with_details(conn: aiosqlite.Connection) -> List[Dict[str, Any]]:
    """
    Retrieves a list of all users' complete profiles asynchronously, including
    roles and a consolidated list of permissions for each user.
    
    Args:
        conn: An active aiosqlite database connection.

    Returns:
        A list of dictionaries, where each dictionary is a complete user profile.
    """
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
        ORDER BY u.id, r.rank, r.name, p.name;
    """
    
    rows = await execute_db_query(conn, sql_query, fetch_all=True)
    
    if not rows:
        return []

    # Use a dictionary to aggregate data for each user
    users_map = {}

    for row_data in rows:
        row = dict(row_data)
        user_id = row['id']
        
        # If we haven't seen this user yet, create their base profile
        if user_id not in users_map:
            users_map[user_id] = {
                "id": user_id,
                "username": row['username'],
                "is_guest": bool(row['is_guest']),
                "is_active": bool(row['is_active']),
                "roles": [],
                "permissions": set(),  # Use a set for unique permissions
                "_roles_map": {}     # Internal helper to track roles
            }
        
        user_profile = users_map[user_id]
        
        # Process roles
        if row['role_id'] and row['role_id'] not in user_profile['_roles_map']:
            user_profile['_roles_map'][row['role_id']] = {
                "id": row['role_id'],
                "name": row['role_name'],
                "rank": row['role_rank'],
                "permissions": []
            }
            user_profile['roles'].append(user_profile['_roles_map'][row['role_id']])

        # Process permissions for roles and the flat list
        if row['permission_id']:
            permission_data = (
                row['permission_id'],
                row['permission_name'],
                row['permission_description']
            )
            # Add to the user's flat permission list
            user_profile['permissions'].add(permission_data)
            
            # Add to the specific role's permission list
            if row['role_id']:
                 user_profile['_roles_map'][row['role_id']]['permissions'].append({
                    "id": row['permission_id'],
                    "name": row['permission_name'],
                    "description": row['permission_description']
                })

    # Final cleanup and formatting
    final_user_list = []
    for user_id, profile in users_map.items():
        # Convert the set of permission tuples to a list of dicts
        profile['permissions'] = [
            {"id": pid, "name": pname, "description": pdesc} 
            for pid, pname, pdesc in sorted(list(profile['permissions']))
        ]
        del profile['_roles_map']  # Remove the internal helper map
        final_user_list.append(profile)
        
    return final_user_list