# service.py

import datetime
import hashlib
import inspect
import aiosqlite  
from collections import defaultdict
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

import bcrypt

from ConfigMedia import get_config
from src.models import User
from src.log.HelperLog import log_event_async # Using async version of logging
import logging
from src.db_utils import AsyncDBContext, execute_db_query
from src.roles.permissions import Permission
from src.roles.rbac_manager import require_permission
from src.users.user_manager import get_all_user_profiles_with_details, get_user_by_id


logger = logging.getLogger("UserService")
logger.setLevel(logging.DEBUG)
config = get_config()


@require_permission(Permission.USER_VIEW_ANY, caller_id_arg='caller_user_id', target_id_arg='target_user_id')
async def get_user_data_by_id_securely(
    caller_user_id: int,
    target_user_id: int,
    conn: Optional[aiosqlite.Connection] = None
) -> Optional[User]:
    """
    Retrieves a user's data securely as a Pydantic User model.
    Requires caller to have USER_VIEW_ANY permission. Rank checks are handled by the decorator.
    """
    # The decorator handles all security checks (permission and rank).
    # If the decorator passes, we fetch the user data using the async helper.
    user_dict = await get_user_by_id(target_user_id, conn=conn)

    if user_dict:
        try:
            # Convert the dictionary from the database into a Pydantic User model.
            return User(**user_dict)
        except Exception as e:
            logger.error(f"Failed to create User model from data for user {target_user_id}: {e}")
            return None
    return None

@require_permission(Permission.USER_VIEW_ANY, caller_id_arg='caller_user_id')
async def get_all_user_profiles_with_details_securely(
    caller_user_id: int,
    conn: Optional[aiosqlite.Connection] = None
) -> Optional[User]:
    """
    Retrieves a user's data securely as a Pydantic User model.
    Requires caller to have USER_VIEW_ANY permission. Rank checks are handled by the decorator.
    """
    # The decorator handles all security checks (permission and rank).
    # If the decorator passes, we fetch the user data using the async helper.
    user_dict = await get_all_user_profiles_with_details(conn=conn)

    if user_dict:
        try:
            # Convert the dictionary from the database into a Pydantic User model.
            return user_dict
        except Exception as e:
            logger.error(f"Failed to create User model from data for users: {e}")
            return None
    return None

# NOTE: The @require_permission decorator must be async-aware to work correctly with async functions.
@require_permission(Permission.USER_BAN, caller_id_arg='admin_user_id', target_id_arg='target_user_id')
async def set_user_active_status_securely(
    admin_user_id: int,
    target_user_id: int,
    is_active: bool,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Sets the is_active flag for a user securely (ban/unban).
    Requires admin_user_id to have USER_BAN permission and rank hierarchy checks.
    Returns False if the operation failed, the target was invalid, or the target is a guest.
    """
    # The decorator handles permission and rank security checks.
    try:
        async with AsyncDBContext(conn) as db:
            # Additional check: Prevent targeting guest users.
            user_row = await execute_db_query(
                db, "SELECT is_guest FROM users WHERE id = ?", (target_user_id,), fetch_one=True
            )
            if user_row and user_row['is_guest'] == 1:
                logger.warning(f"Attempted to change active status for guest user ID {target_user_id}.")
                return False  # Guests cannot be banned/unbanned via this method.

            # Proceed with setting the status
            update_query = "UPDATE users SET is_active = ? WHERE id = ? AND is_guest = 0"
            rowcount = await execute_db_query(db, update_query, (1 if is_active else 0, target_user_id))

            if rowcount > 0:
                event_type = 'user_unban' if is_active else 'user_ban'
                await log_event_async(
                    conn=db,
                    event_type=event_type,
                    actor_type='user',
                    actor_id=admin_user_id, # Log which admin performed the action
                    target_type='user',
                    target_id=target_user_id,
                    details={'is_active': is_active}
                )
                logger.info(f"Admin {admin_user_id} set user ID {target_user_id} active status to {is_active}.")
                return True
            else:
                logger.warning(f"Could not update active status for user ID {target_user_id}. User not found or status already set.")
                return False
    except Exception as e:
        logger.error(f"Failed to set active status for user {target_user_id}: {e}", exc_info=True)
        return False

