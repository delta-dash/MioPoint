#HelperAuth.py
import aiosqlite
import jwt
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# Assuming user_manager has been updated to be async as well
import logging
from src.db_utils import execute_db_query
from src.profile.profile_info import get_user_profile_with_details_by_id
from src.users.auth_config import SECRET_KEY, ALGORITHM

logger = logging.getLogger("HelperAuth")
logger.setLevel(logging.DEBUG)

async def store_refresh_token(
    user_id: int,
    token: str,
    expires_at: datetime,
    conn: aiosqlite.Connection
) -> None:
    """
    Stores a new refresh token in the database allowlist asynchronously.

    Args:
        user_id: The ID of the user to whom the token belongs.
        token: The encoded JWT refresh token string.
        expires_at: The expiration timestamp of the token.
        conn: The aiosqlite connection for the transaction.
    """
    query = "INSERT INTO refresh_tokens (user_id, token, expires_at) VALUES (?, ?, ?)"
    params = (user_id, token, expires_at)
    try:
        await execute_db_query(conn, query, params)
        logger.info(f"Stored new refresh token for user {user_id}.")
    except aiosqlite.IntegrityError:
        logger.error(
            f"Failed to store refresh token for user {user_id}. It might already exist or the user_id is invalid."
        )
        # Depending on your strategy, you might want to raise an exception here.

async def invalidate_user_sessions(user_id: int, conn: aiosqlite.Connection) -> bool:
    """
    Invalidates all existing sessions for a user by setting the token
    revocation timestamp to the current time. Any JWT issued before this
    time will be considered invalid.

    Args:
        user_id: The ID of the user whose sessions to invalidate.
        conn: An active aiosqlite connection.

    Returns:
        True if the update was successful, False otherwise.
    """
    query = "UPDATE users SET revoke_tokens_before = ? WHERE id = ?"
    params = (datetime.now(timezone.utc), user_id)
    try:
        rowcount = await execute_db_query(conn, query, params)
        if rowcount > 0:
            logger.info(f"All active sessions for user ID {user_id} have been invalidated.")
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to invalidate sessions for user ID {user_id}: {e}")
        return False


async def get_user_from_valid_refresh_token(
    token: str, conn: aiosqlite.Connection
) -> Optional[Dict[str, Any]]:
    """
    Validates a refresh token against the database and the user's revocation
    timestamp asynchronously. Returns the associated user if valid.
    """
    try:
        # Check 1: Does the token exist in our allowlist and is it active?
        query1 = "SELECT user_id FROM refresh_tokens WHERE token = ? AND is_active = 1 AND expires_at > CURRENT_TIMESTAMP"
        result = await execute_db_query(conn, query1, (token,), fetch_one=True)

        if not result:
            logger.warning("Refresh token not found in allowlist, is inactive, or has expired.")
            return None

        user_id = result["user_id"]

        # Check 2: Decode the token to verify its signature and get its 'iat'
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        token_iat = payload.get("iat")
        
        if not token_iat:
            logger.warning(f"Refresh token for user {user_id} is missing 'iat' claim. Invalidating.")
            await invalidate_refresh_token(token, conn)
            return None

        # Check 3: Is the token revoked by a more recent security event?
        query2 = "SELECT revoke_tokens_before FROM users WHERE id = ?"
        user_result = await execute_db_query(conn, query2, (user_id,), fetch_one=True)
        
        if not user_result:
            logger.error(f"Refresh token points to a non-existent user ID {user_id}. Invalidating.")
            await invalidate_refresh_token(token, conn)
            return None
        
        # --- START: REPLACEMENT BLOCK ---
        
        revoke_before_ts = 0.0  # Default to 0, meaning no revocation by default
        
        # FIX: Access the row using square brackets `['key']`, not `.get('key')`
        # Also, check if the value is not None before trying to parse it.
        revoke_timestamp_str = user_result['revoke_tokens_before']

        if revoke_timestamp_str:
            try:
                revoke_before_dt_obj = datetime.fromisoformat(revoke_timestamp_str)
                if revoke_before_dt_obj.tzinfo is None:
                    revoke_before_dt_obj = revoke_before_dt_obj.replace(tzinfo=timezone.utc)
                revoke_before_ts = revoke_before_dt_obj.timestamp()
            except (TypeError, ValueError):
                 logger.error(f"Could not parse 'revoke_tokens_before' value: {revoke_timestamp_str}")
                 return None

        # --- END: REPLACEMENT BLOCK ---
        
        if token_iat < revoke_before_ts:
            logger.warning(f"REVOKED REFRESH TOKEN DETECTED for user {user_id}. Invalidating.")
            await invalidate_refresh_token(token, conn)
            return None

        # If all checks pass, the token is valid.
        user = await get_user_profile_with_details_by_id(conn, user_id)
        if user:
            logger.debug(f"Successfully validated refresh token for user {user['username']} (ID: {user_id}).")
            return user
        
        await invalidate_refresh_token(token, conn)
        return None

    except (jwt.PyJWTError, TypeError) as e:
        logger.warning(f"JWT decoding or processing error for refresh token: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during refresh token validation: {e}")
        return None



async def invalidate_refresh_token(token: str, conn: aiosqlite.Connection) -> bool:
    """
    Invalidates a specific refresh token asynchronously.

    Args:
        token: The refresh token string to invalidate.
        conn: The aiosqlite connection for the transaction.

    Returns:
        True if a token was deleted, False otherwise.
    """
    query = "DELETE FROM refresh_tokens WHERE token = ?"
    rowcount = await execute_db_query(conn, query, (token,))
    if rowcount > 0:
        logger.info("Successfully invalidated a refresh token.")
        return True
    logger.warning("Attempted to invalidate a refresh token that was not found.")
    return False

async def invalidate_all_refresh_tokens_for_user(user_id: int, conn: aiosqlite.Connection) -> int:
    """
    Invalidates ALL refresh tokens for a specific user asynchronously.
    Useful for a "log out everywhere" feature or after a password change.

    Args:
        user_id: The ID of the user whose sessions should be terminated.
        conn: The aiosqlite connection for the transaction.

    Returns:
        The number of tokens that were invalidated.
    """
    query = "DELETE FROM refresh_tokens WHERE user_id = ?"
    invalidated_count = await execute_db_query(conn, query, (user_id,))
    if invalidated_count > 0:
        logger.info(f"Invalidated all {invalidated_count} refresh tokens for user {user_id}.")
    return invalidated_count