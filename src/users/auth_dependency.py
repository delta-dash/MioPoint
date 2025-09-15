#auth_dependency.py
from typing import Optional

from fastapi import Depends, HTTPException,status
import jwt

import logging
from src.models import UserProfile
from src.users.auth import TokenRevokedError, authenticate_user_from_token, get_token_from_header_or_cookie

logger = logging.getLogger("Auth")
logger.setLevel(logging.DEBUG)

async def get_current_user_or_guest(
    token: Optional[str] = Depends(get_token_from_header_or_cookie)
) -> UserProfile:
    """
    Primary dependency to get a user identity from a request.
    It authenticates from a token (header or cookie).
    If no valid token is found, it raises a 401 error.
    This should be used by endpoints that can be accessed by guests.
    """
    if token:
        try:
            user = await authenticate_user_from_token(token)
            logger.info(f"Authenticated user '{user.username}' for API request.")
            return user
        except (jwt.PyJWTError, TokenRevokedError, ValueError) as e:
            logger.warning(f"API Token Auth Error: {type(e).__name__} - {e}")
            # Fall through to the exception if token is present but invalid

    # If no token, or if token was invalid, raise 401
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )


# --- NEW: Strict Dependency for Authenticated Users ---
async def get_current_active_user(
    current_user: UserProfile = Depends(get_current_user_or_guest)
) -> UserProfile:
    """
    A stricter dependency for protected routes.
    It ensures the user is fully authenticated (not a guest) and is active.
    """
    if current_user.is_guest:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action is not available to guest users."
        )
    
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Inactive user"
        )
    return current_user