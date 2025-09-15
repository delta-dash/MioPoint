# auth.py

import sqlite3
from cachetools import TTLCache
import jwt
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext

# --- Import from your project modules ---

import logging
from src.db_utils import AsyncDBContext
from src.models import UserProfile
from src.profile.profile_info import get_user_profile_with_details_by_id
from src.users.auth_config import ACCESS_TOKEN_EXPIRE_MINUTES, ALGORITHM, REFRESH_TOKEN_EXPIRE_DAYS, SECRET_KEY


logger = logging.getLogger("Auth")
logger.setLevel(logging.DEBUG)
# ==============================================================================
# --- SETUP AND CONFIGURATION ---
# ==============================================================================

# 1. Password Hashing Setup
# We use passlib's CryptContext to handle password hashing and verification.
# "bcrypt" is the recommended hashing scheme.
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# 2. OAuth2 Password Bearer Scheme
# This creates a dependency that extracts the token from the "Authorization" header.
# It expects the header format to be "Bearer <token>".
# The `tokenUrl` points to the endpoint where a client can obtain a token (your login route).
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/token", auto_error=False)

AUTH_COOKIE_NAME = "auth_token"

revocation_cache = TTLCache(maxsize=1024, ttl=300)

class TokenRevokedError(Exception):
    """Custom exception for revoked tokens for clearer error handling."""
    pass

# ==============================================================================
# --- CORE HELPER FUNCTIONS ---
# ==============================================================================

async def _get_revocation_timestamp(user_id: int) -> float:
    """
    Gets the user's token revocation timestamp, using a cache to avoid DB calls.
    Returns a Unix timestamp.
    """
    # 1. Check cache first
    cached_ts = revocation_cache.get(user_id)
    if cached_ts:
        logger.debug(f"Revocation timestamp for user {user_id} found in cache.")
        return cached_ts

    # 2. If not in cache, hit the database (asynchronously!)
    logger.debug(f"Revocation timestamp for user {user_id} not in cache. Querying DB.")
    async with AsyncDBContext() as conn:
        cursor = await conn.execute("SELECT revoke_tokens_before FROM users WHERE id = ?", (user_id,))
        result = await cursor.fetchone()

    if not result or not result['revoke_tokens_before']:
        # User not found or no revocation set, cache a safe default (0)
        revocation_cache[user_id] = 0.0
        return 0.0
    
    # 3. Parse, convert to timestamp, and cache the result
    revoke_dt = datetime.fromisoformat(result['revoke_tokens_before'])
    # Ensure the datetime is timezone-aware before getting timestamp for consistency
    if revoke_dt.tzinfo is None:
        revoke_dt = revoke_dt.replace(tzinfo=timezone.utc)
        
    revoke_ts = revoke_dt.timestamp()
    revocation_cache[user_id] = revoke_ts
    return revoke_ts

async def authenticate_user_from_token(token: str) -> UserProfile:
    """
    Decodes a slim JWT to get a user ID, validates it, and then fetches
    the full, fresh user profile from the database.
    
    Raises specific errors on failure. Returns a UserProfile on success.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        # --- CHANGE 1: We now expect a slim token with 'sub' (subject) for user ID ---
        user_id = payload.get("sub")
        token_iat = payload.get("iat") # "issued at" timestamp
        
        if not user_id or not token_iat:
            raise jwt.PyJWTError("Token missing required 'sub' or 'iat' claims.")
        
        # Convert user_id to int, as it might be a string from the 'sub' claim
        user_id = int(user_id)

    except (jwt.PyJWTError, ValueError) as e:
        # Re-raise with a consistent type for the middleware to catch
        raise jwt.PyJWTError(f"Token decoding or parsing failed: {e}")


    # --- CHANGE 2: Revocation check is the same, but it's now the *first* check ---
    revoke_before_ts = await _get_revocation_timestamp(user_id)
    if token_iat < revoke_before_ts:
        raise TokenRevokedError(f"Token for user {user_id} was revoked.")

    # --- CHANGE 3: Fetch the full, fresh profile from the database ---
    # This is the core of the new strategy. We no longer trust the token for profile data.
    async with AsyncDBContext() as conn:
        # Assuming you have a function that gets the complete UserProfile data
        # This function needs to join users, user_roles, roles, role_permissions, permissions tables
        full_profile_data = await get_user_profile_with_details_by_id(conn, user_id)

    if not full_profile_data:
        # This can happen if the user was deleted after the token was issued.
        raise ValueError(f"User with ID {user_id} from token not found in database.")

    # --- CHANGE 4: Validate the fetched data with Pydantic ---
    # This ensures the data from the DB matches our UserProfile model structure
    validated_user = UserProfile(**full_profile_data)
    
    return validated_user

async def get_token_from_header_or_cookie(
    request: Request,
    token_from_header: Optional[str] = Depends(oauth2_scheme)
) -> Optional[str]:
    """
    Dependency that extracts a token, prioritizing the Authorization header
    over the authentication cookie.
    """
    if token_from_header:
        logger.debug("Token found in Authorization header.")
        return token_from_header
    
    token_from_cookie = request.cookies.get(AUTH_COOKIE_NAME)
    if token_from_cookie:
        logger.debug("Token found in cookie.")
    return token_from_cookie





def create_refresh_token(data: dict) -> str:
    """Creates a long-lived refresh token."""
    expires = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode = data.copy()
    to_encode.update({"exp": expires, "iat": datetime.now(timezone.utc)})
    encoded_jwt = jwt.encode(payload=to_encode, key=SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

    
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain-text password against a stored hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hashes a plain-text password."""
    return pwd_context.hash(password)


def create_access_token(
    data: dict, expires_delta: Optional[timedelta] = None
) -> str:
    """
    Creates a new JWT access token.

    Args:
        data: The payload to include in the token (e.g., user identifier).
        expires_delta: Optional timedelta for token expiration. Defaults to configured value.

    Returns:
        The encoded JWT as a string.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire, "iat": datetime.now(timezone.utc)}) # Add expiration and "issued at" time
    
    encoded_jwt = jwt.encode(payload=to_encode, key=SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user_optional(
    token: Optional[str] = Depends(get_token_from_header_or_cookie)
) -> Optional[UserProfile]:
    """
    Dependency that returns the authenticated UserProfile if a valid token exists,
    or None if the token is missing or invalid. Does NOT raise an error.
    """
    if not token:
        return None
    try:
        user = await authenticate_user_from_token(token)
        return user
    except (jwt.PyJWTError, TokenRevokedError, ValueError):
        # Any error in validation means no valid user
        return None
