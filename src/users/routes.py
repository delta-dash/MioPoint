# routes/UserRoutes.py

from typing import List, Optional
import aiosqlite 
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status, Path
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field, ValidationError

# --- Import from project modules ---

# --- NEW: Import the async DB dependency ---
import logging
from src.db_utils import get_db_connection

# Assuming these service functions have been updated to be async and accept a connection object
from src.models import Token, User, UserCreate, UserProfile
from src.profile.profile_info import get_current_active_user_profile, get_user_profile_with_details_by_id
from src.roles.errors import PermissionDeniedError
from src.users.auth_dependency import get_current_active_user, get_current_user_or_guest
from src.users.service import get_all_user_profiles_with_details_securely, get_user_data_by_id_securely, set_user_active_status_securely
from src.users.user_manager import create_user, get_or_create_guest_user, get_user_by_id, verify_user

# Secure business logic layer
from src.users.HelperAuth import get_user_from_valid_refresh_token, invalidate_refresh_token, invalidate_user_sessions, store_refresh_token
# Core authentication logic (JWT, passwords, dependencies)
from src.users.auth import (
    AUTH_COOKIE_NAME,
    create_access_token,
    create_refresh_token,
    get_current_user_optional,
    
)
from src.users.auth_config import ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS
from server import rate_limiter_dependency
from common import IS_PRODUCTION

# Pydantic models for data validation

logger = logging.getLogger("UserRoutes")
logger.setLevel(logging.DEBUG)

# ==============================================================================
# --- Pydantic Models for Admin Routes ---
# ==============================================================================
class UserStatusUpdateRequest(BaseModel):
    is_active: bool = Field(..., description="The new active status for the user.")


ACCESS_TOKEN_EXPIRE_HOURS = 1

# ==============================================================================
# --- 1. Authentication Router (Public Facing) ---
# ==============================================================================

auth_router = APIRouter(prefix="/api/auth", tags=["Authentication"])


# File: tools/users/routes.py

@auth_router.post("/session/init", response_model=UserProfile, summary="Initialize a user or guest session")
async def initialize_session(
    request: Request,
    response: Response,
    conn: aiosqlite.Connection = Depends(get_db_connection),
    # Use the new dependency that doesn't throw 401
    current_user: Optional[UserProfile] = Depends(get_current_user_optional)
):
    """
    The primary endpoint for a client app to initialize its session.
    1. If a valid token exists, it returns the current user's profile.
    2. If no valid token exists, it provisions a new guest session, sets the auth cookie,
       and returns the new guest profile.
    """
    if current_user:
        logger.info(f"Session initialized for existing user: {current_user.username}")
        return current_user

    # No valid user, so let's provision a guest.
    logger.info("No valid session found. Provisioning new guest session.")
    guest_result = await get_or_create_guest_user(request.client.host, conn)
    if not guest_result:
        raise HTTPException(status_code=500, detail="Could not create guest identity.")

    guest_data, _ = guest_result
    full_guest_profile_data = await get_user_profile_with_details_by_id(conn, guest_data['id'])
    if not full_guest_profile_data:
        raise HTTPException(status_code=500, detail="Could not retrieve guest profile.")
    
    guest_profile = UserProfile(**full_guest_profile_data)

    # Create and set the cookie for the new guest session
    expires = timedelta(days=90)
    guest_token = create_access_token(
        data={"sub": str(guest_profile.id)},
        expires_delta=expires
    )
    response.set_cookie(
        key=AUTH_COOKIE_NAME, value=guest_token,
        httponly=True, samesite="lax", secure=IS_PRODUCTION,
        max_age=int(expires.total_seconds())
    )
    
    # Mark that we handled the cookie to prevent middleware from interfering
    request.state.auth_cookie_was_set = True

    return guest_profile

@auth_router.post("/token", response_model=Token, dependencies=[Depends(rate_limiter_dependency(times=5, seconds=60))])
async def login_for_access_token(  request: Request,
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
    conn: aiosqlite.Connection = Depends(get_db_connection),
):
    """
    Handles user login with username and password.

    On success, it performs three key actions:
    1. Creates a slim, short-lived JWT access token.
    2. Creates a secure, long-lived JWT refresh token and stores it in the DB.
    3. Sets both tokens in secure, httpOnly cookies on the response.
    """
    # 1. Verify user credentials against the database.
    user_id = await verify_user(
        username=form_data.username, password=form_data.password, conn=conn
    )
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 2. Create the slim JWT access token. Its only job is to identify the user.
    access_token_expires = timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    access_token_payload = {"sub": str(user_id)}  # 'sub' (subject) is the standard claim for user ID
    access_token = create_access_token(
        data=access_token_payload, expires_delta=access_token_expires
    )

    # 3. Create the long-lived refresh token.
    refresh_token_payload = {"sub": str(user_id)}
    refresh_token = create_refresh_token(data=refresh_token_payload)
    
    # 4. Store the refresh token in the database allowlist for security.
    refresh_expires_at = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    await store_refresh_token(user_id, refresh_token, refresh_expires_at, conn)

    # 5. Set the access token in a secure, httpOnly cookie.
    # This is the primary token your middleware will read.
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=access_token,
        httponly=True,  # Prevents client-side JavaScript from accessing the cookie.
        samesite="lax", # Provides CSRF protection. 'strict' is also an option.
        secure=IS_PRODUCTION,    # IMPORTANT: Set to True in production (requires HTTPS).
        max_age=int(access_token_expires.total_seconds()),
    )

    # 6. Set the refresh token in its own secure, httpOnly cookie.
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        samesite="lax",
        secure=IS_PRODUCTION,    # IMPORTANT: Set to True in production.
        max_age=int(timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS).total_seconds()),
    )
    request.state.auth_cookie_was_set = True
    # 7. Return the access token in the response body as well.
    # This is standard practice and useful for non-browser clients (e.g., mobile apps).
    return {"access_token": access_token, "token_type": "bearer"}


@auth_router.post("/refresh_token", response_model=Token)
async def refresh_access_token(
    request: Request,
    response: Response,
    conn: aiosqlite.Connection = Depends(get_db_connection),
):
    """
    Issues a new access token using a valid refresh token from cookies.
    """
    # 1. Attempt to get the refresh token from the request cookies.
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token not found in cookies",
        )

    # 2. Validate the refresh token. This helper function checks the DB allowlist,
    # signature, expiry, and user revocation status.
    user_data = await get_user_from_valid_refresh_token(refresh_token, conn)
    
    if not user_data:
        # SECURITY: If the refresh token is invalid, clear all auth cookies
        # to force a full re-authentication and prevent reuse of a bad token.
        
        content = {"detail": "Session is invalid or has been revoked. Please log in again."}
        
        # 2. Instantiate a JSONResponse with the desired status code and content
        error_response = JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content=content,
        )
        
        # 3. Modify *this* response object to delete the cookies
        error_response.delete_cookie("refresh_token")
        error_response.delete_cookie(AUTH_COOKIE_NAME)
        
        # 4. Return the fully constructed response. Do NOT raise an exception.
        return error_response

    # 3. If validation passed, create a new slim access token.
    user_id = user_data["id"]
    access_token_expires = timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    new_access_token_payload = {"sub": str(user_id)}
    new_access_token = create_access_token(
        data=new_access_token_payload, expires_delta=access_token_expires
    )

    # 4. Set the new access token in the cookie, overwriting the old one.
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=new_access_token,
        httponly=True,
        samesite="lax",
        secure=IS_PRODUCTION,  # IMPORTANT: Set to True in production.
        max_age=int(access_token_expires.total_seconds()),
    )
    request.state.auth_cookie_was_set = True
    # 5. Return the new access token in the response body.
    return {"access_token": new_access_token, "token_type": "bearer"}


@auth_router.post("/logout")
async def logout(
    request: Request, 
    response: Response, 
    conn: aiosqlite.Connection = Depends(get_db_connection) # <-- Use async connection
):
    """Logs out by invalidating the refresh token and clearing the cookie."""
    refresh_token = request.cookies.get("refresh_token")
    if refresh_token:
        # Use await to invalidate the token
        await invalidate_refresh_token(refresh_token, conn)
    response.delete_cookie(key="refresh_token")
    response.delete_cookie(AUTH_COOKIE_NAME)

    return {"message": "Successfully logged out"}


@auth_router.post("/register", status_code=status.HTTP_201_CREATED, response_model=UserProfile, dependencies=[Depends(rate_limiter_dependency(times=2, seconds=3600))])
async def register_user(  request: Request,
    user_in: UserCreate,
    response: Response,  # <-- 1. Add the Response object to the signature
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Public registration for a new user.
    Upon success, it automatically logs the user in by setting auth cookies.
    """
    try:
        new_user_id = await create_user(user_in, conn=conn)
        new_user_profile_data = await get_user_profile_with_details_by_id(conn=conn, user_id=new_user_id)
        if not new_user_profile_data:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Could not create user profile.")

        # --- START: NEW LOGIC TO LOG THE USER IN ---
        
        # 2. Generate tokens, just like in the login endpoint.
        access_token_expires = timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
        access_token_data = {"sub": str(new_user_id)}
        access_token = create_access_token(
            data=access_token_data,
            expires_delta=access_token_expires
        )
        refresh_token = create_refresh_token(data={"sub": str(new_user_id)})
    
        # 3. Store the refresh token in the database.
        logger.info(f"Storing refresh token for new user {new_user_id}")
        refresh_expires_at = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        await store_refresh_token(new_user_id, refresh_token, refresh_expires_at, conn)

        # 4. Set the cookies on the response.
        response.set_cookie(
            key=AUTH_COOKIE_NAME,
            value=access_token,
            httponly=True,
            samesite="lax",
            secure=IS_PRODUCTION,  # Set to True in production
            max_age=int(access_token_expires.total_seconds())
        )
        response.set_cookie(
            key="refresh_token", 
            value=refresh_token, 
            httponly=True, 
            samesite="lax",
            secure=IS_PRODUCTION, # Often more strict
            max_age=int(timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS).total_seconds()),
        )
        
        # --- END: NEW LOGIC ---
        request.state.auth_cookie_was_set = True
        return new_user_profile_data # Return the user profile as before
    
    except aiosqlite.IntegrityError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already registered")
    
@auth_router.get("/me", response_model=UserProfile, summary="Get current user's profile or guest identity")
async def read_users_me(
    current_user: UserProfile = Depends(get_current_user_or_guest)
):
    """
    Returns the profile data for the currently logged-in user.
    Note: The underlying dependencies like 'get_current_active_user_profile'
    must also be updated to use the async DB connection.
    """
    return current_user


# ==============================================================================
# --- 2. User Management Router (Admin Protected) ---
# ==============================================================================

admin_router = APIRouter(prefix="/api/admin", tags=["User Management (Admin)"])

@admin_router.get("/users", response_model=List[UserProfile], summary="Get all users (Admin)")
async def read_all_users(
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection)
):
    """
    Retrieves a list of all user profiles. Requires 'user:view_any' permission.
    """
    logger.info(current_user)
    users = await get_all_user_profiles_with_details_securely(caller_user_id=current_user.id, conn=conn)
    return users

@admin_router.get("/{user_id}", response_model=User, summary="Get user by ID (Admin)")
async def read_user_by_id(
    user_id: int = Path(..., gt=0),
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection) # <-- Use async connection
):
    """Retrieves a specific user's profile. Requires 'user:view' permission."""
    try:
        # Use await to get user data
        user = await get_user_data_by_id_securely(current_user.id, user_id, conn)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return user
    except PermissionDeniedError:
        raise HTTPException(status_code=403, detail="You do not have permission to view other users.")




@admin_router.patch("/{user_id}/status", response_model=User, summary="Update user status (Admin)")
async def update_user_status(
    update_data: UserStatusUpdateRequest,
    user_id: int = Path(..., gt=0),
    current_user: UserProfile = Depends(get_current_active_user),
    conn: aiosqlite.Connection = Depends(get_db_connection), # <-- Use async connection
):
    """Activates or deactivates a user. Requires 'user:manage' permission."""
    try:
        # Use await to update status
        success = await set_user_active_status_securely(
            current_user_id=current_user.id,
            target_user_id=user_id,
            is_active=update_data.is_active,
            conn=conn
        )
        if not success:
            raise HTTPException(status_code=404, detail="User not found or operation failed.")
        
        # Use await to invalidate sessions
        await invalidate_user_sessions(user_id, conn)
        
        # Use await to return the updated user data
        updated_user = await get_user_by_id(user_id, conn)
        return updated_user
    except PermissionDeniedError:
        raise HTTPException(status_code=403, detail="You do not have permission to manage users.")