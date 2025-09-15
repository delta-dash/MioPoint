from typing import List
from fastapi import APIRouter, Depends, HTTPException, Request,status
from src.models import User
from src.users.user_service import search_users_by_username
from src.users.auth_dependency import get_current_active_user
from src.users.user_manager import is_ip_banned

router = APIRouter(
    prefix="/api/users",
    tags=["Users"],
)

async def check_ip_ban(request: Request):
    """
    FastAPI dependency to check if the client's IP address is banned.
    Blocks the request with 403 Forbidden if banned.
    """
    client_host = request.client.host

    # Defer import to avoid circular dependency: auth -> user_manager -> ... -> auth

    if client_host and await is_ip_banned(client_host):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your IP address is banned from accessing this resource."
        )
    # If not banned, continue processing the request
    return True

@router.get("/search", response_model=List[User])
async def search_users_route(username: str, current_user: User = Depends(get_current_active_user)):
    """
    Searches for users by username.
    """
    if not username.strip():
        raise HTTPException(status_code=400, detail="Username query cannot be empty.")
    
    users = await search_users_by_username(username)
    return users
