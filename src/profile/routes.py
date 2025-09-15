from fastapi import APIRouter, Depends

from src.models import UserProfile
from src.profile.profile_info import get_current_active_user_profile
from src.users.auth_dependency import get_current_active_user


auth_router = APIRouter(prefix="/api", tags=["Profile"])

@auth_router.get(
    "/profile", 
    response_model=UserProfile, 
    summary="Get current user's profile with roles and permissions"
)
async def read_users_me(
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Returns the complete profile data for the currently logged-in user.

    The profile includes:
    - Basic user information (id, username, etc.).
    - A list of assigned roles, with each role's specific permissions nested inside.
    - A consolidated, flat list of all effective permissions for easy client-side checks.
    """
    return current_user