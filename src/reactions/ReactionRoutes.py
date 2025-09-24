import aiosqlite
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from aiosqlite import Connection

from src.db_utils import get_db_connection
from src.models import UserProfile
from src.roles.errors import PermissionDeniedError
from src.roles.permissions import Permission
from src.roles.rbac_manager import require_permission_from_profile
from src.users.auth_dependency import get_current_active_user
from src.reactions.HelperReactions import (
    add_reaction_to_instance,
    get_available_reactions,
    get_instances_with_reaction_by_user,
    remove_reaction_from_instance,
    get_reactions_for_instance,
)
import logging

from pydantic import BaseModel, ConfigDict, Field

class ReactionSummaryResponse(BaseModel):
    id: int
    name: str
    label: str
    emoji: Optional[str] = None
    image_path: Optional[str] = None
    count: int
    reacted_by_user: bool


class ReactionInfo(BaseModel):
    """Defines the structure of an available reaction."""
    id: int
    name: str
    label: str
    emoji: Optional[str]
    image_path: Optional[str]
    description: Optional[str]
    
    model_config = ConfigDict(from_attributes=True)


# A generic instance model for responses
class InstanceInfo(BaseModel):
    id: int
    file_name: str
    file_type: str
    is_encrypted: bool
    file_hash: str

    model_config = ConfigDict(from_attributes=True)


class PaginatedInstanceResponse(BaseModel):
    """Standard paginated response structure for a list of file instances."""
    total: int
    page: int
    page_size: int
    items: List[InstanceInfo]

router = APIRouter(prefix='/api',tags=["Reactions"])
logger = logging.getLogger("ReactionRoutes")
logger.setLevel(logging.DEBUG)


@router.get(
    "/reactions/available",
    response_model=List[ReactionInfo],
    summary="Get Available Reactions",
    description="Fetches a list of all active reactions that users can apply to file instances (e.g., like, favorite)."
)
async def get_available_reactions_route(conn: Connection = Depends(get_db_connection)):
    """
    Provides a list of all globally available and active reactions.
    This endpoint is public and does not require special permissions.
    """
    # Call helper function directly
    return await get_available_reactions(conn=conn)


@router.post(
    "/instance/{instance_id}/reactions/{reaction_id}",
    response_model=List[ReactionSummaryResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Add a Reaction to a File Instance",
    description="Adds a reaction from the current user to a specified file instance. Requires 'reaction:add' permission."
)
@require_permission_from_profile(Permission.REACTION_ADD)
async def add_reaction_route(
    instance_id: int,
    reaction_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: Connection = Depends(get_db_connection)
) -> List[Dict[str, Any]]:
    """
    Applies a reaction from the authenticated user to a given file instance.
    """
    try:
        success = await add_reaction_to_instance(
            user_id=current_user.id,
            instance_id=instance_id,
            reaction_id=reaction_id,
            conn=conn
        )
        if not success:
            # This can happen if the reaction already exists (INSERT OR IGNORE), which is not an error.
            # We proceed to return the current state.
            pass
        
        # On success or if it already exists, return the updated list of reactions for the instance.
        return await get_reactions_for_instance(instance_id, current_user.id, conn)

    except PermissionDeniedError as e:
        logger.warning(f"Permission denied for user {current_user.id}: {e}")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except aiosqlite.IntegrityError:
        # This can happen if instance_id or reaction_id is invalid and violates a FK constraint.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instance or reaction not found.")
    except Exception as e:
        logger.error(f"Error adding reaction for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.delete(
    "/instance/{instance_id}/reactions/{reaction_id}",
    response_model=List[ReactionSummaryResponse],
    status_code=status.HTTP_200_OK,
    summary="Remove a Reaction from a File Instance",
    description="Removes a reaction from the current user on a specified file instance. Requires 'reaction:add' permission."
)
@require_permission_from_profile(Permission.REACTION_ADD)
async def remove_reaction_route(
    instance_id: int,
    reaction_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: Connection = Depends(get_db_connection)
):
    """
    Removes a specific reaction applied by the authenticated user from a file instance.
    """
    try:
        await remove_reaction_from_instance(
            user_id=current_user.id,
            instance_id=instance_id,
            reaction_id=reaction_id,
            conn=conn
        )
        # Always return the current state of reactions for the instance.
        return await get_reactions_for_instance(instance_id, current_user.id, conn)
    except PermissionDeniedError as e:
        logger.warning(f"Permission denied for user {current_user.id}: {e}")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        logger.error(f"Error removing reaction for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.get(
    "/reactions/my-instances/{reaction_id}",
    response_model=PaginatedInstanceResponse,
    summary="Get My Instances with a Specific Reaction",
    description="Retrieves a paginated list of file instances that the current user has marked with a specific reaction. Requires 'file:view' permission."
)
@require_permission_from_profile(Permission.FILE_VIEW)
async def get_my_instances_with_reaction_route(
    reaction_id: int,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: Connection = Depends(get_db_connection),
    page: int = Query(1, ge=1, description="Page number to retrieve."),
    page_size: int = Query(24, ge=1, le=100, description="Number of items per page.")
):
    """
    Fetches file instances the authenticated user has reacted to.
    """
    try:
        return await get_instances_with_reaction_by_user(
            user_id=current_user.id,
            reaction_id=reaction_id,
            page=page,
            page_size=page_size,
            conn=conn
        )
    except PermissionDeniedError as e:
        logger.warning(f"Permission denied for user {current_user.id}: {e}")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        logger.error(f"Error retrieving user reactions for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")