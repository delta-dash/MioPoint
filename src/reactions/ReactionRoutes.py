import aiosqlite
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from aiosqlite import Connection

from src.db_utils import get_db_connection
from src.models import UserProfile
from src.roles.errors import PermissionDeniedError
from src.users.auth_dependency import get_current_active_user
from src.reactions.ReactionService import (
    add_reaction_to_file_securely, 
    get_all_available_reactions, 
    get_files_with_reaction_by_user_securely, 
    remove_reaction_from_file_securely
)
import logging

from pydantic import BaseModel, ConfigDict, Field

class ReactionRequest(BaseModel):
    """Request body for adding or removing a reaction."""
    file_id: int = Field(..., gt=0, description="The unique ID of the file.")
    reaction_name: str = Field(..., min_length=1, description="The programmatic name of the reaction (e.g., 'like').")

class ReactionInfo(BaseModel):
    """Defines the structure of an available reaction."""
    name: str
    label: str
    emoji: Optional[str]
    image_path: Optional[str]
    description: Optional[str]
    
    model_config = ConfigDict(from_attributes=True)


# A generic file model for responses
class FileInfo(BaseModel):
    id: int
    file_name: str
    file_type: str
    is_encrypted: bool
    file_hash: str

    model_config = ConfigDict(from_attributes=True)


class PaginatedFileResponse(BaseModel):
    """Standard paginated response structure for a list of files."""
    total: int
    page: int
    page_size: int
    items: List[FileInfo]

router = APIRouter(prefix='/api',tags=["Reactions"])
logger = logging.getLogger("ReactionRoutes")
logger.setLevel(logging.DEBUG)


@router.get(
    "/reactions/available",
    response_model=List[ReactionInfo],
    summary="Get All Available Reactions",
    description="Fetches a list of all active reactions that users can apply to files (e.g., like, favorite)."
)
async def get_available_reactions_route(conn: Connection = Depends(get_db_connection)):
    """
    Provides a list of all globally available and active reactions.
    This endpoint is public and does not require special permissions.
    """
    # Service function is now async
    return await get_all_available_reactions(conn=conn)


@router.post(
    "/reactions",
    status_code=status.HTTP_201_CREATED,
    summary="Add a Reaction to a File",
    description="Adds a reaction from the current user to a specified file. Requires 'user:react' permission."
)
async def add_reaction_route(
    payload: ReactionRequest,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: Connection = Depends(get_db_connection)
) -> Dict[str, Any]:
    """
    Applies a reaction from the authenticated user to a given file.
    """
    try:
        # Service function is now async and uses conn
        success = await add_reaction_to_file_securely(
            user_id=current_user.id,
            file_id=payload.file_id,
            reaction_name=payload.reaction_name,
            conn=conn
        )
        if success:
            return {"status": "success", "detail": "Reaction added."}
        else:
            # The reaction might already exist, which isn't an error.
            raise HTTPException(
                status_code=status.HTTP_200_OK,
                detail="Reaction already exists or file/reaction not found."
            )
    except PermissionDeniedError as e:
        logger.warning(f"Permission denied for user {current_user.id}: {e}")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding reaction for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.delete(
    "/reactions",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a Reaction from a File",
    description="Removes a reaction from the current user on a specified file. Requires 'user:react' permission."
)
async def remove_reaction_route(
    payload: ReactionRequest,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: Connection = Depends(get_db_connection)
):
    """
    Removes a specific reaction applied by the authenticated user from a file.
    """
    try:
        # Service function is now async and uses conn
        success = await remove_reaction_from_file_securely(
            user_id=current_user.id,
            file_id=payload.file_id,
            reaction_name=payload.reaction_name,
            conn=conn
        )
        if not success:
            # If the service returns false, the reaction didn't exist to be removed.
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Reaction not found for this user and file."
            )
        # On success, a 204 No Content response is sent automatically.
        return
    except PermissionDeniedError as e:
        logger.warning(f"Permission denied for user {current_user.id}: {e}")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        logger.error(f"Error removing reaction for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.get(
    "/reactions/my-files/{reaction_name}",
    response_model=PaginatedFileResponse,
    summary="Get My Files with a Specific Reaction",
    description="Retrieves a paginated list of files that the current user has marked with a specific reaction. Requires 'user:view_reactions' permission."
)
async def get_my_files_with_reaction_route(
    reaction_name: str,
    current_user: UserProfile = Depends(get_current_active_user),
    conn: Connection = Depends(get_db_connection),
    page: int = Query(1, ge=1, description="Page number to retrieve."),
    page_size: int = Query(24, ge=1, le=100, description="Number of items per page.")
):
    """
    Fetches files the authenticated user has reacted to.
    """
    try:
        # Service function is now async and uses conn
        return await get_files_with_reaction_by_user_securely(
            user_id=current_user.id,
            reaction_name=reaction_name,
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