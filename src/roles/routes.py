# roles_router.py
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status, Path

# --- Assuming these are correctly imported from your project ---
# The service functions are now async, so we will await them.
from src.models import PermissionResponse, RoleBasicResponse, RoleCreateRequest, RoleDetailResponse, RoleEditRequest, UserProfile
from src.profile.profile_info import get_current_active_user_profile
from src.roles.rbac_manager import get_all_permissions
from src.users.auth_dependency import get_current_active_user
import logging
from src.roles.errors import *
from src.roles.service import assign_role_to_user_securely, create_role_securely, delete_role_securely, edit_role_securely, get_all_roles_securely, get_role_details_securely, remove_role_from_user_securely

# --- API Router Setup ---
router = APIRouter(prefix='/api',tags=["Roles"])

logger = logging.getLogger("RoleRoutes")
logger.setLevel(logging.DEBUG)

# --- API Routes (Now Async) ---



@router.get("/permissions", response_model=List[PermissionResponse], summary="List all available permissions")
async def get_all_permissions_route(current_user: UserProfile = Depends(get_current_active_user)):
    """
    Retrieves a list of all possible permissions that can be assigned to roles.
    This is a public endpoint and does not require specific permissions.
    """
    try:
        # This new service function would query your database for all defined permissions.
        # Example: SELECT id, name, description FROM permissions ORDER BY name;
        return await get_all_permissions()
    except Exception as e:
        logger.error(f"Unexpected error in get_all_permissions_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while fetching permissions.")
    
@router.post(
    "/role/add/{user_id}/{role_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Assign a role to a user",
    tags=["User Roles"] # Override the default 'Roles' tag for better organization
)
async def assign_role_to_user_route(
    user_id: int = Path(..., ge=1, description="The ID of the user to assign the role to."),
    role_id: int = Path(..., ge=1, description="The ID of the role to assign the user to."),
    current_user: UserProfile = Depends(get_current_active_user),
):
    """
    Assigns a role to a specific user.
    - Requires the **'user:manage_roles'** permission.
    """
    try:
        await assign_role_to_user_securely(
            admin_user_id=current_user.id,
            target_user_id=user_id,
            role_to_assign_id=role_id
        )
        # On success, return with 204 No Content
        return
    except ( RoleNotFoundError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    except PermissionDeniedError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except RBACError as e: # Catch-all for other specific RBAC logic errors
        logger.warning(f"RBACError assigning role {role_id} to user {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in assign_role_to_user_route: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while assigning the role.")


@router.delete(
    "/role/rmv/{user_id}/{role_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a role from a user",
    tags=["User Roles"] # Override the default 'Roles' tag
)
async def remove_role_from_user_route(
    user_id: int = Path(..., ge=1, description="The ID of the user to remove the role from."),
    role_id: int = Path(..., ge=1, description="The ID of the role to remove."),
    current_user: UserProfile = Depends(get_current_active_user),
):
    """
    Removes a role from a specific user.
    - Requires the **'user:manage_roles'** permission.
    """
    try:
        await remove_role_from_user_securely(
            admin_user_id=current_user.id,
            target_user_id=user_id,
            role_to_remove_id=role_id
        )
        # On success, return with 204 No Content
        return
    except ( RoleNotFoundError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except PermissionDeniedError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except RBACError as e: # Catch-all for other specific RBAC logic errors
        logger.warning(f"RBACError removing role {role_id} from user {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in remove_role_from_user_route: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while removing the role.")

@router.post(
    "/roles",
    response_model=RoleDetailResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new role"
)
async def create_new_role_route(
    request: RoleCreateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Creates a new role.
    - Requires **'role:manage_hierarchy'** permission.
    - If `permissions` are provided, also requires **'permission:grant'** permission.
    """
    try:
        # Note: Your create_role_securely expects permission NAMES, but your
        # RoleCreateRequest model takes permission IDs. For now, we assume
        # no permissions are set on creation, as per your frontend form.
        # If you wanted to add them, you'd need to convert IDs to names first.
        permission_ids = set(request.permissions) if request.permissions is not None else None

        new_role_id = await create_role_securely(
            user_id=current_user.id,
            name=request.name,
            rank=request.rank,
            permissions=permission_ids  # Pass the IDs directly if service layer is updated
        )
        
        # --- THIS IS THE FIX ---
        # You must fetch and return the details for the NEWLY created role.
        # Ensure you pass both the new role's ID and the current user's ID.
        data = await get_role_details_securely(role_id=new_role_id, user_id=current_user.id)
        return data
    except RoleAlreadyExistsError:
         raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"A role with the name '{request.name}' already exists.")
    except PermissionDeniedError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except PermissionNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in create_new_role_route: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while creating the role.")


@router.get("/roles", response_model=List[RoleBasicResponse], summary="List all roles")
async def get_all_roles_route(current_user: UserProfile = Depends(get_current_active_user)):
    """
    Retrieves a list of all defined roles, ordered by rank.
    This is a public endpoint and does not require specific permissions.
    """
    try:
        # Await the async service function
        return await get_all_roles_securely(current_user.id)
    except Exception as e:
        # Service layer should raise HTTPException on known errors.
        # This catches unexpected ones.
        logger.error(f"Unexpected error in get_all_roles_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while fetching roles.")


@router.get("/roles/{role_id}", response_model=RoleDetailResponse, summary="Get role details")
async def get_single_role_route(
    role_id: int = Path(..., ge=1, description="The ID of the role to retrieve."),
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Retrieves the full details for a specific role, including its permissions.
    This is a public endpoint and does not require specific permissions.
    """
    # The service function is async and handles the 404 case
    return await get_role_details_securely(role_id, current_user.id)


@router.put("/roles/{role_id}", response_model=RoleDetailResponse, summary="Edit a role")
async def edit_role_route(
    request: RoleEditRequest,
    role_id: int = Path(..., ge=1, description="The ID of the role to edit."),
    current_user: UserProfile = Depends(get_current_active_user),
):
    """
    Updates a role's name, rank, and/or permissions.
    - Requires **'role:manage_hierarchy'** permission for name/rank changes.
    - Requires **'permission:grant'** permission for permission changes.
    """
    try:
        permission_set = set(request.permissions_to_set) if request.permissions_to_set is not None else None
        
        # Await the async service function
        success = await edit_role_securely(
            user_id=current_user.id,
            role_id=role_id,
            new_name=request.new_name,
            new_rank=request.new_rank,
            permissions_to_set=permission_set
        )
        
        if not success:
            # The secure function raises specific errors for known failures like 404, 403.
            # Returning False would imply a logic error or an unhandled case.
            raise HTTPException(status_code=400, detail="Failed to edit role due to an unspecified reason.")

        # If successful, fetch and return the updated details
        return await get_role_details_securely(role_id=role_id, user_id=current_user.id)
        
    except RoleNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Role with ID {role_id} not found.")
    except RoleAlreadyExistsError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"A role with the name '{request.new_name}' already exists.")
    except PermissionDeniedError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except PermissionNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in edit_role_route: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while editing the role.")


@router.delete("/roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a role")
async def delete_role_route(
    role_id: int = Path(..., ge=1, description="The ID of the role to delete."),
    current_user: UserProfile = Depends(get_current_active_user),
):
    """
    Deletes a role. Protected system roles like 'Admin', 'User', 'Everyone' cannot be deleted.
    - Requires **'role:manage_hierarchy'** permission.
    """
    try:
        # Await the async service function
        success = await delete_role_securely(current_user.id, role_id)
        if not success:
            # Secure function raises specific errors, so this path indicates an unexpected issue.
            raise HTTPException(status_code=400, detail="Failed to delete role. It may be protected or not exist.")
        
        # Return with 204 No Content
        return

    except ProtectedRoleError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except RoleNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Role with ID {role_id} not found.")
    except PermissionDeniedError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in delete_role_route: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while deleting the role.")