# fastapi_models.py
from typing import Annotated, List, Optional, Union
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator


# ==============================================================================
# 1. Data Transfer Object (DTO)
# ==============================================================================



class PermissionResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]

class UserRoleAssignRequest(BaseModel):
    role_id: int

class RoleBasicResponse(BaseModel):
    """
    Pydantic model for representing a user's role.
    """
    id: int
    name: str
    rank: int

    model_config = ConfigDict(from_attributes=True)

        
class RoleDetailResponse(RoleBasicResponse):
    permissions: List[PermissionResponse]

class RoleCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, description="The unique name for the new role.")
    rank: int = Field(..., ge=0, description="The rank of the role (lower is higher priority).")
    permissions: Optional[List[int]] = Field(None, description="A list of permission IDs to assign to the new role.")

class RoleEditRequest(BaseModel):
    new_name: Optional[str] = Field(None, min_length=1, description="A new unique name for the role.")
    new_rank: Optional[int] = Field(None, ge=0, description="A new rank for the role.")
    permissions_to_set: Optional[List[int]] = Field(None, description="A complete list of permission IDs. This will replace all existing permissions for the role.")


class Token(BaseModel):
    access_token: str
    token_type: str


class UserBase(BaseModel):
    username: Annotated[str, StringConstraints(min_length=2)] = Field(...)
    @field_validator('username')
    def name_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('Name must not be empty')
        return value.strip()


class UserCreate(UserBase):
    password: Annotated[str, StringConstraints(min_length=8)] = Field(...)
    

# 3. The main model for data returned from the API (what we created above)
class User(UserBase):
    id: int
    is_guest: bool
    is_active: bool
    
    model_config = ConfigDict(from_attributes=True)

class UserProfile(User):
    """
    The comprehensive user profile model for API responses.
    Includes basic user info, a detailed list of their assigned roles,
    and a consolidated flat list of all effective permissions for easy checking.
    """
    
    roles: List[RoleDetailResponse]
    permissions: List[PermissionResponse] 

    model_config = ConfigDict(from_attributes=True)

        
# 4. A model representing the full user record in the database, including the password
class UserInDB(User):
    hashed_password: str

# --- ADDED: Models for managing file visibility ---

class FileVisibilityUpdateRequest(BaseModel):
    """Request model for setting the visibility of a file."""
    role_ids: List[int] = Field(
        ...,
        description="A list of role IDs that should be able to see this file. "
                    "Passing an empty list will make the file visible to the 'Everyone' role."
    )

class TagVisibilityUpdateRequest(BaseModel):
    """Request model for setting visibility based on tags."""
    tags: List[Union[str, int]] = Field(..., description="A list of tag names or IDs to identify the target files.")
    # Changed from role_ids: List[int] to role_id: int to match the feature's purpose
    # of adding/removing a single role at a time.
    role_id: int = Field(..., description="The ID of the role to add or remove.")
    tag_type: str = Field(..., description="The type of tag, either 'tags' or 'meta_tags'.")

    @field_validator('tag_type')
    def tag_type_must_be_valid(cls, v):
        if v not in ['tags', 'meta_tags']:
            raise ValueError("tag_type must be either 'tags' or 'meta_tags'")
        return v