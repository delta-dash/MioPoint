class RBACError(Exception):
    """Base exception for RBAC operations."""
    pass

class RoleNotFoundError(RBACError):
    """Raised when a role is not found."""
    pass

class PermissionNotFoundError(RBACError):
    """Raised when a permission is not found."""
    pass

class RoleAlreadyExistsError(RBACError):
    """Raised when trying to create a role that already exists."""
    pass

class ProtectedRoleError(RBACError):
    """Raised when trying to modify or delete a protected role."""
    pass

class RoleCreationError(RBACError):
    """Generic error during role creation."""
    pass

class RoleEditError(RBACError):
    """Generic error during role editing."""
    pass

class RoleDeletionError(RBACError):
    """Generic error during role deletion."""
    pass

class PermissionAssignmentError(RBACError):
    """Raised when assigning or revoking permissions fails."""
    pass

class UserRoleAssignmentError(RBACError):
    """Raised when assigning or removing roles to users fails."""
    pass

class PermissionDeniedError(RBACError):
    """Raised when a user lacks the required permission or rank."""
    pass