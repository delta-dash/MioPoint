"""
Service layer for handling user reactions.

This module provides a secure interface for reaction-related operations.
It enforces permissions using decorators and then delegates the core
database logic to the `helpers.HelperReactions` module.

This separation ensures that security and business rules are handled
consistently before any data access occurs.
"""

from typing import Any, Dict, List, Optional
import aiosqlite

# Local application imports

from src.reactions.HelperReactions import add_reaction_to_file, get_available_reactions, get_files_with_reaction_by_user, remove_reaction_from_file
import logging
from src.roles.rbac_manager import require_permission

# Get the logger for this service
logger = logging.getLogger("ReactionService")
logger.setLevel(logging.DEBUG)

# ==============================================================================
# --- SECURE SERVICE FUNCTIONS ---
# ==============================================================================

# NOTE: The @require_permission decorator must be updated to support async functions.
# If it is a synchronous decorator, it will not work correctly with the async
# functions below and will need to be refactored into an async-aware decorator.

@require_permission(permission='user:react', caller_id_arg='user_id')
async def add_reaction_to_file_securely(
    user_id: int,
    file_id: int,
    reaction_name: str,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Securely adds a user's reaction to a file, requiring 'user:react' permission.

    Args:
        user_id (int): The ID of the user performing the action.
        file_id (int): The ID of the file to react to.
        reaction_name (str): The programmatic name of the reaction (e.g., 'like').
        conn (aiosqlite.Connection, optional): An existing database connection for a transaction.

    Returns:
        bool: True if the reaction was added successfully, False otherwise.
        
    Raises:
        PermissionDeniedError: If the user does not have 'user:react' permission.
    """
    logger.debug(f"User {user_id} is attempting to add reaction '{reaction_name}' to file {file_id}.")
    # Delegate to the data helper after the permission check passes
    return await add_reaction_to_file(
        user_id=user_id,
        file_id=file_id,
        reaction_name=reaction_name,
        conn=conn
    )


@require_permission(permission='user:react', caller_id_arg='user_id')
async def remove_reaction_from_file_securely(
    user_id: int,
    file_id: int,
    reaction_name: str,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Securely removes a user's reaction from a file, requiring 'user:react' permission.

    Args:
        user_id (int): The ID of the user performing the action.
        file_id (int): The ID of the file to remove the reaction from.
        reaction_name (str): The programmatic name of the reaction (e.g., 'like').
        conn (aiosqlite.Connection, optional): An existing database connection for a transaction.

    Returns:
        bool: True if the reaction was removed successfully, False otherwise.
        
    Raises:
        PermissionDeniedError: If the user does not have 'user:react' permission.
    """
    logger.debug(f"User {user_id} is attempting to remove reaction '{reaction_name}' from file {file_id}.")
    return await remove_reaction_from_file(
        user_id=user_id,
        file_id=file_id,
        reaction_name=reaction_name,
        conn=conn
    )


@require_permission(permission='user:view_reactions', caller_id_arg='user_id')
async def get_files_with_reaction_by_user_securely(
    user_id: int,
    reaction_name: str,
    page: int = 1,
    page_size: int = 24,
    conn: Optional[aiosqlite.Connection] = None
) -> Dict[str, Any]:
    """
    Securely retrieves files that a specific user has reacted to.
    Requires 'user:view_reactions' permission.

    Args:
        user_id (int): The ID of the user whose reactions are being queried.
        reaction_name (str): The reaction to filter by.
        page (int): The page number for pagination.
        page_size (int): The number of items per page.
        conn (aiosqlite.Connection, optional): An existing database connection. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary with paginated results.
        
    Raises:
        PermissionDeniedError: If the user does not have 'user:view_reactions' permission.
    """
    logger.debug(f"User {user_id} is attempting to view files with reaction '{reaction_name}'.")
    return await get_files_with_reaction_by_user(
        user_id=user_id,
        reaction_name=reaction_name,
        page=page,
        page_size=page_size,
        conn=conn
    )


async def get_all_available_reactions(conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """
    Gets the list of all active, available reactions.
    This is a public-facing action and does not require special permissions.

    Args:
        conn (aiosqlite.Connection, optional): An existing database connection. Defaults to None.
    
    Returns:
        A list of dictionaries representing available reactions.
    """
    logger.debug("Fetching the list of all available reactions.")
    return await get_available_reactions(conn=conn)