import asyncio
from typing import Any, Dict, List, Optional

import aiosqlite

# Local application imports
from ConfigMedia import get_config
import logging
from src.db_utils import AsyncDBContext, execute_db_query


# ==============================================================================
# --- INITIAL SETUP ---
# ==============================================================================

logger = logging.getLogger("HelperReactions")
logger.setLevel(logging.DEBUG)
config = get_config()

# ==============================================================================
# --- DATABASE INTEGRITY AND SETUP ---
# ==============================================================================

async def setup_reactions_database_async():
    """
    Creates or updates the database schema for reactions using the AsyncDBContext manager.
    This function is idempotent and should be run once at application startup.
    """
    try:
        # Use the consistent AsyncDBContext manager for this setup task.
        async with AsyncDBContext() as conn:
            # First, ensure dependencies are met (assuming an async check function)
            # If `check_database_requirements_async` doesn't exist, this check must be adapted.
            
            # Foreign keys are enabled by the context manager, but being explicit is safe.
            await conn.execute("PRAGMA foreign_keys = ON;")

            # Table for available reaction types (e.g., like, favorite)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS reactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    label TEXT NOT NULL,
                    emoji TEXT,
                    image_path TEXT,
                    description TEXT,
                    sort_order INTEGER DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Junction table to link users, files, and reactions
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_file_reactions (
                    user_id INTEGER NOT NULL,
                    file_id INTEGER NOT NULL,
                    reaction_id INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, file_id, reaction_id),
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE,
                    FOREIGN KEY (reaction_id) REFERENCES reactions (id) ON DELETE CASCADE
                );
            """)
            
            # Populate default reactions within the same transaction
            await populate_default_reactions_async(conn)
            
            # The AsyncDBContext manager will commit everything on successful exit.
            
        logger.debug(f"Database '{config.data['DB_FILE']}' schema for reactions is verified.")
    except Exception as e:
        logger.critical(f"Failed to set up reactions database schema: {e}", exc_info=True)
        # In a real app, you might want to sys.exit(1) here if the DB is critical
        raise

async def populate_default_reactions_async(conn: aiosqlite.Connection):
    """
    Populates the reactions table with a default set if it's empty.
    This function is idempotent and uses a provided async connection.
    It does not commit; it relies on the caller to manage the transaction.
    """
    default_reactions = [
        ('like', 'Like', '👍', None, 'Like this file', 0),
        ('favorite', 'Favorite', '⭐', None, 'Mark as a favorite', 1),
        ('laugh', 'Laugh', '😂', None, 'Find this file funny', 2),
        ('love', 'Love', '❤️', None, 'Love this file', 3),
    ]
    try:
        # Using INSERT OR IGNORE to prevent errors on subsequent runs
        await conn.executemany(
            "INSERT OR IGNORE INTO reactions (name, label, emoji, image_path, description, sort_order) VALUES (?, ?, ?, ?, ?, ?)",
            default_reactions
        )
        logger.info("Default reactions populated if necessary.")
        
    except aiosqlite.Error as e:
        logger.error(f"Failed to populate default reactions: {e}", exc_info=True)
        raise # Propagate the exception to cause a transaction rollback



# ==============================================================================
# --- INTERNAL HELPERS ---
# ==============================================================================

async def _get_reaction_id_by_name_async(conn: aiosqlite.Connection, reaction_name: str) -> Optional[int]:
    """Internal async helper to get a reaction's ID from its programmatic name."""
    query = "SELECT id FROM reactions WHERE name = ? AND is_active = 1"
    result = await execute_db_query(conn, query, (reaction_name,), fetch_one=True)
    return result['id'] if result else None


# ==============================================================================
# --- REACTION MANAGEMENT ---
# ==============================================================================

async def add_reaction_to_file(
    user_id: int, 
    file_id: int, 
    reaction_name: str, 
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously adds a reaction from a user to a file.

    Args:
        user_id: ID of the user adding the reaction.
        file_id: ID of the file being reacted to.
        reaction_name: The programmatic name of the reaction (e.g., 'like').
        conn: An optional aiosqlite connection for transactional operations.

    Returns:
        True if the reaction was added successfully, False otherwise.
    """
    async with AsyncDBContext(conn) as connection:
        normalized_name = reaction_name.strip().lower()
        if not normalized_name:
            return False
            
        reaction_id = await _get_reaction_id_by_name_async(connection, normalized_name)
        if reaction_id is None:
            logger.warning(f"Reaction '{normalized_name}' is not a valid or active reaction type.")
            return False
        
        try:
            query = "INSERT OR IGNORE INTO user_file_reactions (user_id, file_id, reaction_id) VALUES (?, ?, ?)"
            rowcount = await execute_db_query(connection, query, (user_id, file_id, reaction_id))
            return rowcount > 0
        except aiosqlite.IntegrityError:
            logger.error(f"Could not add reaction due to integrity constraint. User {user_id} or File {file_id} may not exist.")
            return False


async def remove_reaction_from_file(
    user_id: int, 
    file_id: int, 
    reaction_name: str, 
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously removes a specific reaction from a user on a file.

    Args:
        user_id: ID of the user whose reaction is being removed.
        file_id: ID of the file.
        reaction_name: The programmatic name of the reaction to remove.
        conn: An optional aiosqlite connection for transactional operations.

    Returns:
        True if a reaction was removed, False otherwise.
    """
    async with AsyncDBContext(conn) as connection:
        normalized_name = reaction_name.strip().lower()
        reaction_id = await _get_reaction_id_by_name_async(connection, normalized_name)
        if reaction_id is None:
            return False
            
        query = "DELETE FROM user_file_reactions WHERE user_id = ? AND file_id = ? AND reaction_id = ?"
        rowcount = await execute_db_query(connection, query, (user_id, file_id, reaction_id))
        return rowcount > 0


# ==============================================================================
# --- QUERY FUNCTIONS ---
# ==============================================================================

async def get_files_with_reaction_by_user(
    user_id: int, 
    reaction_name: str, 
    page: int = 1, 
    page_size: int = 24, 
    conn: Optional[aiosqlite.Connection] = None
) -> Dict[str, Any]:
    """
    Asynchronously finds all files that a specific user has reacted to in a certain way.
    
    Args:
        user_id: ID of the user.
        reaction_name: The programmatic name of the reaction (e.g., 'favorite').
        page: The page number for pagination (1-indexed).
        page_size: The number of items per page.
        conn: An optional aiosqlite connection for transactional operations.

    Returns:
        A dictionary with pagination info and a list of file data.
    """
    async with AsyncDBContext(conn) as connection:
        offset = (page - 1) * page_size
        normalized_name = reaction_name.strip().lower()
        
        reaction_id = await _get_reaction_id_by_name_async(connection, normalized_name)
        if reaction_id is None:
            return {"total": 0, "page": page, "page_size": page_size, "items": []}
        
        count_query = "SELECT COUNT(file_id) FROM user_file_reactions WHERE user_id = ? AND reaction_id = ?"
        total_row = await execute_db_query(connection, count_query, (user_id, reaction_id), fetch_one=True)
        total_count = total_row[0] if total_row else 0
        
        items_query = """
            SELECT f.id, f.file_name, f.file_type, f.is_encrypted, f.file_hash
            FROM files f JOIN user_file_reactions ufr ON f.id = ufr.file_id
            WHERE ufr.user_id = ? AND ufr.reaction_id = ?
            ORDER BY ufr.created_at DESC LIMIT ? OFFSET ?
        """
        items_rows = await execute_db_query(connection, items_query, (user_id, reaction_id, page_size, offset), fetch_all=True)
        items = [dict(row) for row in items_rows]
        
        return {"total": total_count, "page": page, "page_size": page_size, "items": items}

async def get_available_reactions(conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """
    Asynchronously retrieves all active, available reactions from the database.

    Args:
        conn: An optional aiosqlite connection for transactional operations.
        
    Returns:
        A list of dictionaries, where each dictionary represents a reaction.
    """

    async with AsyncDBContext(conn) as connection:
        query = (
            "SELECT name, label, emoji, image_path, description FROM reactions "
            "WHERE is_active = 1 ORDER BY sort_order ASC, name ASC"
        )
        reactions_rows = await execute_db_query(connection, query, fetch_all=True)
        return [dict(row) for row in reactions_rows]
        
