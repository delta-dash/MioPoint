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
            # NOTE: This schema has been updated to reference file_instances instead of a legacy 'files' table.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_instance_reactions (
                    user_id INTEGER NOT NULL,
                    instance_id INTEGER NOT NULL,
                    reaction_id INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, instance_id, reaction_id),
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    FOREIGN KEY (instance_id) REFERENCES file_instances (id) ON DELETE CASCADE,
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
# ==============================================================================
# --- REACTION MANAGEMENT ---
# ==============================================================================

async def add_reaction_to_instance(
    user_id: int, instance_id: int, reaction_id: int, conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously adds a reaction from a user to a file instance.

    Args:
        user_id: ID of the user adding the reaction.
        instance_id: ID of the file instance being reacted to.
        reaction_id: The ID of the reaction to add.
        conn: An optional aiosqlite connection for transactional operations.

    Returns:
        True if the reaction was added successfully, False otherwise.
    """
    async with AsyncDBContext(conn) as connection:
        try:
            query = "INSERT OR IGNORE INTO user_instance_reactions (user_id, instance_id, reaction_id) VALUES (?, ?, ?)"
            rowcount = await execute_db_query(connection, query, (user_id, instance_id, reaction_id))
            return rowcount > 0
        except aiosqlite.IntegrityError:
            logger.error(
                f"Could not add reaction due to integrity constraint. User {user_id}, Instance {instance_id}, or Reaction {reaction_id} may not exist."
            )
            return False


async def remove_reaction_from_instance(
    user_id: int, instance_id: int, reaction_id: int, conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously removes a specific reaction from a user on a file instance.

    Args:
        user_id: ID of the user whose reaction is being removed.
        instance_id: ID of the file instance.
        reaction_id: The ID of the reaction to remove.
        conn: An optional aiosqlite connection for transactional operations.

    Returns:
        True if a reaction was removed, False otherwise.
    """
    async with AsyncDBContext(conn) as connection:
        query = "DELETE FROM user_instance_reactions WHERE user_id = ? AND instance_id = ? AND reaction_id = ?"
        rowcount = await execute_db_query(connection, query, (user_id, instance_id, reaction_id))
        return rowcount > 0


# ==============================================================================
# --- QUERY FUNCTIONS ---
# ==============================================================================

async def get_reactions_for_instance(
    instance_id: int,
    user_id: int,
    conn: Optional[aiosqlite.Connection] = None
) -> List[Dict[str, Any]]:
    """
    Asynchronously retrieves a summary of reactions for a specific file instance.

    For each available reaction, it provides the count and whether the current user has applied it.
    """
    async with AsyncDBContext(conn) as connection:
        # This query gets all active reactions and LEFT JOINs the reactions for the specific instance.
        # This ensures all reaction types are returned, even with a count of 0.
        query = """
            SELECT
                r.id,
                r.name,
                r.label,
                r.emoji,
                r.image_path,
                COUNT(uir.user_id) as count,
                CAST(SUM(CASE WHEN uir.user_id = ? THEN 1 ELSE 0 END) > 0 AS INTEGER) as reacted_by_user
            FROM reactions r
            LEFT JOIN user_instance_reactions uir ON r.id = uir.reaction_id AND uir.instance_id = ?
            WHERE r.is_active = 1
            GROUP BY r.id
            ORDER BY r.sort_order ASC, r.name ASC;
        """
        params = (user_id, instance_id)
        rows = await execute_db_query(connection, query, params, fetch_all=True)

        # Convert the integer 'reacted_by_user' to a boolean for the response model.
        results = []
        for row in rows:
            row_dict = dict(row)
            row_dict['reacted_by_user'] = bool(row_dict['reacted_by_user'])
            results.append(row_dict)
        return results


async def get_instances_with_reaction_by_user(
    user_id: int,
    reaction_id: int,
    page: int = 1,
    page_size: int = 24,
    conn: Optional[aiosqlite.Connection] = None
) -> Dict[str, Any]:
    """
    Asynchronously finds all file instances that a specific user has reacted to in a certain way.
    
    Args:
        user_id: ID of the user.
        reaction_id: The ID of the reaction to filter by.
        page: The page number for pagination (1-indexed).
        page_size: The number of items per page.
        conn: An optional aiosqlite connection for transactional operations.

    Returns:
        A dictionary with pagination info and a list of instance card data.
    """
    async with AsyncDBContext(conn) as connection:
        offset = (page - 1) * page_size
        count_query = "SELECT COUNT(instance_id) FROM user_instance_reactions WHERE user_id = ? AND reaction_id = ?"
        total_row = await execute_db_query(connection, count_query, (user_id, reaction_id), fetch_one=True)
        total_count = total_row[0] if total_row else 0
        
        items_query = """
            SELECT fi.id, fi.file_name, fc.file_type, fc.is_encrypted, fc.file_hash
            FROM file_instances fi
            JOIN file_content fc ON fi.content_id = fc.id
            JOIN user_instance_reactions uir ON fi.id = uir.instance_id
            WHERE uir.user_id = ? AND uir.reaction_id = ?
            ORDER BY uir.created_at DESC LIMIT ? OFFSET ?
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
            "SELECT id, name, label, emoji, image_path, description FROM reactions "
            "WHERE is_active = 1 ORDER BY sort_order ASC, name ASC"
        )
        reactions_rows = await execute_db_query(connection, query, fetch_all=True)
        return [dict(row) for row in reactions_rows]
        
