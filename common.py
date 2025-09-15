import logging
import aiosqlite 
from typing import Optional, Set


from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query

config = get_config()
# --- Import the new async db utils ---
# Assuming db_utils.py is in the same directory. Adjust the path if necessary.


logger = logging.getLogger("ConmonLog")
logger.setLevel(logging.DEBUG)



IS_PRODUCTION = False


async def check_database_requirements(
    required_tables: Set[str],
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously checks if required tables exist in the database. Returns True if all exist, False otherwise.

    Uses the AsyncDBContext to manage the connection. It can be used standalone or as part of a
    larger transaction by passing an existing aiosqlite connection.

    Args:
        required_tables: A set of table names to check for (e.g., {'users', 'files'}).
        conn: (Optional) An active aiosqlite database connection to use instead of creating a new one.

    Returns:
        True if all required tables are found, False otherwise.
    """
    try:
        # The context manager handles creating/closing a connection
        # or using the one that is passed in, simplifying the logic.
        async with AsyncDBContext(external_conn=conn) as db_conn:
            query = "SELECT name FROM sqlite_master WHERE type='table';"
            # Use the helper to execute the query and fetch all results
            rows = await execute_db_query(db_conn, query, fetch_all=True)
            # The row_factory gives us dict-like access by column name, which is more readable.
            existing_tables = {row['name'] for row in rows}

        # Check for missing tables after the connection context is safely closed.
        missing_tables = required_tables - existing_tables
        if missing_tables:
            logger.critical(
                f"Database is missing required tables: {', '.join(missing_tables)}."
            )
            return False
        else:
            logger.info(f"Database requirements met for tables: {', '.join(required_tables)}.")
            return True

    except aiosqlite.Error as e:
        db_file_path = config.get('DB_FILE', 'N/A')
        logger.critical(f"Database error during requirement check for '{db_file_path}': {e}", exc_info=True)
        return False
    except Exception as e:
        logger.critical(f"An unexpected error occurred during database requirement check: {e}", exc_info=True)
        return False