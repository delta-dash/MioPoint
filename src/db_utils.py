# db_utils.py 
from typing import Any, Optional, AsyncGenerator

import aiosqlite
from ConfigMedia import get_config
import logging


logger = logging.getLogger("DB_UTILS")
logger.setLevel(logging.DEBUG)
config = get_config()

# --- Async Database Dependency ---

async def get_db_connection() -> AsyncGenerator[aiosqlite.Connection, None]:
    """
    FastAPI dependency to get an async database connection and handle transactions.
    """
    try:
        async with aiosqlite.connect(config["DB_FILE"]) as conn:
            conn.row_factory = aiosqlite.Row # Use Row factory for dict-like access
            await conn.execute("PRAGMA foreign_keys = ON;")
            yield conn
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Database transaction failed, rolling back. Error: {e}", exc_info=True)
        # Rollback is implicitly handled by the context manager on error
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_db_connection: {e}", exc_info=True)
        raise e

# --- Async Context Manager for DB Operations ---

class AsyncDBContext:
    """
    Manages an async database connection, allowing for external (shared) or internal (new) management.
    This is useful for service layer functions that might be part of a larger transaction.
    """
    def __init__(self, external_conn: Optional[aiosqlite.Connection] = None):
        self._external_conn = external_conn
        self._conn: Optional[aiosqlite.Connection] = None
        self._managed_conn = False

    async def __aenter__(self) -> aiosqlite.Connection:
        if self._external_conn:
            logger.debug("Using external connection within AsyncDBContext.")
            self._conn = self._external_conn
        else:
            logger.debug("Creating new connection within AsyncDBContext.")
            # --- THIS LINE WAS CORRECTED for consistency ---
            self._conn = await aiosqlite.connect(config['DB_FILE'])
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA foreign_keys = ON;")
            self._managed_conn = True
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._managed_conn and self._conn:
            logger.debug("Closing managed connection.")
            if exc_type:
                await self._conn.rollback()
                logger.debug("Rolled back transaction due to exception.")
            else:
                await self._conn.commit()
            await self._conn.close()
        # Do not commit/rollback/close external connections; let the caller manage them.
        return False # Propagate exceptions

# --- Core Async DB Execution Function ---
async def execute_db_query(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple = (),
    fetch_one: bool = False,
    fetch_all: bool = False,
    return_last_row_id: bool = False
) -> Any:
    """
    Executes an async database query using a provided aiosqlite connection.
    Commit/rollback is handled by the context managing the connection.
    """
    try:
        logger.debug(f"Executing query: {query} with params: {params}")
        async with conn.cursor() as cursor:
            await cursor.execute(query, params)

            if return_last_row_id:
                return cursor.lastrowid
            elif fetch_one:
                return await cursor.fetchone()
            elif fetch_all:
                return await cursor.fetchall()
            else:
                return cursor.rowcount  # For INSERT/UPDATE/DELETE

    except Exception as e:
        logger.error(f"Error executing query '{query}': {e}", exc_info=True)
        raise e