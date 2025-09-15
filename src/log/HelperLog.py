# HelperLog.py
import json
from typing import Any, Dict, List, Optional, Union

import aiosqlite
from ConfigMedia import get_config
import logging
from src.db_utils import AsyncDBContext, execute_db_query

config = get_config()

logger = logging.getLogger("HelperLog")
logger.setLevel(logging.DEBUG)

async def setup_log_database_async(conn: Optional[aiosqlite.Connection] = None):
    """
    Creates or updates the audit_log table in the database asynchronously.

    This can be called standalone (it will manage its own connection) or as part
    of a larger transaction by passing an active aiosqlite connection.

    Args:
        conn: (Optional) An active aiosqlite database connection.
    """
    try:
        # Use AsyncDBContext to either use the provided connection or create a new one.
        async with AsyncDBContext(conn) as db:
            logger.info("--- Setting up async schema for 'audit_log' ---")
            
            # 1. Main Audit Log Table
            await db.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT NOT NULL,
                actor_type TEXT NOT NULL,
                actor_name TEXT NOT NULL,
                target_type TEXT,
                target_id INTEGER,
                details TEXT
            );""")

            # 2. Indexes for efficient querying
            logger.debug("Creating indexes for audit_log table...")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_target ON audit_log (target_type, target_id);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_actor ON audit_log (actor_type, actor_name);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log (timestamp);")

            logger.info("Audit log schema is up-to-date.")

    except aiosqlite.Error as e:
        logger.exception(f"Database error during async audit log setup: {e}")
        # The AsyncDBContext will handle rollback on error; re-raising allows the caller to know.
        raise


async def log_event_async(
    event_type: str,
    actor_type: str,
    actor_name: str,
    conn: Optional[aiosqlite.Connection] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    details: Optional[Union[dict, str]] = None
):
    """
    Logs an event to the audit_log table asynchronously.

    Can be called standalone (manages its own connection) or as part of a larger
    transaction by passing an active aiosqlite connection.

    Args:
        event_type: A string describing the action (e.g., 'file_ingest', 'user_create').
        actor_type: The type of entity performing the action ('system', 'user').
        actor_name: The specific name/ID of the actor ('api', 'user:admin').
        conn: (Optional) An active aiosqlite connection for transactional logging.
        target_type: (Optional) The type of entity being acted upon ('file', 'user').
        target_id: (Optional) The ID of the target entity.
        details: (Optional) A string or dictionary with extra context. Dictionaries are saved as JSON.
    """
    # Serialize details to JSON if it's a dictionary
    details_json = json.dumps(details) if isinstance(details, dict) else details

    sql = """
        INSERT INTO audit_log (event_type, actor_type, actor_name, target_type, target_id, details)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    params = (event_type, actor_type, actor_name, target_type, target_id, details_json)

    try:
        # Use AsyncDBContext to manage the transaction.
        async with AsyncDBContext(conn) as db:
            await execute_db_query(db, sql, params)

    except aiosqlite.Error as e:
        logger.exception(f"Failed to write to async audit log: {e}")
        # Re-raise to ensure the transaction is rolled back by the context manager
        raise

async def get_logs_async(
    conn: Optional[aiosqlite.Connection] = None,
    event_type: Optional[str] = None,
    actor_type: Optional[str] = None,
    actor_name: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str = "timestamp",
    order_direction: str = "DESC"
) -> List[Dict[str, Any]]:
    """
    Retrieves logs from the audit_log table with filtering and pagination.

    Args:
        conn: (Optional) An active aiosqlite connection.
        event_type: (Optional) Filter by the type of event.
        actor_type: (Optional) Filter by the type of actor.
        actor_name: (Optional) Filter by the specific actor's name.
        target_type: (Optional) Filter by the type of the target entity.
        target_id: (Optional) Filter by the ID of the target entity.
        start_time: (Optional) ISO 8601 formatted string for the start of a time range.
        end_time: (Optional) ISO 8601 formatted string for the end of a time range.
        limit: The maximum number of log entries to return.
        offset: The number of log entries to skip (for pagination).
        order_by: The column to sort the results by. Defaults to 'timestamp'.
        order_direction: The direction to sort ('ASC' or 'DESC'). Defaults to 'DESC'.

    Returns:
        A list of dictionaries, where each dictionary represents a log entry.
        The 'details' field is automatically JSON-decoded if it's a valid JSON string.
    """
    base_query = "SELECT * FROM audit_log"
    where_clauses = []
    params = []

    # --- Build dynamic WHERE clauses ---
    if event_type:
        where_clauses.append("event_type = ?")
        params.append(event_type)
    if actor_type:
        where_clauses.append("actor_type = ?")
        params.append(actor_type)
    if actor_name:
        where_clauses.append("actor_name = ?")
        params.append(actor_name)
    if target_type:
        where_clauses.append("target_type = ?")
        params.append(target_type)
    if target_id is not None:
        where_clauses.append("target_id = ?")
        params.append(target_id)
    if start_time:
        where_clauses.append("timestamp >= ?")
        params.append(start_time)
    if end_time:
        where_clauses.append("timestamp <= ?")
        params.append(end_time)

    # --- Combine clauses into a full query ---
    query = base_query
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    # --- Add ordering (with validation to prevent SQL injection) ---
    valid_order_columns = ['id', 'timestamp', 'event_type', 'actor_type', 'actor_name']
    if order_by not in valid_order_columns:
        raise ValueError(f"Invalid 'order_by' column: {order_by}. Must be one of {valid_order_columns}")
    if order_direction.upper() not in ['ASC', 'DESC']:
        raise ValueError(f"Invalid 'order_direction': {order_direction}. Must be 'ASC' or 'DESC'.")

    query += f" ORDER BY {order_by} {order_direction.upper()}"

    # --- Add pagination ---
    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    results = []
    try:
        async with AsyncDBContext(conn) as db:
            async with db.execute(query, tuple(params)) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                
                for row in rows:
                    log_entry = dict(zip(columns, row))
                    # Attempt to parse the 'details' field from JSON string to a dict
                    if isinstance(log_entry.get('details'), str):
                        try:
                            log_entry['details'] = json.loads(log_entry['details'])
                        except (json.JSONDecodeError, TypeError):
                            # Keep it as a string if it's not valid JSON
                            pass
                    results.append(log_entry)
        return results
    except aiosqlite.Error as e:
        logger.exception(f"Failed to retrieve logs: {e}")
        raise
