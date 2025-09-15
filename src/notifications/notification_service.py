# notification_service.py

import json
import aiosqlite
from typing import Any, Dict, List, Optional

from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query
import logging

config = get_config()

logger = logging.getLogger("NotificationService")
logger.setLevel(logging.DEBUG)
async def setup_notifications_database_async():
    """
    Creates or updates the database schema for the notification system.

    This function establishes the 'notifications' table and its necessary indexes,
    ensuring foreign key constraints are enabled. It manages its own database
    connection and is designed to be run safely at application startup.
    """
    db_file = config.get("DB_FILE")
    if not db_file:
        logger.error("DB_FILE not found in configuration. Cannot set up notifications database.")
        raise ValueError("Database file path is not configured.")

    try:
        async with aiosqlite.connect(db_file) as conn:
            # Enforce foreign key constraints for data integrity
            await conn.execute("PRAGMA foreign_keys = ON;")

            # 1. Create the main 'notifications' table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    
                    -- The user who should receive the notification.
                    recipient_id INTEGER NOT NULL,
                    
                    -- The user who performed the action (e.g., who replied). Can be NULL for system notifications.
                    actor_id INTEGER,
                    
                    -- The type of event that triggered the notification.
                    event_type TEXT NOT NULL CHECK(event_type IN (
                        'new_reply', 
                        'mention', 
                        'thread_invite', 
                        'role_assigned'
                        -- Add other event types here as your application grows
                    )),
                    
                    -- Flag to track if the user has seen this notification (0=unread, 1=read).
                    is_read INTEGER NOT NULL DEFAULT 0,
                    
                    -- A JSON blob containing all context needed to display the notification.
                    -- e.g., {"thread_id": 1, "post_id": 105, "actor_username": "jane_doe"}
                    context_data TEXT NOT NULL,
                    
                    -- Timestamp for when the notification was generated.
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    -- Define foreign key relationships
                    FOREIGN KEY (recipient_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE SET NULL
                );
            """)

            # 2. Create an index for performant lookups of unread notifications
            # This is the most common query, so a dedicated index is crucial.
            # The index covers the main filter columns (recipient_id, is_read) and the sorting column (created_at).
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_notifications_recipient_unread
                ON notifications (recipient_id, is_read, created_at);
            """)

            await conn.commit()

        logger.info(f"Database schema for notifications in '{db_file}' is verified.")

    except aiosqlite.Error as e:
        logger.error(f"Failed to set up notifications database schema: {e}", exc_info=True)
        # Re-raise the exception to halt application startup, as this is a critical failure.
        raise

# ==============================================================================
# --- PUBLIC API ---
# ==============================================================================
# messaging_service.py

# ... after other private helpers

async def _get_thread_members_for_notification(db: aiosqlite.Connection, thread_id: int, exclude_user_id: int) -> List[int]:
    """Retrieves all member IDs in a thread, excluding one specific user (usually the actor)."""
    query = "SELECT user_id FROM thread_members WHERE thread_id = ? AND user_id != ?"
    results = await execute_db_query(db, query, (thread_id, exclude_user_id), fetch_all=True)
    return [row['user_id'] for row in results] if results else []

async def create_notification(
    recipient_id: int,
    event_type: str,
    actor_id: Optional[int] = None,
    context_data: Optional[Dict[str, Any]] = None,
    conn: Optional[aiosqlite.Connection] = None
) -> Optional[int]:
    """
    Creates a new notification for a user.
    This is the main entry point for other services.
    """
    if context_data is None:
        context_data = {}
        
    context_json = json.dumps(context_data)
    
    query = """
        INSERT INTO notifications (recipient_id, actor_id, event_type, context_data)
        VALUES (?, ?, ?, ?)
    """
    params = (recipient_id, actor_id, event_type, context_json)
    
    async with AsyncDBContext(conn) as db:
        try:
            notification_id = await execute_db_query(db, query, params, return_last_row_id=True)
            logger.debug(f"Created notification {notification_id} for user {recipient_id} (type: {event_type})")
            
            # TODO: Here you would trigger a real-time event, e.g., via WebSocket
            # await websocket_manager.send_to_user(recipient_id, {"type": "new_notification"})
            
            return notification_id
        except Exception as e:
            logger.error(f"Failed to create notification for user {recipient_id}: {e}", exc_info=True)
            return None


async def get_unread_notifications_for_user(
    user_id: int,
    limit: int = 20,
    conn: Optional[aiosqlite.Connection] = None
) -> List[Dict[str, Any]]:
    """Retrieves the most recent unread notifications for a user."""
    query = """
        SELECT id, actor_id, event_type, is_read, context_data, created_at
        FROM notifications
        WHERE recipient_id = ? AND is_read = 0
        ORDER BY created_at DESC
        LIMIT ?
    """
    async with AsyncDBContext(conn) as db:
        rows = await execute_db_query(db, query, (user_id, limit), fetch_all=True)
        
    notifications = []
    for row in rows:
        notif = dict(row)
        notif['context_data'] = json.loads(notif['context_data'])
        notifications.append(notif)
    return notifications

async def get_unread_notification_count(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> int:
    """Efficiently gets the count of unread notifications for a UI badge."""
    query = "SELECT COUNT(id) FROM notifications WHERE recipient_id = ? AND is_read = 0"
    async with AsyncDBContext(conn) as db:
        result = await execute_db_query(db, query, (user_id,), fetch_one=True)
    return result[0] if result else 0


async def mark_notification_as_read(notification_id: int, user_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Marks a single notification as read, ensuring the user owns it."""
    query = "UPDATE notifications SET is_read = 1 WHERE id = ? AND recipient_id = ?"
    async with AsyncDBContext(conn) as db:
        rows_affected = await execute_db_query(db, query, (notification_id, user_id), return_rows_affected=True)
    return rows_affected > 0

async def mark_all_notifications_as_read(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Marks all of a user's notifications as read."""
    # BUG FIX: This should be is_read = 1 to mark them as read.
    query = "UPDATE notifications SET is_read = 1 WHERE recipient_id = ? AND is_read = 0"
    async with AsyncDBContext(conn) as db:
        # We only update unread messages to get an accurate count of what changed.
        rows_affected = await execute_db_query(db, query, (user_id,), return_rows_affected=True)
    return rows_affected > 0