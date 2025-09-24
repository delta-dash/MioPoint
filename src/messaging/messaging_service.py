#messaging_service.py
"""
Messaging Service Module

Handles the business logic for all user-generated threaded content, including:
- Direct Messages (DMs)
- Group Chats
- Multiple Watch Party Chats per file/content
- Comment Sections with nested replies

Features:
- Thread ownership and roles (owner, moderator, member).
- Unique invite codes for joinable threads (groups, watch parties).
- RBAC integration for granular permission control.
- Nested post/reply structure.
- Transactional database operations via AsyncDBContext.
- Integrated audit logging and user notifications.
"""
import aiosqlite
import secrets
from typing import Any, Dict, List, Optional, Set, Tuple

import logging
from src.db_utils import AsyncDBContext, execute_db_query
from src.notifications.notification_service import create_notification
from src.roles.permissions import Permission
from src.roles.rbac_manager import user_has_permission

# --- INTEGRATION ---
# Import the necessary functions for logging and notifications.
# Ensure the import path is correct for your project structure.
from src.log.HelperLog import log_event_async


logger = logging.getLogger("MessagingService")
logger.setLevel(logging.DEBUG)


# ==============================================================================
# --- DATABASE SETUP (Run once at startup) ---
# ==============================================================================

async def setup_messaging_database(conn: Optional[aiosqlite.Connection] = None):
    """
    Creates or updates the database schema for the messaging service.
    """
    async with AsyncDBContext(conn) as db:
        await db.execute("PRAGMA foreign_keys = ON;")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL CHECK(type IN ('direct_message', 'group', 'comment_section', 'watch_party', 'public_group')),
                name TEXT,
                picture_url TEXT,
                description TEXT,
                invite_code TEXT UNIQUE,   -- For joinable threads (groups, watch parties) or predictable keys (DMs)
                content_id INTEGER,        -- For linking watch parties or comments to a file/content ID
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
            );
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_threads_type_content ON threads (type, content_id);")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS thread_members (
                thread_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('owner', 'moderator', 'member')) DEFAULT 'member',
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (thread_id, user_id),
                FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id INTEGER NOT NULL, sender_id INTEGER NOT NULL,
                parent_id INTEGER, content TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                edited_at TIMESTAMP,
                FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
                FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES posts(id) ON DELETE CASCADE
            );
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_posts_thread_parent ON posts (thread_id, parent_id, created_at);")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS banned_thread_users (
                thread_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (thread_id, user_id),
                FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """)

        await db.execute("DELETE FROM threads WHERE type = 'watch_party';")

        await db.commit()
    logger.debug("Messaging database schema is verified.")


# ==============================================================================
# --- PRIVATE HELPER FUNCTIONS ---
# ==============================================================================

async def _get_user_thread_role_logic(db: aiosqlite.Connection, user_id: int, thread_id: int) -> Optional[str]:
    """Core logic to get a user's role within a specific thread."""
    query = "SELECT role FROM thread_members WHERE user_id = ? AND thread_id = ?"
    result = await execute_db_query(db, query, (user_id, thread_id), fetch_one=True)
    return result['role'] if result else None

async def _generate_unique_invite_code(db: aiosqlite.Connection, length: int = 8) -> str:
    """Generates a cryptographically secure, URL-safe invite code and ensures it's unique."""
    while True:
        code = secrets.token_urlsafe(length)
        res = await execute_db_query(db, "SELECT 1 FROM threads WHERE invite_code = ?", (code,), fetch_one=True)
        if not res:
            return code

# --- INTEGRATION ---
async def _get_thread_members_for_notification(db: aiosqlite.Connection, thread_id: int, exclude_user_id: int) -> List[int]:
    """Retrieves all member IDs in a thread, excluding one specific user (usually the actor)."""
    query = "SELECT user_id FROM thread_members WHERE thread_id = ? AND user_id != ?"
    results = await execute_db_query(db, query, (thread_id, exclude_user_id), fetch_all=True)
    return [row['user_id'] for row in results] if results else []

# ==============================================================================
# --- THREAD MANAGEMENT (Public) ---
# ==============================================================================

async def create_direct_message_thread(creator_id: int, recipient_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[int]:
    """Creates a 1-on-1 DM thread. Idempotent. Requires THREAD_CREATE_DM permission."""
    if not await user_has_permission(creator_id, Permission.THREAD_CREATE_DM, conn):
        logger.warning(f"User {creator_id} denied creation of DM: lacks THREAD_CREATE_DM permission.")
        return None
    if creator_id == recipient_id:
        raise ValueError("Cannot create a direct message with yourself.")

    user_ids = sorted([creator_id, recipient_id])
    dm_key = f"dm:{user_ids[0]}-{user_ids[1]}"

    async with AsyncDBContext(conn) as db:
        existing = await execute_db_query(db, "SELECT id FROM threads WHERE invite_code = ?", (dm_key,), fetch_one=True)
        if existing:
            return existing['id']

        params = ('direct_message', dm_key, creator_id)
        thread_id = await execute_db_query(db, "INSERT INTO threads (type, invite_code, created_by) VALUES (?, ?, ?)", params, return_last_row_id=True)
        if not thread_id: return None

        members_data = [(thread_id, user_id, 'owner') for user_id in user_ids]
        await db.executemany("INSERT INTO thread_members (thread_id, user_id, role) VALUES (?, ?, ?)", members_data)
        logger.info(f"Created DM thread {thread_id} between users {creator_id} and {recipient_id}.")
        
        # --- INTEGRATION: LOGGING ---
        await log_event_async(
            conn=db,
            event_type="dm_thread_created",
            actor_type="user",
            actor_name=str(creator_id),
            target_type="thread",
            target_id=thread_id,
            details={"participants": user_ids}
        )
        return thread_id

async def create_group_thread(creator_id: int, name: str, participant_ids: Set[int], is_public: bool = False, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """Creates a group thread. Requires THREAD_CREATE_GROUP permission."""
    permission_needed = Permission.THREAD_CREATE_PUBLIC if is_public else Permission.THREAD_CREATE_GROUP
    if not await user_has_permission(creator_id, permission_needed, conn):
        logger.warning(f"User {creator_id} denied creation of group: lacks {permission_needed.name} permission.")
        return None

    thread_type = 'public_group' if is_public else 'group'
    async with AsyncDBContext(conn) as db:
        invite_code = await _generate_unique_invite_code(db)
        params = (thread_type, name, invite_code, creator_id)
        thread_id = await execute_db_query(db, "INSERT INTO threads (type, name, invite_code, created_by) VALUES (?, ?, ?, ?)", params, return_last_row_id=True)
        if not thread_id: return None

        all_user_ids = participant_ids | {creator_id}
        members_data = [(thread_id, uid, 'owner' if uid == creator_id else 'member') for uid in all_user_ids]
        await db.executemany("INSERT INTO thread_members (thread_id, user_id, role) VALUES (?, ?, ?)", members_data)
        
        # --- INTEGRATION: LOGGING ---
        await log_event_async(
            conn=db,
            event_type="group_thread_created",
            actor_type="user",
            actor_name=str(creator_id),
            target_type="thread",
            target_id=thread_id,
            details={"name": name, "member_count": len(all_user_ids), "is_public": is_public}
        )

        # --- INTEGRATION: NOTIFICATIONS ---
        if not is_public:
            creator_username_row = await execute_db_query(db, "SELECT username FROM users WHERE id=?", (creator_id,), fetch_one=True)
            creator_username = creator_username_row['username'] if creator_username_row else "A user"
            
            for user_id in participant_ids: # participant_ids excludes the creator
                await create_notification(
                    conn=db,
                    recipient_id=user_id,
                    event_type="thread_invite",
                    actor_id=creator_id,
                    context_data={
                        "thread_id": thread_id,
                        "thread_name": name,
                        "actor_username": creator_username
                    }
                )

        logger.info(f"Created group thread '{name}' ({thread_id}) by user {creator_id}.")
        new_thread = await execute_db_query(db, "SELECT * FROM threads WHERE id=?", (thread_id,), fetch_one=True)
        return dict(new_thread) if new_thread else None

async def update_thread_name(thread_id: int, name: str, user_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Updates the name of a thread if the user has permission."""
    async with AsyncDBContext(conn) as db:
        user_role = await _get_user_thread_role_logic(db, user_id, thread_id)
        if user_role not in ['owner', 'moderator']:
            logger.warning(f"User {user_id} denied name change for thread {thread_id}: not an owner or moderator.")
            return False

        rows_affected = await execute_db_query(
            db,
            "UPDATE threads SET name = ? WHERE id = ?",
            (name, thread_id),
        )
        if rows_affected > 0:
            logger.info(f"Thread {thread_id} name changed to '{name}' by user {user_id}.")
            await log_event_async(
                conn=db,
                event_type="thread_name_updated",
                actor_type="user",
                actor_name=str(user_id),
                target_type="thread",
                target_id=thread_id,
                details={"new_name": name}
            )
        return rows_affected > 0

async def get_or_create_comment_thread(creator_id: int, content_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[int]:
    """Gets an existing comment thread for content or creates it."""
    context_key = f"file:{content_id}"
    async with AsyncDBContext(conn) as db:
        existing = await execute_db_query(db, "SELECT id FROM threads WHERE invite_code = ?", (context_key,), fetch_one=True)
        if existing:
            return existing['id']

        params = ('comment_section', context_key, content_id, creator_id)
        thread_id = await execute_db_query(db, "INSERT INTO threads (type, invite_code, content_id, created_by) VALUES (?, ?, ?, ?)", params, return_last_row_id=True)
        logger.info(f"Created comment thread {thread_id} for content '{content_id}'.")
        return thread_id


# ==============================================================================
# --- WATCH PARTY WORKFLOW (Public) ---
# ==============================================================================

async def create_watch_party_thread(creator_id: int, file_id: int, name: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """Creates a new watch party. Requires THREAD_CREATE_WATCH_PARTY permission."""
    if not await user_has_permission(creator_id, Permission.THREAD_CREATE_WATCH_PARTY, conn):
        logger.warning(f"User {creator_id} denied creation of watch party: lacks permission.")
        return None

    async with AsyncDBContext(conn) as db:
        invite_code = await _generate_unique_invite_code(db)
        params = ('watch_party', name, invite_code, file_id, creator_id)
        thread_id = await execute_db_query(db, "INSERT INTO threads (type, name, invite_code, content_id, created_by) VALUES (?, ?, ?, ?, ?)", params, return_last_row_id=True)
        if not thread_id: return None

        await db.execute("INSERT INTO thread_members (thread_id, user_id, role) VALUES (?, ?, 'owner')", (thread_id, creator_id))
        
        # --- INTEGRATION: LOGGING ---
        await log_event_async(
            conn=db,
            event_type="watch_party_created",
            actor_type="user",
            actor_name=str(creator_id),
            target_type="thread",
            target_id=thread_id,
            details={"name": name, "file_id": file_id}
        )
        
        logger.info(f"User {creator_id} created new watch party '{name}' ({thread_id}) for file {file_id}.")
        new_thread = await execute_db_query(db, "SELECT * FROM threads WHERE id = ?", (thread_id,), fetch_one=True)
        return dict(new_thread) if new_thread else None

async def list_watch_parties_for_file(file_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """Retrieves active watch parties for a file, sorted by creation date."""
    async with AsyncDBContext(conn) as db:
        # The query now handles the sorting
        query_sorted = """
            SELECT t.id, t.name, t.invite_code, t.created_at,
                   (SELECT COUNT(tm.user_id) FROM thread_members tm WHERE tm.thread_id = t.id) as member_count
            FROM threads t
            WHERE t.type = 'watch_party' AND t.content_id = ?
            ORDER BY t.created_at DESC;
        """
        all_parties = await execute_db_query(db, query_sorted, (file_id,), fetch_all=True)

        if not all_parties:
            return []

        # No more sorting needed here!
        # Just convert the rows to dicts and return.
        return [dict(party) for party in all_parties]

async def get_user_active_watch_party(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """
    Finds the active watch party for a given user, if any.
    Returns details about the party including thread_id, content_id, and the user's role.
    """
    query = """
        SELECT
            t.id as thread_id,
            t.content_id,
            tm.role
        FROM thread_members tm
        JOIN threads t ON tm.thread_id = t.id
        WHERE tm.user_id = ? AND t.type = 'watch_party'
        LIMIT 1;
    """
    async with AsyncDBContext(conn) as db:
        party_data = await execute_db_query(db, query, (user_id,), fetch_one=True)
        return dict(party_data) if party_data else None


async def join_watch_party_thread(user_id: int, invite_code: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """
    Adds a user to a watch party. If the user is already in another watch party,
    they will be removed from it first.
    Returns a dictionary with details about the join and any party that was left.
    """
    async with AsyncDBContext(conn) as db:
        # 1. Find the party the user wants to join.
        new_party_data = await execute_db_query(
            db,
            "SELECT id, content_id FROM threads WHERE invite_code = ? AND type = 'watch_party'",
            (invite_code,),
            fetch_one=True
        )
        if not new_party_data:
            logger.warning(f"User {user_id} failed to join watch party: invite code '{invite_code}' not found.")
            return None
        new_thread_id = new_party_data['id']

        # 2. Find if the user is in any *other* watch party.
        old_party_query = """
            SELECT tm.thread_id, t.content_id
            FROM thread_members tm
            JOIN threads t ON tm.thread_id = t.id
            WHERE tm.user_id = ? AND t.type = 'watch_party' AND t.id != ?
        """
        old_party_data = await execute_db_query(db, old_party_query, (user_id, new_thread_id), fetch_one=True)

        left_party_details = None
        if old_party_data:
            old_thread_id = old_party_data['thread_id']
            old_content_id = old_party_data['content_id']
            logger.info(f"User {user_id} is leaving old watch party {old_thread_id} to join new one {new_thread_id}.")
            
            # 3. Remove user from the old party.
            success, new_owner_id = await leave_thread(user_id=user_id, thread_id=old_thread_id, conn=db)
            if success:
                left_party_details = {
                    "thread_id": old_thread_id,
                    "content_id": old_content_id,
                    "new_owner_id": new_owner_id
                }

        # 4. Add the user to the new party.
        await db.execute("INSERT OR IGNORE INTO thread_members (thread_id, user_id, role) VALUES (?, ?, 'member')", (new_thread_id, user_id))
        
        # --- INTEGRATION: LOGGING ---
        await log_event_async(conn=db, event_type="watch_party_joined", actor_type="user", actor_name=str(user_id), target_type="thread", target_id=new_thread_id, details={"invite_code": invite_code, "file_id": new_party_data['content_id']})

        logger.info(f"User {user_id} is now in watch party {new_thread_id} (invite code '{invite_code}').")
        return {
            "thread_id": new_thread_id,
            "left_party": left_party_details
        }

async def update_watch_party_file(thread_id: int, new_file_id: int, acting_user_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """
    Updates the content_id for a watch party thread.
    This is typically called when the party owner changes the video.
    The calling handler is responsible for verifying the user's permission.
    """
    async with AsyncDBContext(conn) as db:
        # We only update threads of type 'watch_party' to be safe.
        rows_affected = await execute_db_query(
            db,
            "UPDATE threads SET content_id = ? WHERE id = ? AND type = 'watch_party'",
            (new_file_id, thread_id),
        )

        if rows_affected > 0:
            logger.info(f"Watch party thread {thread_id} changed file to content_id {new_file_id} by user {acting_user_id}.")
            # Log the event
            await log_event_async(
                conn=db,
                event_type="watch_party_file_changed",
                actor_type="user",
                actor_name=str(acting_user_id),
                target_type="thread",
                target_id=thread_id,
                details={"new_content_id": new_file_id}
            )
            return True
        else:
            logger.warning(f"Failed to update file for watch party {thread_id}. It might not exist or is not a watch party.")
            return False

async def leave_thread(user_id: int, thread_id: int, conn: Optional[aiosqlite.Connection] = None) -> Tuple[bool, Optional[int]]:
    """
    Removes a user from a thread's member list in the database.
    If the leaving user is the owner, it attempts to transfer ownership.
    Returns a tuple of (success, new_owner_id).
    """
    new_owner_id: Optional[int] = None
    async with AsyncDBContext(conn) as db:
        # Get the user's role before they leave
        user_role = await _get_user_thread_role_logic(db, user_id, thread_id)

        # Remove the user from the thread
        rows_affected = await execute_db_query(
            db,
            "DELETE FROM thread_members WHERE thread_id = ? AND user_id = ?",
            (thread_id, user_id),
        )

        if rows_affected > 0:
            logger.info(f"User {user_id} was removed from thread {thread_id} members.")
            await log_event_async(
                conn=db,
                event_type="thread_member_left",
                actor_type="user",
                actor_name=str(user_id),
                target_type="thread",
                target_id=thread_id
            )

            # If the owner left, transfer ownership
            if user_role == 'owner':
                remaining_members = await execute_db_query(
                    db,
                    "SELECT user_id FROM thread_members WHERE thread_id = ? ORDER BY joined_at ASC LIMIT 1",
                    (thread_id,),
                    fetch_one=True
                )
                if remaining_members:
                    new_owner_id = remaining_members['user_id']
                    await execute_db_query(
                        db,
                        "UPDATE thread_members SET role = 'owner' WHERE thread_id = ? AND user_id = ?",
                        (thread_id, new_owner_id)
                    )
                    logger.info(f"Transferred ownership of thread {thread_id} to user {new_owner_id}.")
                    await log_event_async(
                        conn=db,
                        event_type="thread_ownership_transferred",
                        actor_type="system",
                        actor_name="auto_transfer",
                        target_type="thread",
                        target_id=thread_id,
                        details={"new_owner_id": new_owner_id}
                    )

        return rows_affected > 0, new_owner_id


async def delete_watch_party_thread(thread_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """
    Deletes a watch party thread from the database.
    This is intended for cleanup when a party becomes empty.
    """
    async with AsyncDBContext(conn) as db:
        # We only delete threads of type 'watch_party' to be safe.
        rows_affected = await execute_db_query(
            db,
            "DELETE FROM threads WHERE id = ? AND type = 'watch_party'",
            (thread_id,),
        )

        if rows_affected > 0:
            logger.info(f"Successfully deleted empty watch party thread {thread_id}.")
            # Log the deletion event
            await log_event_async(
                conn=db,
                event_type="watch_party_deleted",
                actor_type="system",
                actor_name="auto_cleanup",
                target_type="thread",
                target_id=thread_id,
                details={"reason": "Party became empty."}
            )
            return True
        return False

# ==============================================================================
# --- POST MANAGEMENT (Public) ---
# ==============================================================================

async def create_post(sender_id: int, thread_id: int, content: str, parent_id: Optional[int] = None, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """Creates a post (message or reply). Requires POST_CREATE and membership (except for comment sections)."""
    if not content.strip(): raise ValueError("Content cannot be empty.")
    async with AsyncDBContext(conn) as db:
        if not await user_has_permission(sender_id, Permission.POST_CREATE, db):
            logger.warning(f"User {sender_id} denied post creation: lacks POST_CREATE permission.")
            return None

        # A user can post if they are a member (have a role) OR if it's a public comment section.
        user_role = await _get_user_thread_role_logic(db, sender_id, thread_id)
        thread_info = await execute_db_query(db, "SELECT type, name FROM threads WHERE id=?", (thread_id,), fetch_one=True)
        is_comment_section = thread_info and thread_info['type'] == 'comment_section'
        if not user_role and not is_comment_section:
            logger.warning(f"User {sender_id} tried to post in thread {thread_id} but is not a member.")
            return None

        if parent_id and not await execute_db_query(db, "SELECT 1 FROM posts WHERE id=? AND thread_id=?", (parent_id, thread_id), fetch_one=True):
            raise ValueError(f"Parent post {parent_id} does not exist in thread {thread_id}.")

        params = (thread_id, sender_id, content, parent_id)
        post_id = await execute_db_query(db, "INSERT INTO posts (thread_id, sender_id, content, parent_id) VALUES (?, ?, ?, ?)", params, return_last_row_id=True)
        if not post_id: return None

        new_post_query = "SELECT p.*, u.username as sender_username FROM posts p JOIN users u ON p.sender_id=u.id WHERE p.id=?"
        new_post = await execute_db_query(db, new_post_query, (post_id,), fetch_one=True)
        if not new_post: return None

        # --- INTEGRATION: LOGGING ---
        await log_event_async(
            conn=db,
            event_type="post_created",
            actor_type="user",
            actor_name=str(sender_id),
            target_type="thread",
            target_id=thread_id,
            details={"post_id": post_id, "content_preview": content[:75]}
        )

        # --- INTEGRATION: NOTIFICATIONS ---
        recipients = await _get_thread_members_for_notification(db, thread_id, exclude_user_id=sender_id)
        for recipient_id in recipients:
            await create_notification(
                conn=db,
                recipient_id=recipient_id,
                event_type="new_reply",
                actor_id=sender_id,
                context_data={
                    "thread_id": thread_id,
                    "thread_name": thread_info['name'] , #thread_info dont have get method
                    "post_id": post_id,
                    "actor_username": new_post['sender_username']
                }
            )
        
        return dict(new_post)


async def delete_post(deleting_user_id: int, post_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Deletes a post based on ownership or thread/global permissions."""
    async with AsyncDBContext(conn) as db:
        post_data = await execute_db_query(db, "SELECT sender_id, thread_id FROM posts WHERE id=?", (post_id,), fetch_one=True)
        if not post_data: return False

        sender_id, thread_id = post_data['sender_id'], post_data['thread_id']
        can_delete = False

        if sender_id == deleting_user_id and await user_has_permission(deleting_user_id, Permission.POST_DELETE_OWN, db):
            can_delete = True
        if not can_delete:
            user_role = await _get_user_thread_role_logic(db, deleting_user_id, thread_id)
            if user_role in ('owner', 'moderator'): can_delete = True
        if not can_delete and await user_has_permission(deleting_user_id, Permission.POST_DELETE_ANY, db):
            can_delete = True

        if can_delete:
            if await execute_db_query(db, "DELETE FROM posts WHERE id=?", (post_id,)) > 0:
                logger.info(f"User {deleting_user_id} deleted post {post_id}.")
                
                # --- INTEGRATION: LOGGING ---
                await log_event_async(
                    conn=db,
                    event_type="post_deleted",
                    actor_type="user",
                    actor_name=str(deleting_user_id),
                    target_type="post",
                    target_id=post_id,
                    details={"thread_id": thread_id, "original_sender_id": sender_id}
                )
                return True
    
    logger.warning(f"User {deleting_user_id} failed to delete post {post_id} due to lack of permissions.")
    return False

# ==============================================================================
# --- DATA RETRIEVAL (Public) ---
# ==============================================================================
# (These functions are read-only and do not require logging/notifications)

async def get_public_threads(conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """Retrieves all public group threads."""
    query = "SELECT id, type, name, picture_url FROM threads WHERE type = 'public_group' ORDER BY created_at DESC"
    async with AsyncDBContext(conn) as db:
        rows = await execute_db_query(db, query, fetch_all=True)
        return [dict(row) for row in rows] if rows else []

async def join_thread(user_id: int, thread_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Adds a user to a thread, typically a public one."""
    async with AsyncDBContext(conn) as db:
        thread_info = await execute_db_query(db, "SELECT type FROM threads WHERE id=?", (thread_id,), fetch_one=True)
        if not thread_info or thread_info['type'] not in ['public_group', 'watch_party']:
            logger.warning(f"User {user_id} cannot join thread {thread_id} as it is not public or a watch party.")
            return False

        await db.execute("INSERT OR IGNORE INTO thread_members (thread_id, user_id, role) VALUES (?, ?, 'member')", (thread_id, user_id))
        logger.info(f"User {user_id} joined thread {thread_id}.")
        await log_event_async(
            conn=db,
            event_type="thread_member_joined",
            actor_type="user",
            actor_name=str(user_id),
            target_type="thread",
            target_id=thread_id
        )
        return True

async def get_thread_details(thread_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """Fetches basic details for a single thread, like its content_id."""
    async with AsyncDBContext(conn) as db:
        query = "SELECT id, type, name, content_id FROM threads WHERE id = ?"
        row = await execute_db_query(db, query, (thread_id,), fetch_one=True)
        return dict(row) if row else None

async def get_user_threads(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """Retrieves all threads a user is a member of."""
    query = """
        SELECT t.id, t.type, t.name, t.picture_url, tm.role FROM threads t
        JOIN thread_members tm ON t.id = tm.thread_id
        WHERE tm.user_id = ? ORDER BY t.created_at DESC
    """
    async with AsyncDBContext(conn) as db:
        rows = await execute_db_query(db, query, (user_id,), fetch_all=True)
        return [dict(row) for row in rows] if rows else []

async def get_thread_members(thread_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """Retrieves all members of a specific thread."""
    query = """
        SELECT u.id, u.username, tm.role
        FROM users u
        JOIN thread_members tm ON u.id = tm.user_id
        WHERE tm.thread_id = ?
        ORDER BY u.username
    """
    async with AsyncDBContext(conn) as db:
        rows = await execute_db_query(db, query, (thread_id,), fetch_all=True)
        return [dict(row) for row in rows] if rows else []

async def get_posts_for_thread(thread_id: int, viewing_user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[List[Dict[str, Any]]]:
    """Fetches all posts for a thread and structures them into a nested tree."""
    async with AsyncDBContext(conn) as db:
        user_role = await _get_user_thread_role_logic(db, viewing_user_id, thread_id)
        thread_info = await execute_db_query(db, "SELECT type FROM threads WHERE id=?", (thread_id,), fetch_one=True)
        is_public = thread_info and thread_info['type'] in ('comment_section', 'watch_party', 'public_group')
        if not user_role and not is_public:
            logger.warning(f"User {viewing_user_id} denied access to posts in thread {thread_id}.")
            return None

        query = """
            SELECT p.id, p.thread_id, p.sender_id, p.parent_id, p.content, p.created_at, u.username as sender_username
            FROM posts p JOIN users u ON p.sender_id = u.id
            WHERE p.thread_id = ? ORDER BY p.created_at ASC
        """
        flat_posts = await execute_db_query(db, query, (thread_id,), fetch_all=True)

    if not flat_posts: return []
    post_map = {dict(p)['id']: dict(p) for p in flat_posts}
    nested_posts = []
    for post_id, post in post_map.items():
        post['replies'] = []
        parent_id = post.get('parent_id')
        if parent_id and parent_id in post_map:
            post_map[parent_id]['replies'].append(post)
        else:
            nested_posts.append(post)
    return nested_posts

async def transfer_thread_ownership(thread_id: int, current_owner_id: int, new_owner_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Transfers ownership of a thread from the current owner to a new owner."""
    async with AsyncDBContext(conn) as db:
        # Verify current owner
        current_owner_role = await _get_user_thread_role_logic(db, current_owner_id, thread_id)
        if current_owner_role != 'owner':
            logger.warning(f"User {current_owner_id} attempted to transfer ownership of thread {thread_id} but is not the owner.")
            return False

        # Verify new owner is a member
        new_owner_role = await _get_user_thread_role_logic(db, new_owner_id, thread_id)
        if new_owner_role is None:
            logger.warning(f"User {new_owner_id} cannot be made owner of thread {thread_id} because they are not a member.")
            return False

        # Update roles
        await execute_db_query(db, "UPDATE thread_members SET role = 'member' WHERE thread_id = ? AND user_id = ?", (thread_id, current_owner_id))
        await execute_db_query(db, "UPDATE thread_members SET role = 'owner' WHERE thread_id = ? AND user_id = ?", (thread_id, new_owner_id))

        logger.info(f"Transferred ownership of thread {thread_id} from user {current_owner_id} to user {new_owner_id}.")
        await log_event_async(
            conn=db,
            event_type="thread_ownership_transferred",
            actor_type="user",
            actor_name=str(current_owner_id),
            target_type="thread",
            target_id=thread_id,
            details={"new_owner_id": new_owner_id}
        )
        return True

async def ban_user_from_thread(thread_id: int, banning_user_id: int, banned_user_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Bans a user from a thread."""
    async with AsyncDBContext(conn) as db:
        # Verify banning user is owner
        banning_user_role = await _get_user_thread_role_logic(db, banning_user_id, thread_id)
        if banning_user_role != 'owner':
            logger.warning(f"User {banning_user_id} attempted to ban user from thread {thread_id} but is not the owner.")
            return False

        # Remove user from thread_members
        await execute_db_query(db, "DELETE FROM thread_members WHERE thread_id = ? AND user_id = ?", (thread_id, banned_user_id))

        # Add user to banned_thread_users
        await execute_db_query(db, "INSERT OR IGNORE INTO banned_thread_users (thread_id, user_id) VALUES (?, ?)", (thread_id, banned_user_id))

        logger.info(f"User {banned_user_id} has been banned from thread {thread_id} by user {banning_user_id}.")
        await log_event_async(
            conn=db,
            event_type="user_banned_from_thread",
            actor_type="user",
            actor_name=str(banning_user_id),
            target_type="thread",
            target_id=thread_id,
            details={"banned_user_id": banned_user_id}
        )
        return True