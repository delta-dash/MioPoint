import logging
from typing import Dict, List, Literal, Optional, Union
import aiosqlite
import base64
import json

from src.db_utils import AsyncDBContext, execute_db_query
from src.roles.rbac_manager import get_everyone_role_id
from src.tag_utils import _get_table_names
from src.tags.HelperTags import _resolve_tag_ids

logger = logging.getLogger("media_repository")


async def update_visibility_for_instances_by_tags(
    tags: List[Union[int, str]],
    new_role_ids: List[int],
    tag_type: str,
    conn: Optional[aiosqlite.Connection] = None
):
    """
    Finds all file instances associated with the given content tags and overwrites
    their visibility roles. This is a bulk operation performed in a single transaction.

    Args:
        tags: A list of tag names or IDs to identify the target content.
        new_role_ids: A list of role IDs to set for the found instances. If empty,
                      visibility will be set to the 'Everyone' role.
        tag_type: The type of tag, either 'tags' or 'meta_tags'.
        conn: An optional existing database connection.
    """
    if not tags:
        logger.warning("update_visibility_for_instances_by_tags called with no tags. No action taken.")
        return

    async with AsyncDBContext(conn) as db_conn:
        try:
            tables = _get_table_names(tag_type)
            junction_table = tables['file_junction']

            tag_ids = await _resolve_tag_ids(tags, tag_type, db_conn)
            if not tag_ids:
                logger.warning(f"No valid tags found for identifiers: {tags}. No instances updated.")
                return

            async with db_conn.execute("BEGIN"):
                # 1. Find all unique content IDs associated with these tags
                placeholders = ', '.join('?' for _ in tag_ids)
                query_content = f"SELECT DISTINCT content_id FROM {junction_table} WHERE tag_id IN ({placeholders})"
                content_rows = await execute_db_query(db_conn, query_content, tuple(tag_ids), fetch_all=True)
                content_ids = [row['content_id'] for row in content_rows]

                if not content_ids:
                    logger.info(f"No content found with the specified tags: {tags}. No visibility updated.")
                    return

                # 2. Find all file instances linked to that content
                content_placeholders = ', '.join('?' for _ in content_ids)
                query_instances = f"SELECT id FROM file_instances WHERE content_id IN ({content_placeholders})"
                instance_rows = await execute_db_query(db_conn, query_instances, tuple(content_ids), fetch_all=True)
                instance_ids_to_update = [row['id'] for row in instance_rows]

                if not instance_ids_to_update:
                    logger.info(f"Content found for tags {tags}, but no corresponding file instances exist. No visibility updated.")
                    return

                logger.info(f"Found {len(instance_ids_to_update)} instances to update visibility for tags: {tags}")

                # 3. Delete all existing visibility roles for these instances
                instance_placeholders = ', '.join('?' for _ in instance_ids_to_update)
                await execute_db_query(
                    db_conn,
                    f"DELETE FROM instance_visibility_roles WHERE instance_id IN ({instance_placeholders})",
                    tuple(instance_ids_to_update)
                )

                # 4. Prepare and insert the new roles
                roles_to_insert = list(set(new_role_ids))
                if not roles_to_insert:
                    try:
                        everyone_id = get_everyone_role_id()
                        roles_to_insert.append(everyone_id)
                        logger.debug("Role list was empty. Defaulting to 'Everyone' role for bulk update.")
                    except RuntimeError as e:
                        logger.error(f"Could not get 'Everyone' role ID during bulk update: {e}. Instances will have NO visibility.")
                
                if roles_to_insert:
                    params_to_insert = [
                        (instance_id, role_id)
                        for instance_id in instance_ids_to_update
                        for role_id in roles_to_insert
                    ]
                    await db_conn.executemany(
                        "INSERT OR IGNORE INTO instance_visibility_roles (instance_id, role_id) VALUES (?, ?)",
                        params_to_insert
                    )

            logger.info(f"Successfully updated visibility for {len(instance_ids_to_update)} instances to roles: {roles_to_insert}")

        except (aiosqlite.Error, ValueError) as e:
            logger.exception(f"Failed to update visibility by tags {tags}. Error: {e}")
            
async def update_instance_visibility_roles(instance_id: int, new_role_ids: List[int], conn: Optional[aiosqlite.Connection] = None):
    """
    Overwrites the visibility roles for a specific file instance.

    Args:
        instance_id: The ID of the file instance to update.
        new_role_ids: A list of role IDs. If empty, visibility will be set to 'Everyone'.
    """
    async with AsyncDBContext(conn) as db_conn:
        async with db_conn.execute("BEGIN"):
            await execute_db_query(db_conn, "DELETE FROM instance_visibility_roles WHERE instance_id = ?", (instance_id,))
            
            roles_to_insert = list(set(new_role_ids))
            if not roles_to_insert:
                try:
                    everyone_id = get_everyone_role_id()
                    roles_to_insert.append(everyone_id)
                    logger.debug(f"Role list for instance {instance_id} was empty. Defaulting to 'Everyone' role.")
                except RuntimeError as e:
                    logger.error(f"Could not get 'Everyone' role ID during update: {e}. Instance {instance_id} will have NO visibility.")
            
            if roles_to_insert:
                params = [(instance_id, role_id) for role_id in roles_to_insert]
                await db_conn.executemany(
                    "INSERT OR IGNORE INTO instance_visibility_roles (instance_id, role_id) VALUES (?, ?)",
                    params
                )
                logger.info(f"Updated visibility for instance ID {instance_id} to roles: {roles_to_insert}")

async def add_role_to_all_instances(role_id: int, conn: Optional[aiosqlite.Connection] = None):
    """Adds a specific role to the visibility list of every file instance in the database."""
    async with AsyncDBContext(conn) as db_conn:
        query = "INSERT OR IGNORE INTO instance_visibility_roles (instance_id, role_id) SELECT id, ? FROM file_instances"
        await execute_db_query(db_conn, query, (role_id,))
    logger.info(f"Globally added role ID {role_id} to all file instances.")

async def remove_role_from_all_instances(role_id: int, conn: Optional[aiosqlite.Connection] = None):
    """Removes a specific role from the visibility list of every file instance in the database."""
    async with AsyncDBContext(conn) as db_conn:
        query = "DELETE FROM instance_visibility_roles WHERE role_id = ?"
        await execute_db_query(db_conn, query, (role_id,))
    logger.info(f"Globally removed role ID {role_id} from all file instances.")

async def manage_role_for_instances_by_tags(
    tags: List[Union[int, str]],
    role_id: int,
    tag_type: str,
    action: str, # 'add' or 'remove'
    conn: Optional[aiosqlite.Connection] = None
):
    """
    Adds or removes a single role for all file instances associated with given content tags.
    """
    if not tags:
        logger.warning(f"manage_role_for_instances_by_tags called with no tags for action '{action}'. No action taken.")
        return

    if action not in ['add', 'remove']:
        raise ValueError("Action must be either 'add' or 'remove'.")
        
    async with AsyncDBContext(conn) as db_conn:
        try:
            tables = _get_table_names(tag_type)
            junction_table = tables['file_junction']
            tag_ids = await _resolve_tag_ids(tags, tag_type, db_conn)
            if not tag_ids:
                logger.warning(f"No valid tags found for identifiers: {tags}. No instances updated.")
                return

            async with db_conn.execute("BEGIN"):
                placeholders = ', '.join('?' for _ in tag_ids)
                query_content = f"SELECT DISTINCT content_id FROM {junction_table} WHERE tag_id IN ({placeholders})"
                content_rows = await execute_db_query(db_conn, query_content, tuple(tag_ids), fetch_all=True)
                content_ids = [row['content_id'] for row in content_rows]

                if not content_ids:
                    logger.info(f"No content found with tags: {tags}. No visibility updated.")
                    return

                content_placeholders = ', '.join('?' for _ in content_ids)
                query_instances = f"SELECT id FROM file_instances WHERE content_id IN ({content_placeholders})"
                instance_rows = await execute_db_query(db_conn, query_instances, tuple(content_ids), fetch_all=True)
                instance_ids = [row['id'] for row in instance_rows]

                if not instance_ids:
                    logger.info(f"Content found for tags {tags}, but no corresponding file instances exist. No visibility updated.")
                    return

                logger.info(f"Found {len(instance_ids)} instances for role management on tags: {tags}")

                if action == 'add':
                    params_to_insert = [(instance_id, role_id) for instance_id in instance_ids]
                    await db_conn.executemany(
                        "INSERT OR IGNORE INTO instance_visibility_roles (instance_id, role_id) VALUES (?, ?)",
                        params_to_insert
                    )
                elif action == 'remove':
                    instance_placeholders = ', '.join('?' for _ in instance_ids)
                    params = (role_id,) + tuple(instance_ids)
                    await execute_db_query(
                        db_conn,
                        f"DELETE FROM instance_visibility_roles WHERE role_id = ? AND instance_id IN ({instance_placeholders})",
                        params
                    )
                
            logger.info(f"Successfully performed '{action}' for role {role_id} on {len(instance_ids)} instances matching tags: {tags}")

        except (aiosqlite.Error, ValueError) as e:
            logger.exception(f"Failed to '{action}' role by tags {tags}. Error: {e}")

async def get_cards_with_cursor(
    user_id: int,
    page_size: int,
    search: Optional[str] = None,
    scene_search: Optional[str] = None,
    filename: Optional[str] = None,
    tags: Optional[str] = None,
    exclude_tags: Optional[str] = None,
    meta_tags: Optional[str] = None,
    exclude_meta_tags: Optional[str] = None,
    file_type: Optional[List[str]] = None,
    is_encrypted: Optional[bool] = None,
    duration_min: Optional[int] = None,
    duration_max: Optional[int] = None,
    folder_id: Optional[int] = None,
    mode: str = 'expanded',
    sort_by: Literal['id', 'created_at'] = 'id',
    cursor: Optional[str] = None,
    conn: Optional[aiosqlite.Connection] = None
) -> Dict:
    """
    Performs a complex search for file instances with cursor-based pagination,
    respecting user visibility and different view modes.

    Args:
        user_id: The ID of the user performing the search.
        page_size: The number of items to return per page.
        search: General search on instance name and main content transcript.
        scene_search: Specific search within scene transcripts.
        filename: Filter by file instance name.
        tags: Comma-separated string of tags that ALL must be present.
        exclude_tags: Comma-separated string of tags that NONE must be present.
        meta_tags: Comma-separated string of meta_tags that ALL must be present.
        exclude_meta_tags: Comma-separated string of meta_tags that NONE must be present.
        file_type: A list of file types to include (e.g., ['video', 'audio']).
        is_encrypted: Filter by encryption status.
        duration_min: Minimum duration in seconds.
        duration_max: Maximum duration in seconds.
        folder_id: The ID of the folder to search within.
        mode: Search mode ('expanded', 'directory', 'recursive').
        sort_by: The field to sort by. 'id' (default) sorts by creation order,
                 'created_at' sorts by the file's original timestamp.
                 Both are sorted from recent to old.
        cursor: The pagination cursor from the previous page's response.
        conn: An optional existing database connection.

    Returns:
        A dictionary containing 'folders', 'items', and 'next_cursor'.
    """
    async with AsyncDBContext(conn) as db_conn:
        # --- Mode-dependent setup ---
        folders_list = None
        folder_ids_to_search = []
        if mode == 'directory' and folder_id is not None:
            folder_rows = await execute_db_query(
                db_conn, "SELECT id, name, path FROM folders WHERE parent_id = ? ORDER BY name COLLATE NOCASE", (folder_id,), fetch_all=True
            )
            folders_list = [dict(row) for row in folder_rows]
            folder_ids_to_search.append(folder_id)
        elif mode == 'recursive' and folder_id is not None:
            recursive_query = """
                WITH RECURSIVE descendant_folders(id) AS (
                    SELECT id FROM folders WHERE id = ?
                    UNION ALL
                    SELECT f.id FROM folders f JOIN descendant_folders df ON f.parent_id = df.id
                )
                SELECT id FROM descendant_folders;
            """
            folder_rows = await execute_db_query(db_conn, recursive_query, (folder_id,), fetch_all=True)
            if folder_rows:
                folder_ids_to_search.extend([row['id'] for row in folder_rows])
        elif folder_id is not None:
            folder_ids_to_search.append(folder_id)

        # --- Sorting and Pagination Setup ---
        if sort_by == 'created_at':
            order_by_clause = "fi.original_created_at DESC, fi.id DESC"
            cursor_columns = ('original_created_at', 'id')
            cursor_where_clause = "(fi.original_created_at, fi.id) < (?, ?)"
        elif sort_by == 'id':
            order_by_clause = "fi.id DESC"
            cursor_columns = ('id',)
            cursor_where_clause = "fi.id < ?"
        else:
            raise ValueError("Invalid sort_by value. Must be 'id' or 'created_at'.")

        # --- Common Query Building ---
        params = [user_id]
        query_parts = [
            "SELECT DISTINCT", "fi.id,", "fi.file_name as display_name,", "fc.file_type,",
            "fc.is_encrypted,", "fc.file_hash,", "fc.duration_seconds,", "fi.original_created_at",
            "FROM file_instances fi",
            "JOIN file_content fc ON fi.content_id = fc.id",
            "JOIN instance_visibility_roles ivr ON fi.id = ivr.instance_id",
            "JOIN user_roles ur ON ivr.role_id = ur.role_id",
        ]
        where_clauses = ["ur.user_id = ?"]

        if search:
            where_clauses.append("(fi.file_name LIKE ? OR fc.transcript LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        if scene_search:
            where_clauses.append("fi.content_id IN (SELECT DISTINCT content_id FROM scenes WHERE transcript LIKE ?)")
            params.append(f"%{scene_search}%")
        if filename:
            where_clauses.append("fi.file_name LIKE ?")
            params.append(f"%{filename}%")
        if file_type:
            placeholders = ', '.join('?' for _ in file_type)
            where_clauses.append(f"fc.file_type IN ({placeholders})")
            params.extend(file_type)
        if is_encrypted is not None:
            where_clauses.append("fc.is_encrypted = ?")
            params.append(int(is_encrypted))
        if duration_min is not None:
            where_clauses.append("fc.duration_seconds >= ?")
            params.append(duration_min)
        if duration_max is not None:
            where_clauses.append("fc.duration_seconds <= ?")
            params.append(duration_max)
        if folder_ids_to_search:
            placeholders = ', '.join('?' for _ in folder_ids_to_search)
            where_clauses.append(f"fi.folder_id IN ({placeholders})")
            params.extend(folder_ids_to_search)

        for tag_str, tag_type_name, should_exclude in [
            (tags, 'tags', False), (exclude_tags, 'tags', True),
            (meta_tags, 'meta_tags', False), (exclude_meta_tags, 'meta_tags', True)
        ]:
            if not tag_str: continue
            tag_list = [t.strip() for t in tag_str.split(',') if t.strip()]
            if not tag_list: continue
            tables = _get_table_names(tag_type_name)
            placeholders = ', '.join('?' for _ in tag_list)
            if not should_exclude:
                sub_query = f"""
                    SELECT content_id FROM {tables['file_junction']} j
                    JOIN {tables['main']} t ON j.tag_id = t.id
                    WHERE t.name IN ({placeholders})
                    GROUP BY content_id HAVING COUNT(DISTINCT t.id) = ?
                """
                where_clauses.append(f"fi.content_id IN ({sub_query})")
                params.extend(tag_list)
                params.append(len(tag_list))
            else:
                sub_query = f"""
                    SELECT content_id FROM {tables['file_junction']} j
                    JOIN {tables['main']} t ON j.tag_id = t.id
                    WHERE t.name IN ({placeholders})
                """
                where_clauses.append(f"fi.content_id NOT IN ({sub_query})")
                params.extend(tag_list)

        # --- Cursor Pagination ---
        if cursor:
            try:
                decoded_cursor = base64.b64decode(cursor).decode('utf-8')
                cursor_values_str = decoded_cursor.split(':')
                cursor_params = []
                if sort_by == 'created_at':
                    if len(cursor_values_str) != 2: raise ValueError("Invalid cursor format for created_at sort")
                    cursor_params.append(cursor_values_str[0])
                    cursor_params.append(int(cursor_values_str[1]))
                else: # sort_by == 'id'
                    if len(cursor_values_str) != 1: raise ValueError("Invalid cursor format for id sort")
                    cursor_params.append(int(cursor_values_str[0]))
                where_clauses.append(cursor_where_clause)
                params.extend(cursor_params)
            except (ValueError, TypeError, UnicodeDecodeError, base64.binascii.Error) as e:
                logger.warning(f"Invalid cursor received: {cursor}. Ignoring. Error: {e}")

        # --- Final Query Assembly ---
        query = " ".join(query_parts) + " WHERE " + " AND ".join(where_clauses)
        query += f" ORDER BY {order_by_clause} LIMIT ?"
        params.append(page_size + 1)

        rows = await execute_db_query(db_conn, query, tuple(params), fetch_all=True)

        items = [dict(row) for row in rows]
        next_cursor = None
        if len(items) > page_size:
            last_item = items.pop()
            cursor_parts = [str(last_item[col]) for col in cursor_columns]
            next_cursor_val = ":".join(cursor_parts)
            next_cursor = base64.b64encode(next_cursor_val.encode()).decode()

        return {"folders": folders_list, "items": items, "next_cursor": next_cursor}

async def get_duplicates(
    user_id: int,
    duplicate_type: str,
    file_type: Optional[List[str]],
    conn: Optional[aiosqlite.Connection] = None
):
    """
    Finds groups of duplicate files based on content hash.
    Respects user visibility.
    """
    hash_duplicates = []
    similarity_duplicates = []  # Placeholder for future implementation

    if duplicate_type in ['all', 'hash']:
        async with AsyncDBContext(conn) as db_conn:
            # 1. Find hashes with more than one visible instance for the user
            hash_query_parts = [
                "SELECT fc.file_hash FROM file_instances fi",
                "JOIN file_content fc ON fi.content_id = fc.id",
                "JOIN instance_visibility_roles ivr ON fi.id = ivr.instance_id",
                "JOIN user_roles ur ON ivr.role_id = ur.role_id AND ur.user_id = ?",
            ]
            params = [user_id]
            
            if file_type:
                placeholders = ', '.join('?' for _ in file_type)
                hash_query_parts.append(f"WHERE fc.file_type IN ({placeholders})")
                params.extend(file_type)

            hash_query_parts.extend(["GROUP BY fc.file_hash", "HAVING COUNT(fi.id) > 1"])
            
            duplicate_hash_rows = await execute_db_query(db_conn, " ".join(hash_query_parts), tuple(params), fetch_all=True)
            duplicate_hashes = [row['file_hash'] for row in duplicate_hash_rows]

            # 2. For each duplicate hash, get all visible instances
            if duplicate_hashes:
                for h in duplicate_hashes:
                    instance_query = """
                        SELECT DISTINCT fi.id, fi.file_name as display_name, fc.file_type, fc.is_encrypted, fc.file_hash, fc.duration_seconds, fi.original_created_at
                        FROM file_instances fi
                        JOIN file_content fc ON fi.content_id = fc.id
                        JOIN instance_visibility_roles ivr ON fi.id = ivr.instance_id
                        JOIN user_roles ur ON ivr.role_id = ur.role_id AND ur.user_id = ?
                        WHERE fc.file_hash = ? ORDER BY fi.id
                    """
                    instance_rows = await execute_db_query(db_conn, instance_query, (user_id, h), fetch_all=True)
                    if instance_rows and len(instance_rows) > 1:
                        hash_duplicates.append([dict(row) for row in instance_rows])

    # Similarity duplicates logic would go here if implemented
    
    return hash_duplicates, similarity_duplicates