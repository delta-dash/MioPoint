# HelperTags.py
import aiosqlite
from typing import Any, List, Union, Optional

from ConfigMedia import get_config
import logging
from src.db_utils import AsyncDBContext, execute_db_query
from src.tag_utils import _get_table_names

logger = logging.getLogger("HelperTags")
logger.setLevel(logging.DEBUG)
config = get_config()

async def setup_tags_database_with_loop_async(conn: Optional[aiosqlite.Connection] = None):
    """
    Asynchronously creates or updates the database schema for multiple tag types.

    Can be used standalone or as part of a larger transaction by passing an
    existing connection.

    Args:
        conn: (Optional) An active aiosqlite database connection.
    """
    async with AsyncDBContext(conn) as db_conn:
        try:
            # check_database_requirements is likely synchronous.
            # For a fully async system, this might need an async version.
            # Assuming it can be run within a sync-to-async executor if needed,
            # but for schema checks, it's often okay.
            # check_database_requirements({'users', 'files'}, cursor) # TODO: Adapt check_database_requirements to be async

            # --- Core Logic ---
            await db_conn.execute("PRAGMA foreign_keys = ON;")
            
            tag_types = ["tags", "meta_tags"]

            for tag_type in tag_types:
                logger.info(f"--- Setting up schema for '{tag_type}' ---")
                tables = _get_table_names(tag_type)

                await db_conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {tables['main']} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    canonical_tag_id INTEGER,
                    is_protection_tag INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (canonical_tag_id) REFERENCES {tables['main']} (id) ON DELETE SET NULL
                );""")
                await db_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tables['main']}_canonical_id ON {tables['main']} (canonical_tag_id);")

                await db_conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {tables['relationships']} (
                    parent_tag_id INTEGER NOT NULL,
                    child_tag_id INTEGER NOT NULL,
                    FOREIGN KEY (parent_tag_id) REFERENCES {tables['main']} (id) ON DELETE CASCADE,
                    FOREIGN KEY (child_tag_id) REFERENCES {tables['main']} (id) ON DELETE CASCADE,
                    PRIMARY KEY (parent_tag_id, child_tag_id)
                );""")
                await db_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tables['relationships']}_child ON {tables['relationships']} (child_tag_id);")

                await db_conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {tables['file_junction']} (
                    content_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    FOREIGN KEY (content_id) REFERENCES file_content (id) ON DELETE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES {tables['main']} (id) ON DELETE CASCADE,
                    PRIMARY KEY (content_id, tag_id)
                );""")
                await db_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tables['file_junction']}_tag_id ON {tables['file_junction']} (tag_id);")

                await db_conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {tables['scene_junction']} (
                    scene_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    FOREIGN KEY (scene_id) REFERENCES scenes (id) ON DELETE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES {tables['main']} (id) ON DELETE CASCADE,
                    PRIMARY KEY (scene_id, tag_id)
                );""")
                await db_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tables['scene_junction']}_tag_id ON {tables['scene_junction']} (tag_id);")
            
            logger.info("Database schema is fully verified and up-to-date.")
        except aiosqlite.Error as e:
            logger.exception(f"Database error during schema setup: {e}")
            raise # Re-raise to be handled by AsyncDBContext rollback

async def verify_schema(conn: Optional[aiosqlite.Connection] = None):
    """
    Connects to the DB and lists all created tables to verify the result.

    Args:
        conn: (Optional) An active aiosqlite database connection.
    """
    print("\n--- Verifying Created Tables ---")
    async with AsyncDBContext(conn) as db_conn:
        try:
            tables = await execute_db_query(db_conn, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", fetch_all=True)
            if tables:
                for table in tables:
                    print(f"  - Found table: {table['name']}")
            else:
                print("No tables found.")
        except aiosqlite.Error as e:
            logger.exception(f"Database error during schema verification: {e}")

async def _resolve_tag_ids(identifiers: List[Union[int, str]], tag_type: str, conn: aiosqlite.Connection) -> List[int]:
    """Helper to convert a list of names or IDs into a list of just IDs."""
    if not identifiers:
        return []
    
    tables = _get_table_names(tag_type)
    resolved_ids = []
    names_to_resolve = []
    
    for identifier in identifiers:
        if isinstance(identifier, int):
            resolved_ids.append(identifier)
        else:
            names_to_resolve.append(str(identifier))
            
    if names_to_resolve:
        placeholders = ', '.join('?' for _ in names_to_resolve)
        sql = f"SELECT id FROM {tables['main']} WHERE name IN ({placeholders})"
        found_rows = await execute_db_query(conn, sql, tuple(names_to_resolve), fetch_all=True)
        found_ids = [row['id'] for row in found_rows]
        resolved_ids.extend(found_ids)
        
        if len(found_ids) != len(names_to_resolve):
            logger.warning("Some tag names provided could not be found and were ignored.")

    return list(set(resolved_ids))

async def create_tag(
    tag_name: str,
    tag_type: str,
    conn: Optional[aiosqlite.Connection] = None,
    parents: Optional[List[Union[int, str]]] = None,
    alias_of: Optional[Union[int, str]] = None,
    is_protection_tag: bool = False
) -> Optional[int]:
    """
    Asynchronously creates a new tag or meta_tag. If `alias_of` is a name that
    doesn't exist, a new canonical tag with that name will be created.

    Returns:
        The ID of the newly created tag, or None if creation failed.
    """
    async with AsyncDBContext(conn) as db_conn:
        try:
            tables = _get_table_names(tag_type)

            if alias_of is not None and parents:
                raise ValueError("Cannot assign parents to a tag that is also an alias.")

            existing_tag = await execute_db_query(db_conn, f"SELECT id FROM {tables['main']} WHERE name = ?", (tag_name,), fetch_one=True)
            if existing_tag:
                raise ValueError(f"A tag named '{tag_name}' already exists in '{tables['main']}'.")

            canonical_id = None
            if alias_of is not None:
                # Use the new helper to resolve or create the canonical tag
                canonical_id = await _resolve_or_create_tag_id(alias_of, tag_type, db_conn)
                if not canonical_id:
                    raise ValueError(f"Failed to resolve or create canonical tag '{alias_of}'.")

            sql = f"INSERT INTO {tables['main']} (name, canonical_tag_id, is_protection_tag) VALUES (?, ?, ?)"
            params = (tag_name, canonical_id, int(is_protection_tag))
            new_tag_id = await execute_db_query(db_conn, sql, params, return_last_row_id=True)
            logger.info(f"Created tag '{tag_name}' (ID: {new_tag_id}) in '{tables['main']}'.")

            if parents and not alias_of:
                parent_ids = await _resolve_tag_ids(parents, tag_type, db_conn)
                if parent_ids:
                    relationship_data = [(parent_id, new_tag_id) for parent_id in parent_ids]
                    await db_conn.executemany(
                        f"INSERT OR IGNORE INTO {tables['relationships']} (parent_tag_id, child_tag_id) VALUES (?, ?)",
                        relationship_data
                    )
                    logger.debug(f"Added parents {parent_ids} to new tag ID {new_tag_id}.")
            
            return new_tag_id

        except (aiosqlite.Error, ValueError) as e:
            logger.warning(f"Operation failed in create_tag for '{tag_name}': {e}")
            return None

async def _resolve_or_create_tag_id(
    identifier: Union[int, str],
    tag_type: str,
    conn: aiosqlite.Connection
) -> Optional[int]:
    """
    Resolves a tag identifier to an ID. If the identifier is a name that
    doesn't exist, it creates a new, simple canonical tag with that name.

    Args:
        identifier: The tag's ID or name.
        tag_type: The type of tag ('tags' or 'meta_tags').
        conn: An active aiosqlite database connection.

    Returns:
        The tag's ID, or None if creation failed.
    """
    if isinstance(identifier, int):
        # We assume integer IDs must already exist.
        return identifier

    tables = _get_table_names(tag_type)
    name = str(identifier)

    # 1. Try to find the existing tag by name
    existing_tag = await execute_db_query(
        conn, f"SELECT id FROM {tables['main']} WHERE name = ?", (name,), fetch_one=True
    )
    if existing_tag:
        return existing_tag['id']

    # 2. If not found, create it as a new canonical tag
    logger.info(f"Canonical tag '{name}' not found. Creating it now...")
    try:
        # Create a basic, non-aliased tag
        sql = f"INSERT INTO {tables['main']} (name, canonical_tag_id) VALUES (?, NULL)"
        new_tag_id = await execute_db_query(conn, sql, (name,), return_last_row_id=True)
        if new_tag_id is None:
            raise aiosqlite.Error("Failed to get last row ID for new canonical tag.")
            
        logger.info(f"Created new canonical tag '{name}' (ID: {new_tag_id}).")
        return new_tag_id
    except aiosqlite.IntegrityError:
         # This could happen in a race condition, so we re-fetch the ID.
        logger.warning(f"Tag '{name}' was created by another process. Re-fetching ID.")
        refetched_tag = await execute_db_query(conn, f"SELECT id FROM {tables['main']} WHERE name = ?", (name,), fetch_one=True)
        return refetched_tag['id'] if refetched_tag else None
    except aiosqlite.Error as e:
        logger.error(f"Failed to create new canonical tag '{name}': {e}")
        return None
    
async def edit_tag(
    tag_id: int,
    tag_type: str,
    conn: aiosqlite.Connection,
    **updates: Any
) -> bool:
    """
    Asynchronously edits an existing tag's properties.
    If `alias_of` is a name that doesn't exist, a new canonical tag will be created.
    This version uses an "overwrite" strategy.

    Returns:
        True if the edit was successful, False otherwise.
    """
    try:
        tables = _get_table_names(tag_type)

        tag_exists = await execute_db_query(conn, f"SELECT 1 FROM {tables['main']} WHERE id = ?", (tag_id,), fetch_one=True)
        if not tag_exists:
            logger.warning(f"Error: Tag with ID {tag_id} does not exist in '{tables['main']}'.")
            return False

        # Handle alias update first, as it affects parent relationships
        if 'alias_of' in updates:
            new_alias_target = updates['alias_of']

            if new_alias_target is None:
                await execute_db_query(conn, f"UPDATE {tables['main']} SET canonical_tag_id = NULL WHERE id = ?", (tag_id,))
                logger.debug(f"Tag ID {tag_id} is no longer an alias.")
            else:
                # Use the new helper to resolve or create the canonical tag
                canonical_id = await _resolve_or_create_tag_id(new_alias_target, tag_type, conn)
                if not canonical_id:
                    logger.warning(f"Error: Failed to resolve or create canonical tag '{new_alias_target}'.")
                    return False
                
                if canonical_id == tag_id:
                    logger.warning("Error: A tag cannot be an alias of itself.")
                    return False

                is_canonical_for_others = await execute_db_query(conn, f"SELECT 1 FROM {tables['main']} WHERE canonical_tag_id = ?", (tag_id,), fetch_one=True)
                if is_canonical_for_others:
                    logger.warning(f"Error: Tag ID {tag_id} is a canonical tag for others. It cannot be made into an alias.")
                    return False

                await execute_db_query(conn, f"UPDATE {tables['main']} SET canonical_tag_id = ? WHERE id = ?", (canonical_id, tag_id))
                await execute_db_query(conn, f"DELETE FROM {tables['relationships']} WHERE child_tag_id = ?", (tag_id,))
                logger.debug(f"Tag ID {tag_id} is now an alias of {canonical_id}. Its parent relationships were cleared.")

        # ... (the rest of the function remains the same as the previous answer) ...
        # Handle name change
        if 'name' in updates:
            new_name = updates['name']
            is_name_taken = await execute_db_query(conn, f"SELECT 1 FROM {tables['main']} WHERE name = ? AND id != ?", (new_name, tag_id), fetch_one=True)
            if is_name_taken:
                logger.warning(f"Error: The name '{new_name}' is already in use.")
                return False
            await execute_db_query(conn, f"UPDATE {tables['main']} SET name = ? WHERE id = ?", (new_name, tag_id))
            logger.debug(f"Renamed tag ID {tag_id} to '{new_name}'.")

        # Handle is_protection_tag update
        if 'is_protection_tag' in updates and updates['is_protection_tag'] is not None:
            is_protection = int(updates['is_protection_tag'])
            await execute_db_query(conn, f"UPDATE {tables['main']} SET is_protection_tag = ? WHERE id = ?", (is_protection, tag_id))
            logger.debug(f"Updated is_protection_tag for tag ID {tag_id} to {is_protection}.")


        # Handle parent overwrite
        if 'parents' in updates:
            current_tag_info = await execute_db_query(conn, f"SELECT canonical_tag_id FROM {tables['main']} WHERE id = ?", (tag_id,), fetch_one=True)
            if current_tag_info and current_tag_info['canonical_tag_id'] is not None:
                logger.warning(f"Cannot set parents for tag ID {tag_id} because it is an alias.")
                return False

            new_parents = updates['parents'] or []
            await execute_db_query(conn, f"DELETE FROM {tables['relationships']} WHERE child_tag_id = ?", (tag_id,))
            logger.debug(f"Cleared existing parents for tag ID {tag_id}.")

            if new_parents:
                parent_ids_to_add = await _resolve_tag_ids(new_parents, tag_type, conn)
                valid_parent_ids = [pid for pid in parent_ids_to_add if pid != tag_id]
                if valid_parent_ids:
                    relationship_data = [(parent_id, tag_id) for parent_id in valid_parent_ids]
                    await conn.executemany(
                        f"INSERT OR IGNORE INTO {tables['relationships']} (parent_tag_id, child_tag_id) VALUES (?, ?)",
                        relationship_data
                    )
                    logger.debug(f"Set new parents {valid_parent_ids} for tag ID {tag_id}.")

    except (aiosqlite.Error, ValueError) as e:
        logger.warning(f"Database error during tag edit: {e}")
        return False

    return True


async def remove_tag(
    tag_identifier: Union[int, str],
    tag_type: str,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously removes a tag or meta_tag from the database.
    """
    async with AsyncDBContext(conn) as db_conn:
        try:
            tables = _get_table_names(tag_type)
            
            column = 'id' if isinstance(tag_identifier, int) else 'name'
            row = await execute_db_query(db_conn, f"SELECT id FROM {tables['main']} WHERE {column} = ?", (tag_identifier,), fetch_one=True)
            
            if not row:
                logger.warning(f"Tag '{tag_identifier}' not found in '{tables['main']}'.")
                return False
            
            tag_id_to_delete = row['id']
            rows_affected = await execute_db_query(db_conn, f"DELETE FROM {tables['main']} WHERE id = ?", (tag_id_to_delete,))
            
            if rows_affected > 0:
                logger.info(f"Successfully deleted tag ID {tag_id_to_delete} ('{tag_identifier}') from '{tables['main']}'.")
                return True
            else:
                raise RuntimeError(f"Tag {tag_id_to_delete} found but not deleted.")

        except (aiosqlite.Error, ValueError, RuntimeError) as e:
            logger.warning(f"Operation failed in remove_tag for '{tag_identifier}': {e}")
            return False

async def assign_tags_to_file(
    instance_id: int,
    tags: List[Union[int, str]],
    tag_type: str,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously assigns one or more tags to a specific file's content.
    """
    if not tags:
        return True

    async with AsyncDBContext(conn) as db_conn:
        try:
            # Get content_id from instance_id
            content_row = await execute_db_query(db_conn, "SELECT content_id FROM file_instances WHERE id = ?", (instance_id,), fetch_one=True)
            if not content_row:
                logger.warning(f"No instance found with ID {instance_id}. Cannot assign tags.")
                return False
            content_id = content_row['content_id']

            tables = _get_table_names(tag_type)
            
            tag_ids_to_assign = await _resolve_tag_ids(tags, tag_type, db_conn)
            if not tag_ids_to_assign:
                logger.warning(f"No valid tags found from input {tags} to assign for instance ID {instance_id}.")
                return False

            assignments = [(content_id, tag_id) for tag_id in tag_ids_to_assign]
            sql = f"INSERT OR IGNORE INTO {tables['file_junction']} (content_id, tag_id) VALUES (?, ?)"
            await db_conn.executemany(sql, assignments)
            logger.debug(f"Assigned tags {tag_ids_to_assign} to content ID {content_id} (from instance ID {instance_id}) in '{tables['file_junction']}'.")
            return True

        except (aiosqlite.Error, ValueError) as e:
            logger.warning(f"Operation failed in assign_tags_to_file for instance {instance_id}: {e}")
            return False


async def unassign_tags_from_file(
    instance_id: int,
    tags: List[Union[int, str]],
    tag_type: str,
    conn: Optional[aiosqlite.Connection] = None
) -> bool:
    """
    Asynchronously unassigns one or more tags from a specific file's content.
    """
    if not tags:
        return True

    async with AsyncDBContext(conn) as db_conn:
        try:
            # Get content_id from instance_id
            content_row = await execute_db_query(db_conn, "SELECT content_id FROM file_instances WHERE id = ?", (instance_id,), fetch_one=True)
            if not content_row:
                logger.warning(f"No instance found with ID {instance_id}. Cannot unassign tags.")
                return False
            content_id = content_row['content_id']

            tables = _get_table_names(tag_type)
            
            tag_ids_to_remove = await _resolve_tag_ids(tags, tag_type, db_conn)
            if not tag_ids_to_remove:
                return True

            placeholders = ', '.join('?' for _ in tag_ids_to_remove)
            sql = f"DELETE FROM {tables['file_junction']} WHERE content_id = ? AND tag_id IN ({placeholders})"
            params = (content_id, *tag_ids_to_remove)
            
            await execute_db_query(db_conn, sql, params)
            logger.debug(f"Unassigned tags {tag_ids_to_remove} from content ID {content_id} (from instance ID {instance_id}) in '{tables['file_junction']}'.")
            return True

        except (aiosqlite.Error, ValueError) as e:
            logger.warning(f"Operation failed in unassign_tags_from_file for instance {instance_id}: {e}")
            return False

async def get_visible_tags_for_user(user_id: int, tag_type: str, conn: Optional[aiosqlite.Connection] = None) -> list[dict]:
    """
    Asynchronously fetches all tags of a given type.
    Note: The permission system has been removed, so this function returns all tags.
    The user_id parameter is ignored.
    """
    async with AsyncDBContext(conn) as db_conn:
        try:
            tables = _get_table_names(tag_type)
            main_table = tables['main']

            query = f"SELECT t.id, t.name, t.canonical_tag_id FROM {main_table} t"
            rows = await execute_db_query(db_conn, query, fetch_all=True)
            return [dict(row) for row in rows]

        except (aiosqlite.Error, ValueError) as e:
            logger.exception(f"Database error getting visible tags: {e}")
            return []

async def search_files_by_tags(user_id: int, tag_names: list[str], conn: Optional[aiosqlite.Connection] = None) -> list:
    """
    A simplified, async search function.
    """
    async with AsyncDBContext(conn) as db_conn:
        try:
            if not tag_names:
                return []
                
            logger.info(f"Searching for files with tags: {tag_names}")
            # ... database query to find files would go here, using db_conn ...
            return []

        except aiosqlite.Error as e:
            logger.exception(f"Database error during file search for user {user_id}: {e}")
            return []

async def get_user_accessible_data(user_id: int, conn: Optional[aiosqlite.Connection] = None) -> dict:
    """
    Asynchronously fetches all tags and roles that are accessible to a given user.
    """
    all_data = {'tags': [], 'roles': []}
    async with AsyncDBContext(conn) as db_conn:
        try:
            # This data is typically for UI population, so we get the main 'tags'
            all_data['tags'] = await get_visible_tags_for_user(user_id, "tags", conn=db_conn)

            role_rows = await execute_db_query(db_conn, "SELECT id, name, rank FROM roles ORDER BY rank, name;", fetch_all=True)
            all_data['roles'] = [dict(row) for row in role_rows]
            
            return all_data
            
        except aiosqlite.Error as e:
            logger.exception(f"Database error in get_user_accessible_data for user {user_id}: {e}")
            return {}



async def get_tag_data(
    tag_type: str, 
    conn: aiosqlite.Connection, 
    tag_id: Optional[int] = None, 
    tag_name: Optional[str] = None
) -> Optional[dict]:
    """
    Asynchronously retrieves comprehensive data for a single tag.
    This function expects to be part of a transaction managed by the caller.

    Exactly one of `tag_id` or `tag_name` must be provided.
    """
    if (tag_id is None and tag_name is None) or \
       (tag_id is not None and tag_name is not None):
        raise ValueError("You must provide exactly one of 'tag_id' or 'tag_name'.")
        
    try:
        tables = _get_table_names(tag_type)
    except ValueError as e:
        logger.info(f"Error: {e}")
        return None

    if tag_id is not None:
        column_to_query = "id"
        identifier_value = tag_id
    else:
        column_to_query = "name"
        identifier_value = tag_name

    query = f"SELECT id, name, canonical_tag_id, is_protection_tag FROM {tables['main']} WHERE {column_to_query} = ?"
    
    main_tag_row = await execute_db_query(conn, query, (identifier_value,), fetch_one=True)
    if not main_tag_row:
        logger.debug(f"Tag with {column_to_query} '{identifier_value}' not found in '{tables['main']}'.")
        return None

    result = {
        "id": main_tag_row['id'], "name": main_tag_row['name'], "type": tag_type, "is_protection_tag": bool(main_tag_row['is_protection_tag']),
        "is_alias": main_tag_row['canonical_tag_id'] is not None,
        "canonical_tag": None, "aliases": [], "parents": [], "children": []
    }
    found_tag_id = result['id']

    if main_tag_row['canonical_tag_id']:
        canonical_row = await execute_db_query(conn, f"SELECT id, name FROM {tables['main']} WHERE id = ?", (main_tag_row['canonical_tag_id'],), fetch_one=True)
        if canonical_row:
            result["canonical_tag"] = {"id": canonical_row['id'], "name": canonical_row['name']}

    alias_rows = await execute_db_query(conn, f"SELECT id, name FROM {tables['main']} WHERE canonical_tag_id = ?", (found_tag_id,), fetch_all=True)
    result["aliases"] = [{"id": row['id'], "name": row['name']} for row in alias_rows]

    parent_sql = f"SELECT p.id, p.name FROM {tables['main']} p JOIN {tables['relationships']} r ON p.id = r.parent_tag_id WHERE r.child_tag_id = ?"
    parent_rows = await execute_db_query(conn, parent_sql, (found_tag_id,), fetch_all=True)
    result["parents"] = [{"id": row['id'], "name": row['name']} for row in parent_rows]

    child_sql = f"SELECT c.id, c.name FROM {tables['main']} c JOIN {tables['relationships']} r ON c.id = r.child_tag_id WHERE r.parent_tag_id = ?"
    child_rows = await execute_db_query(conn, child_sql, (found_tag_id,), fetch_all=True)
    result["children"] = [{"id": row['id'], "name": row['name']} for row in child_rows]
    
    return result

# The initial call to setup_database_with_loop() should be done
# from an async context, for example, in your application's startup event.
# import asyncio
# if __name__ == "__main__":
#     asyncio.run(setup_database_with_loop())