import logging
from typing import List, Optional, Union
import aiosqlite

from src.db_utils import AsyncDBContext, execute_db_query
from src.roles.rbac_manager import get_everyone_role_id
from src.tag_utils import _get_table_names
from src.tags.HelperTags import _resolve_tag_ids


logger = logging.getLogger("media_repository")

async def update_visibility_for_files_by_tags(
    tags: List[Union[int, str]],
    new_role_ids: List[int],
    tag_type: str,
    conn: Optional[aiosqlite.Connection] = None
):
    """
    Finds all files associated with the given tags and overwrites their visibility roles.
    This is a bulk operation performed in a single transaction.

    Args:
        tags: A list of tag names or IDs to identify the target files.
        new_role_ids: A list of role IDs to set for the found files. If empty,
                      visibility will be set to the 'Everyone' role.
        tag_type: The type of tag, either 'tags' or 'meta_tags'.
        conn: An optional existing database connection.
    """
    if not tags:
        logger.warning("update_visibility_for_files_by_tags called with no tags. No action taken.")
        return

    async with AsyncDBContext(conn) as db_conn:
        try:
            # Get the correct junction table name for the given tag type
            tables = _get_table_names(tag_type)
            junction_table = tables['file_junction']

            # Resolve the provided tag identifiers (names or IDs) into a list of IDs
            tag_ids = await _resolve_tag_ids(tags, tag_type, db_conn)
            if not tag_ids:
                logger.warning(f"No valid tags found for identifiers: {tags}. No files updated.")
                return

            # Start a transaction for this bulk operation
            async with db_conn.execute("BEGIN"):
                # 1. Find all unique file IDs associated with these tags
                placeholders = ', '.join('?' for _ in tag_ids)
                query = f"SELECT DISTINCT file_id FROM {junction_table} WHERE tag_id IN ({placeholders})"
                
                rows = await execute_db_query(db_conn, query, tuple(tag_ids), fetch_all=True)
                file_ids_to_update = [row['file_id'] for row in rows]

                if not file_ids_to_update:
                    logger.info(f"No files found with the specified tags: {tags}. No visibility updated.")
                    return

                logger.info(f"Found {len(file_ids_to_update)} files to update visibility for tags: {tags}")

                # 2. Delete all existing visibility roles for these files
                file_placeholders = ', '.join('?' for _ in file_ids_to_update)
                await execute_db_query(
                    db_conn,
                    f"DELETE FROM file_visibility_roles WHERE file_id IN ({file_placeholders})",
                    tuple(file_ids_to_update)
                )

                # 3. Prepare and insert the new roles
                roles_to_insert = list(set(new_role_ids)) # Ensure unique

                # Default to 'Everyone' if the provided list is empty
                if not roles_to_insert:
                    try:
                        everyone_id = get_everyone_role_id()
                        roles_to_insert.append(everyone_id)
                        logger.debug(f"Role list was empty. Defaulting to 'Everyone' role for bulk update.")
                    except RuntimeError as e:
                        logger.error(f"Could not get 'Everyone' role ID during bulk update: {e}. Files will have NO visibility.")
                
                if roles_to_insert:
                    # Create a list of all (file_id, role_id) pairs to insert
                    params_to_insert = [
                        (file_id, role_id)
                        for file_id in file_ids_to_update
                        for role_id in roles_to_insert
                    ]
                    
                    await db_conn.executemany(
                        "INSERT OR IGNORE INTO file_visibility_roles (file_id, role_id) VALUES (?, ?)",
                        params_to_insert
                    )

            logger.info(f"Successfully updated visibility for {len(file_ids_to_update)} files to roles: {roles_to_insert}")

        except (aiosqlite.Error, ValueError) as e:
            logger.exception(f"Failed to update visibility by tags {tags}. Error: {e}")
            # The transaction will be rolled back automatically by AsyncDBContext
            
async def update_file_visibility_roles(file_id: int, new_role_ids: List[int], conn: Optional[aiosqlite.Connection] = None):
    """
    Overwrites the visibility roles for a specific file.

    Args:
        file_id: The ID of the file to update.
        new_role_ids: A list of role IDs. If empty, visibility will be set to 'Everyone'.
    """
    async with AsyncDBContext(conn) as db_conn:
        # Start a transaction to ensure atomicity
        async with db_conn.execute("BEGIN"):
            # 1. Delete all existing visibility roles for this file
            await execute_db_query(db_conn, "DELETE FROM file_visibility_roles WHERE file_id = ?", (file_id,))
            
            # 2. Insert the new ones
            roles_to_insert = list(set(new_role_ids)) # Ensure unique
            
            # Default to 'Everyone' if the provided list is empty
            if not roles_to_insert:
                try:
                    everyone_id = get_everyone_role_id()
                    roles_to_insert.append(everyone_id)
                    logger.debug(f"Role list for file {file_id} was empty. Defaulting to 'Everyone' role.")
                except RuntimeError as e:
                    logger.error(f"Could not get 'Everyone' role ID during update: {e}. File {file_id} will have NO visibility.")
                    # Let the transaction complete, leaving the file with no roles.
            
            if roles_to_insert:
                params = [(file_id, role_id) for role_id in roles_to_insert]
                await db_conn.executemany(
                    "INSERT OR IGNORE INTO file_visibility_roles (file_id, role_id) VALUES (?, ?)",
                    params
                )
                logger.info(f"Updated visibility for file ID {file_id} to roles: {roles_to_insert}")

async def add_role_to_all_files(role_id: int, conn: Optional[aiosqlite.Connection] = None):
    """Adds a specific role to the visibility list of every file in the database."""
    async with AsyncDBContext(conn) as db_conn:
        query = "INSERT OR IGNORE INTO file_visibility_roles (file_id, role_id) SELECT id, ? FROM files"
        await execute_db_query(db_conn, query, (role_id,))
    logger.info(f"Globally added role ID {role_id} to all files.")

async def remove_role_from_all_files(role_id: int, conn: Optional[aiosqlite.Connection] = None):
    """Removes a specific role from the visibility list of every file in the database."""
    async with AsyncDBContext(conn) as db_conn:
        
        
        query = "DELETE FROM file_visibility_roles WHERE role_id = ?"
        await execute_db_query(db_conn, query, (role_id,))
    logger.info(f"Globally removed role ID {role_id} from all files.")

async def manage_role_for_files_by_tags(
    tags: List[Union[int, str]],
    role_id: int,
    tag_type: str,
    action: str, # 'add' or 'remove'
    conn: Optional[aiosqlite.Connection] = None
):
    """
    Adds or removes a single role for all files associated with given tags.
    This is a bulk operation performed in a single transaction.
    """
    if not tags:
        logger.warning(f"manage_role_for_files_by_tags called with no tags for action '{action}'. No action taken.")
        return

    if action not in ['add', 'remove']:
        raise ValueError("Action must be either 'add' or 'remove'.")
        
    async with AsyncDBContext(conn) as db_conn:
        try:
            tables = _get_table_names(tag_type)
            junction_table = tables['file_junction']
            tag_ids = await _resolve_tag_ids(tags, tag_type, db_conn)
            if not tag_ids:
                logger.warning(f"No valid tags found for identifiers: {tags}. No files updated.")
                return

            async with db_conn.execute("BEGIN"):
                placeholders = ', '.join('?' for _ in tag_ids)
                query = f"SELECT DISTINCT file_id FROM {junction_table} WHERE tag_id IN ({placeholders})"
                rows = await execute_db_query(db_conn, query, tuple(tag_ids), fetch_all=True)
                file_ids = [row['file_id'] for row in rows]

                if not file_ids:
                    logger.info(f"No files found with tags: {tags}. No visibility updated.")
                    return

                logger.info(f"Found {len(file_ids)} files for role management on tags: {tags}")

                if action == 'add':
                    params_to_insert = [(file_id, role_id) for file_id in file_ids]
                    await db_conn.executemany(
                        "INSERT OR IGNORE INTO file_visibility_roles (file_id, role_id) VALUES (?, ?)",
                        params_to_insert
                    )
                elif action == 'remove':
                    file_placeholders = ', '.join('?' for _ in file_ids)
                    params = (role_id,) + tuple(file_ids)
                    await execute_db_query(
                        db_conn,
                        f"DELETE FROM file_visibility_roles WHERE role_id = ? AND file_id IN ({file_placeholders})",
                        params
                    )
                
            logger.info(f"Successfully performed '{action}' for role {role_id} on {len(file_ids)} files matching tags: {tags}")

        except (aiosqlite.Error, ValueError) as e:
            logger.exception(f"Failed to '{action}' role by tags {tags}. Error: {e}")