# src/media/folder_manager.py
import os
from typing import Optional, Dict
import aiosqlite
import shutil
import asyncio
import logging

from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query

logger = logging.getLogger("FolderManager")
config = get_config()

async def create_folder(name: str, parent_id: Optional[int] = None, conn: Optional[aiosqlite.Connection] = None) -> Dict:
    """
    Creates a new folder record in the database and a corresponding physical directory.

    Args:
        name: The name of the new folder.
        parent_id: The ID of the parent folder. If None, created in the first WATCH_DIR.
        conn: An optional existing database connection.

    Returns:
        A dictionary representing the newly created folder.

    Raises:
        ValueError: If input is invalid (e.g., parent not found, name conflict).
        RuntimeError: If configuration is missing.
        IOError: If physical directory creation fails.
    """
    async with AsyncDBContext(conn) as db_conn:
        base_path = ""
        if parent_id:
            parent_row = await execute_db_query(db_conn, "SELECT path FROM folders WHERE id = ?", (parent_id,), fetch_one=True)
            if not parent_row:
                raise ValueError(f"Parent folder with ID {parent_id} not found.")
            base_path = parent_row['path']
        else:
            default_create_dir = config.get('DEFAULT_CREATE_FOLDER')
            watched_dirs = [os.path.normpath(p) for p in config.get('WATCH_DIRS', [])]

            if default_create_dir:
                # Ensure the configured default is a valid, watched directory
                normalized_default = os.path.normpath(default_create_dir)
                if normalized_default in watched_dirs:
                    base_path = normalized_default
                else:
                    # This error message is important for user configuration.
                    raise RuntimeError(f"Configuration error: DEFAULT_CREATE_FOLDER ('{default_create_dir}') is not one of the configured WATCH_DIRS.")
            else:
                # Fallback to original behavior if DEFAULT_CREATE_FOLDER is not set
                if not watched_dirs:
                    raise RuntimeError("Cannot create a root folder: WATCH_DIRS is not configured and DEFAULT_CREATE_FOLDER is not set.")
                base_path = watched_dirs[0]

        # Clean the folder name to prevent path traversal issues
        clean_name = os.path.basename(name)
        if not clean_name or clean_name in ('.', '..'):
            raise ValueError("Folder name is invalid.")

        new_path = os.path.join(base_path, clean_name)
        new_path = os.path.normpath(new_path)

        # Check for existing folder in DB
        existing = await execute_db_query(db_conn, "SELECT id FROM folders WHERE path = ?", (new_path,), fetch_one=True)
        if existing:
            raise ValueError(f"A folder with path '{new_path}' already exists.")

        # Create physical directory
        try:
            os.makedirs(new_path, exist_ok=True)
            logger.info(f"Created physical directory at: {new_path}")
        except OSError as e:
            raise IOError(f"Failed to create physical directory at '{new_path}': {e}") from e

        # Insert into database
        try:
            insert_query = "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)"
            new_folder_id = await execute_db_query(
                db_conn, insert_query, (new_path, clean_name, parent_id), return_last_row_id=True
            )
            if not new_folder_id:
                raise aiosqlite.Error("Failed to get lastrowid for new folder.")

            logger.info(f"Created folder '{clean_name}' (ID: {new_folder_id}) in database.")
            # Return a dictionary that matches the FolderItem model
            return {"id": new_folder_id, "name": clean_name, "path": new_path}

        except aiosqlite.IntegrityError:
            # This is a race condition, another worker created it.
            raise ValueError(f"A folder with path '{new_path}' was created by another process.")

async def move_folder(folder_id: int, new_parent_id: Optional[int], conn: Optional[aiosqlite.Connection] = None) -> Dict:
    """
    Moves a folder and its contents to a new parent folder.
    This operation is restricted to moves within the 'DEFAULT_CREATE_FOLDER' and its subdirectories.
    It performs a physical move on the filesystem and updates all corresponding database records.

    Args:
        folder_id: The ID of the folder to move.
        new_parent_id: The ID of the new parent folder. If None, moves to the root of 'DEFAULT_CREATE_FOLDER'.
        conn: An optional existing database connection.

    Returns:
        A dictionary representing the moved folder with its new path.

    Raises:
        RuntimeError: If configuration is invalid for this operation.
        ValueError: If the move is invalid (e.g., destination conflict, moving into self).
        IOError: If the physical move fails.
    """
    # 1. Configuration validation
    if config.get('COPY_FILE_TO_FOLDER', False):
        raise RuntimeError("Folder move operation is not supported when 'COPY_FILE_TO_FOLDER' is enabled, as folders are virtual and do not contain physical files.")

    allowed_root = config.get('DEFAULT_CREATE_FOLDER')
    if not allowed_root:
        raise RuntimeError("Folder move operation requires 'DEFAULT_CREATE_FOLDER' to be configured to define the allowed destination area.")
    allowed_root = os.path.normpath(allowed_root)

    async with AsyncDBContext(conn) as db_conn:
        # 2. Fetch source and destination details
        source_folder = await execute_db_query(db_conn, "SELECT path, name FROM folders WHERE id = ?", (folder_id,), fetch_one=True)
        if not source_folder:
            raise ValueError(f"Source folder with ID {folder_id} not found.")
        source_path = os.path.normpath(source_folder['path'])
        source_name = source_folder['name']

        watched_dirs = [os.path.normpath(p) for p in config.get('WATCH_DIRS', [])]
        if source_path in watched_dirs:
            raise ValueError("Cannot move a top-level watch directory.")

        dest_parent_path = ""
        if new_parent_id is None:
            dest_parent_path = allowed_root
        else:
            if new_parent_id == folder_id:
                raise ValueError("Cannot move a folder into itself.")
            dest_parent_folder = await execute_db_query(db_conn, "SELECT path FROM folders WHERE id = ?", (new_parent_id,), fetch_one=True)
            if not dest_parent_folder:
                raise ValueError(f"Destination parent folder with ID {new_parent_id} not found.")
            dest_parent_path = os.path.normpath(dest_parent_folder['path'])

        # 3. Business logic and safety checks
        if not dest_parent_path.startswith(allowed_root):
            raise ValueError("Folders can only be moved into the configured 'DEFAULT_CREATE_FOLDER' or one of its subdirectories.")

        if dest_parent_path.startswith(source_path):
            raise ValueError("Cannot move a folder into one of its own subdirectories.")

        new_path = os.path.normpath(os.path.join(dest_parent_path, source_name))

        if os.path.exists(new_path):
            raise ValueError(f"A file or folder with the name '{source_name}' already exists at the destination.")

        # 4. Execute the move
        try:
            logger.info(f"Moving physical folder from '{source_path}' to '{new_path}'")
            await asyncio.to_thread(shutil.move, source_path, new_path)

            # --- Database Updates ---
            await execute_db_query(db_conn, "UPDATE folders SET parent_id = ?, path = ? WHERE id = ?", (new_parent_id, new_path, folder_id))
            like_prefix = source_path + os.sep + '%'
            substr_len = len(source_path) + 1
            await execute_db_query(db_conn, "UPDATE folders SET path = ? || SUBSTR(path, ?) WHERE path LIKE ?", (new_path, substr_len, like_prefix))
            await execute_db_query(db_conn, "UPDATE file_instances SET stored_path = ? || SUBSTR(stored_path, ?) WHERE stored_path LIKE ?", (new_path, substr_len, like_prefix))

            logger.info(f"Successfully moved folder ID {folder_id} and updated all associated database records.")
            return {"id": folder_id, "name": source_name, "path": new_path}

        except Exception as e:
            logger.error(f"An error occurred during folder move: {e}. Attempting to roll back physical move.", exc_info=True)
            if os.path.exists(new_path) and not os.path.exists(source_path):
                try:
                    await asyncio.to_thread(shutil.move, new_path, source_path)
                    logger.info("Successfully rolled back physical folder move.")
                except Exception as rollback_e:
                    logger.critical(f"CRITICAL: FAILED TO ROLL BACK physical folder move from '{new_path}' to '{source_path}'. Filesystem and DB are now out of sync. Error: {rollback_e}")
            if isinstance(e, (ValueError, RuntimeError)): raise e
            raise IOError(f"A filesystem or database error occurred during the move operation: {e}") from e

async def delete_folder_if_empty(folder_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """
    Deletes a folder if it is empty, both from the database and the filesystem.

    An empty folder is defined as having no sub-folders and no file instances in the
    database, and no files in the physical directory.

    Args:
        folder_id: The ID of the folder to delete.
        conn: An optional existing database connection.

    Returns:
        True if the folder was successfully deleted.

    Raises:
        ValueError: If the folder is not found, not empty, or is a protected watch directory.
        IOError: If the physical directory cannot be removed.
    """
    async with AsyncDBContext(conn) as db_conn:
        # 1. Fetch folder details and perform initial checks
        folder_to_delete = await execute_db_query(db_conn, "SELECT path FROM folders WHERE id = ?", (folder_id,), fetch_one=True)
        if not folder_to_delete:
            raise ValueError(f"Folder with ID {folder_id} not found.")
        
        folder_path = os.path.normpath(folder_to_delete['path'])
        
        watched_dirs = [os.path.normpath(p) for p in config.get('WATCH_DIRS', [])]
        if folder_path in watched_dirs:
            raise ValueError("Cannot delete a top-level watch directory.")

        # 2. Check if the folder is empty in the database
        if await execute_db_query(db_conn, "SELECT 1 FROM folders WHERE parent_id = ? LIMIT 1", (folder_id,), fetch_one=True):
            raise ValueError("Folder is not empty: contains sub-folders.")

        if await execute_db_query(db_conn, "SELECT 1 FROM file_instances WHERE folder_id = ? LIMIT 1", (folder_id,), fetch_one=True):
            raise ValueError("Folder is not empty: contains files.")

        # 3. Check and delete the physical directory
        if os.path.exists(folder_path):
            if any(f for f in os.listdir(folder_path) if f not in ['.DS_Store', 'Thumbs.db']):
                raise ValueError("Physical directory is not empty.")
            try:
                await asyncio.to_thread(os.rmdir, folder_path)
                logger.info(f"Removed empty physical directory: {folder_path}")
            except OSError as e:
                raise IOError(f"Failed to remove physical directory '{folder_path}': {e}") from e

        # 4. Delete from database
        return await execute_db_query(db_conn, "DELETE FROM folders WHERE id = ?", (folder_id,)) > 0