# src/media/MediaDB.py
import logging
import asyncio
import os
from typing import List, Optional
from datetime import datetime
import json
import aiosqlite

from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query
from src.log.HelperLog import log_event_async
from src.media.schemas import MediaData
from src.roles.rbac_manager import get_everyone_role_id
from src.tag_utils import _get_table_names

config = get_config()
logger = logging.getLogger(__name__)

class DatabaseService:
    """Handles all database write operations for media ingestion using async methods."""

    async def _get_or_create_folder_id(self, conn: aiosqlite.Connection, folder_path: str) -> Optional[int]:
        """
        Gets the ID of a folder, creating it and its parents recursively if they don't exist.
        Returns None for folders that are top-level watch directories, stopping recursion there.
        """
        folder_path = os.path.normpath(folder_path)

        # Get watched directories and normalize them.
        watched_dirs = [os.path.normpath(p) for p in config.get('WATCH_DIRS', [])]
        # If the current path is a watched directory, it has no parent in our DB.
        if folder_path in watched_dirs:
            return None

        # Optimistic check first, as most folders will exist after the first few files.
        cursor = await conn.execute("SELECT id FROM folders WHERE path = ?", (folder_path,))
        row = await cursor.fetchone()
        if row:
            return row[0]

        # If not found, attempt to create it, handling the race condition.
        try:
            parent_path = os.path.dirname(folder_path)
            parent_id = None
            # Stop recursion when we reach the root (e.g., '/' or 'C:\').
            if parent_path != folder_path:
                parent_id = await self._get_or_create_folder_id(conn, parent_path)

            folder_name = os.path.basename(folder_path) or folder_path # Handle root case like 'C:\'
            insert_query = "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)"
            return await execute_db_query(conn, insert_query, (folder_path, folder_name, parent_id), return_last_row_id=True)
        except aiosqlite.IntegrityError:
            # This means another worker created the folder while we were working.
            # The folder is guaranteed to exist now, so we can just select it.
            cursor = await conn.execute("SELECT id FROM folders WHERE path = ?", (folder_path,))
            row = await cursor.fetchone()
            if row: return row[0]
            raise RuntimeError(f"IntegrityError on folder insert, but could not find existing folder for path {folder_path}")

    async def add_media(self, data: MediaData, conn: Optional[aiosqlite.Connection] = None) -> int:
        """
        Adds media to the database using a content/instance model.
        Returns the ID of the file instance.
        """
        async with AsyncDBContext(conn) as db_conn:
            try:
                # First, check if an instance at this exact path already exists to avoid reprocessing.
                instance_exists = await execute_db_query(
                    db_conn, "SELECT id FROM file_instances WHERE stored_path = ?", (data.stored_path,), fetch_one=True
                )
                if instance_exists:
                    logger.info(f"File instance at path '{data.stored_path}' already exists in DB with ID {instance_exists['id']}. Skipping.")
                    return instance_exists['id']

                # Step 1: Get or create the content record. This handles all content-specific data.
                content_id = await self._get_or_create_content_record(db_conn, data)
                
                # Step 2: Create the file instance record linking to the content.
                folder_path = os.path.dirname(data.sanitized_content_path)
                folder_id = await self._get_or_create_folder_id(db_conn, folder_path)
                data.folder_id = folder_id

                instance_id = await self._insert_file_instance(db_conn, data, content_id)
                
                # Step 3: Add visibility roles for this SPECIFIC INSTANCE.
                await self._insert_visibility_roles(db_conn, instance_id, data.visibility_roles)

                # Step 4: Log the ingestion of the new instance.
                await log_event_async(
                    conn=db_conn, event_type='file_ingest', actor_type='system',
                    actor_name=data.ingest_source, target_type='file_instance',
                    target_id=instance_id, details={'file_name': data.file_name, 'content_id': content_id}
                )
                
                logger.info(f"File instance '{data.file_name}' (ID: {instance_id}) successfully ingested, linked to content (ID: {content_id}).")
                return instance_id
            except Exception as e:
                logger.exception(f"Database transaction failed for {data.file_name}. Error: {e}")
                raise # The AsyncDBContext will handle rollback

    async def _get_or_create_content_record(self, conn: aiosqlite.Connection, data: MediaData) -> int:
        """
        Finds content by hash. If it doesn't exist, creates it along with its
        associated content-level data (embeddings, scenes, tags).
        """
        try:
            # --- Optimistic Insert: Attempt to create the content record first. ---
            logger.info(f"New content detected for hash '{data.file_hash}'. Attempting to create new content record.")
            query = """
                INSERT INTO file_content (file_hash, file_type, extension, phash, metadata, transcript,
                                          duration_seconds, size_in_bytes, is_encrypted, is_webready)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (data.file_hash, data.file_type, data.extension, data.phash, json.dumps(data.metadata),
                      data.transcript, data.duration_seconds, data.size_in_bytes, int(data.is_encrypted),
                      int(data.is_webready))
            
            content_id = await execute_db_query(conn, query, params, return_last_row_id=True)

            # --- If insert succeeds, this is new content, so insert all related data. ---
            if data.clip_embedding:
                await self._insert_embedding(conn, data.clip_embedding, data.clip_model_name, content_id=content_id)

            if data.scenes_data:
                await self._insert_scenes(conn, content_id, data.scenes_data, data.clip_model_name)
            
            if data.tags_list:
                await self._insert_tags_for_entity(conn, data.tags_list, tag_type='tags', content_id=content_id)
            
            if data.model_name:
                meta_tag_name = f"Auto Tagged By: {data.model_name}"
                await self._insert_tags_for_entity(conn, [meta_tag_name], tag_type='meta_tags', content_id=content_id)
                
            return content_id

        except aiosqlite.IntegrityError:
            # --- This block runs if the INSERT fails due to a UNIQUE constraint on file_hash. ---
            logger.info(f"Content for hash '{data.file_hash}' was created by another worker. Fetching existing content ID.")
            content_row = await execute_db_query(conn, "SELECT id FROM file_content WHERE file_hash = ?", (data.file_hash,), fetch_one=True)
            if content_row:
                return content_row['id']
            # This state is highly unlikely but indicates a serious problem if it occurs.
            raise RuntimeError(f"IntegrityError on content insert, but could not find existing content for hash {data.file_hash}")

    async def _insert_file_instance(self, conn: aiosqlite.Connection, data: MediaData, content_id: int) -> int:
        """Inserts a record for a specific file instance on disk."""
        query = """
            INSERT INTO file_instances (content_id, stored_path, file_name,
                                        ingest_source, original_created_at, original_modified_at,
                                        uploader_id, folder_id, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (content_id, data.stored_path, data.file_name,
                  data.ingest_source, data.original_created_at,
                  data.original_modified_at, data.uploader_id, data.folder_id,
                  datetime.now().isoformat())
        
        instance_id = await execute_db_query(conn, query, params, return_last_row_id=True)
        return instance_id

    async def _insert_visibility_roles(self, conn: aiosqlite.Connection, instance_id: int, role_ids: List[int]):
        """Inserts visibility role relationships for a file instance."""
        if not role_ids:
            try:
                everyone_id = get_everyone_role_id()
                roles_to_insert = [everyone_id]
                logger.debug(f"No visibility roles specified for instance ID {instance_id}. Defaulting to 'Everyone' (Role ID: {everyone_id}).")
            except RuntimeError as e:
                logger.error(f"Could not get 'Everyone' role ID: {e}. Instance ID {instance_id} will have NO visibility roles set.")
                return
        else:
            roles_to_insert = list(set(role_ids))

        if roles_to_insert:
            query = "INSERT OR IGNORE INTO instance_visibility_roles (instance_id, role_id) VALUES (?, ?)"
            params = [(instance_id, role_id) for role_id in roles_to_insert]
            await conn.executemany(query, params)

    async def _insert_scenes(self, conn: aiosqlite.Connection, content_id: int, scenes_data: List[dict], clip_model_name: str):
        for scene in scenes_data:
            query = "INSERT INTO scenes (content_id, start_timecode, end_timecode, transcript, phash) VALUES (?, ?, ?, ?, ?)"
            params = (content_id, scene.get('start'), scene.get('end'), scene.get('transcript'), scene.get('phash'))
            scene_id = await execute_db_query(conn, query, params, return_last_row_id=True)

            if scene_embedding := scene.get('embedding'):
                await self._insert_embedding(conn, scene_embedding, clip_model_name, scene_id=scene_id)

            if scene_tags := scene.get('tags'):
                await self._insert_tags_for_entity(conn, scene_tags, tag_type='tags', scene_id=scene_id)

    async def _insert_embedding(self, conn: aiosqlite.Connection, embedding_bytes, model_name, *, content_id=None, scene_id=None):
        query = "INSERT OR IGNORE INTO clip_embeddings (content_id, scene_id, embedding, model_name) VALUES (?, ?, ?, ?)"
        params = (content_id, scene_id, embedding_bytes, model_name)
        await execute_db_query(conn, query, params)

    async def _insert_tags_for_entity(self, conn: aiosqlite.Connection, tags: List[str], tag_type: str, *, content_id: Optional[int] = None, scene_id: Optional[int] = None):
        if not (content_id or scene_id):
            raise ValueError("Either content_id or scene_id must be provided.")
        
        try:
            tables = _get_table_names(tag_type)
        except ValueError as e:
            logger.error(f"Invalid tag_type provided to _insert_tags_for_entity: {e}")
            return

        for tag_name in tags:
            tag_name_lower = tag_name.lower()
            await execute_db_query(conn, f"INSERT OR IGNORE INTO {tables['main']} (name) VALUES (?)", (tag_name_lower,))
            
            tag_id_row = await execute_db_query(conn, f"SELECT id FROM {tables['main']} WHERE name = ?", (tag_name_lower,), fetch_one=True)
            
            if not tag_id_row:
                logger.warning(f"Could not create or find tag '{tag_name}' in '{tables['main']}'. Skipping assignment.")
                continue
            tag_id = tag_id_row['id']
            
            if content_id:
                # Assuming the junction table is named 'file_content_tags' or similar and the helper handles it.
                # If the junction table name in your helper is hardcoded to 'file_tags', that helper will need updating.
                # For this code to work, the junction table must link `content_id` and `tag_id`.
                await execute_db_query(conn, f"INSERT OR IGNORE INTO {tables['file_junction']} (content_id, tag_id) VALUES (?, ?)", (content_id, tag_id))

            if scene_id:
                await execute_db_query(conn, f"INSERT OR IGNORE INTO {tables['scene_junction']} (scene_id, tag_id) VALUES (?, ?)", (scene_id, tag_id))

async def setup_database():
    """Creates or updates the database schema asynchronously."""
    db_file = config['DB_FILE']
    for path_key in ['FILESTORE_DIR', 'TRANSCODED_CACHE_DIR', 'THUMBNAILS_DIR']:
        dir_path = os.path.abspath(config[path_key])
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Ensured directory exists: {dir_path}")

    async with aiosqlite.connect(db_file) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")

        # --- Folder Table ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS folders (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            path TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES folders (id) ON DELETE CASCADE
        );""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_folders_path ON folders (path);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_folders_parent_id ON folders (parent_id);")

        # Stores data about unique content, identified by its hash. One row per unique file.
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS file_content (
            id INTEGER PRIMARY KEY,
            file_hash TEXT NOT NULL UNIQUE,
            file_type TEXT,
            extension TEXT,
            phash TEXT,
            metadata TEXT,
            transcript TEXT,
            duration_seconds REAL,
            size_in_bytes INTEGER,
            is_encrypted INTEGER NOT NULL DEFAULT 0,
            is_webready INTEGER NOT NULL DEFAULT 0,
            transcoded_path TEXT
        );""")

        # Stores each discovered copy of a file. Multiple instances can point to the same content.
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS file_instances (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            stored_path TEXT NOT NULL UNIQUE,
            file_name TEXT,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP,
            ingest_source TEXT,
            original_created_at TIMESTAMP,
            original_modified_at TIMESTAMP,
            uploader_id INTEGER, 
            folder_id INTEGER,
            FOREIGN KEY (content_id) REFERENCES file_content (id) ON DELETE CASCADE,
            FOREIGN KEY (uploader_id) REFERENCES users (id) ON DELETE SET NULL,
            FOREIGN KEY (folder_id) REFERENCES folders (id) ON DELETE SET NULL
        );""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_instances_content_id ON file_instances (content_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_instances_folder_id ON file_instances (folder_id);")

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS scenes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            content_id INTEGER NOT NULL,
            start_timecode TEXT NOT NULL, end_timecode TEXT NOT NULL,
            transcript TEXT, phash TEXT,
            FOREIGN KEY (content_id) REFERENCES file_content (id) ON DELETE CASCADE
        );""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_scenes_content_id ON scenes (content_id);")
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS clip_embeddings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id INTEGER UNIQUE, 
            scene_id INTEGER UNIQUE,
            embedding BLOB NOT NULL, model_name TEXT,
            FOREIGN KEY (content_id) REFERENCES file_content (id) ON DELETE CASCADE,
            FOREIGN KEY (scene_id) REFERENCES scenes (id) ON DELETE CASCADE
        );""")

        # Embedding similarity table remains the same, as it links embedding IDs.
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS embedding_similarity (
            embedding_id_a INTEGER NOT NULL, embedding_id_b INTEGER NOT NULL,
            score REAL NOT NULL, method TEXT NOT NULL,
            FOREIGN KEY (embedding_id_a) REFERENCES clip_embeddings (id) ON DELETE CASCADE,
            FOREIGN KEY (embedding_id_b) REFERENCES clip_embeddings (id) ON DELETE CASCADE,
            PRIMARY KEY (embedding_id_a, embedding_id_b),
            CHECK (embedding_id_a < embedding_id_b)
        );""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_embedding_similarity_b ON embedding_similarity (embedding_id_b);")
        
        # --- Visibility is per-instance ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS instance_visibility_roles (
            instance_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            FOREIGN KEY (instance_id) REFERENCES file_instances (id) ON DELETE CASCADE,
            FOREIGN KEY (role_id) REFERENCES roles (id) ON DELETE CASCADE,
            PRIMARY KEY (instance_id, role_id)
        );""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_instance_visibility_roles_role_id ON instance_visibility_roles (role_id);")
        
        await conn.commit()
    logger.debug(f"Database '{db_file}' is set up and schema is verified.")