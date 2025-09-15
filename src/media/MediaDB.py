# src/media/MediaDB.py
import logging
import os
from typing import List, Optional
from datetime import datetime
import json
import aiosqlite

from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query
from src.log.HelperLog import log_event_async
from src.models import MediaData
from src.roles.rbac_manager import get_everyone_role_id
from src.tag_utils import _get_table_names

config = get_config()
logger = logging.getLogger(__name__)

class DatabaseService:
    """Handles all database write operations for media ingestion using async methods."""

    async def add_media(self, data: MediaData, conn: Optional[aiosqlite.Connection] = None) -> int:
        """Adds all media data to the database in a single async transaction."""
        async with AsyncDBContext(conn) as db_conn:
            try:
                file_id = await self._insert_file_record(db_conn, data)
                
                await self._insert_visibility_roles(db_conn, file_id, data.visibility_roles)
                
                if data.clip_embedding:
                    await self._insert_embedding(db_conn, data.clip_embedding, data.clip_model_name, file_id=file_id)

                if data.scenes_data:
                    await self._insert_scenes(db_conn, file_id, data.scenes_data, data.clip_model_name)
                
                if data.tags_list:
                    await self._insert_tags_for_entity(
                        db_conn, data.tags_list, tag_type='tags', file_id=file_id
                    )
                
                if data.model_name:
                    meta_tag_name = f"Auto Tagged By: {data.model_name}"
                    await self._insert_tags_for_entity(
                        db_conn, [meta_tag_name], tag_type='meta_tags', file_id=file_id
                    )

                await log_event_async(
                    conn=db_conn,
                    event_type='file_ingest',
                    actor_type='system',
                    actor_name=data.ingest_source,
                    target_type='file',
                    target_id=file_id,
                    details={'file_name': data.file_name}
                )
                
                logger.info(f"File {data.file_name} (ID: {file_id}) successfully ingested into DB.")
                return file_id
            except Exception as e:
                logger.exception(f"Database transaction failed for {data.file_name}. Error: {e}")
                raise # The AsyncDBContext will handle rollback

    async def _insert_file_record(self, conn: aiosqlite.Connection, data: MediaData) -> int:
        query = """
            INSERT INTO files (file_hash, file_type, extension, stored_path, file_name, phash,
                               is_encrypted, metadata, discovered_at, processed_at, ingest_source,
                               original_created_at, original_modified_at, transcript, duration_seconds, 
                               uploader_id, size_in_bytes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """ 
        params = (data.file_hash, data.file_type, data.extension, data.stored_path, data.file_name, data.phash,
                  int(data.is_encrypted), json.dumps(data.metadata), datetime.now().isoformat(),
                  datetime.now().isoformat(), data.ingest_source,
                  data.original_created_at, data.original_modified_at, data.transcript, data.duration_seconds, 
                  data.uploader_id, data.size_in_bytes)
        file_id = await execute_db_query(conn, query, params, return_last_row_id=True)
        return file_id

    async def _insert_visibility_roles(self, conn: aiosqlite.Connection, file_id: int, role_ids: List[int]):
        """Inserts visibility role relationships for a file."""
        # If no roles are specified, default to the 'Everyone' role.
        if not role_ids:
            try:
                # This function is synchronous but just reads from a cached dict, so it's fast.
                everyone_id = get_everyone_role_id()
                roles_to_insert = [everyone_id]
                logger.debug(f"No visibility roles specified for file ID {file_id}. Defaulting to 'Everyone' (Role ID: {everyone_id}).")
            except RuntimeError as e:
                # This happens if RBAC isn't initialized. It's a critical error.
                logger.error(f"Could not get 'Everyone' role ID: {e}. File ID {file_id} will have NO visibility roles set.")
                return
        else:
            roles_to_insert = list(set(role_ids)) # Ensure unique IDs

        if roles_to_insert:
            query = "INSERT OR IGNORE INTO file_visibility_roles (file_id, role_id) VALUES (?, ?)"
            params = [(file_id, role_id) for role_id in roles_to_insert]
            await conn.executemany(query, params)

    async def _insert_embedding(self, conn: aiosqlite.Connection, embedding_bytes, model_name, *, file_id=None, scene_id=None):
        query = "INSERT OR IGNORE INTO clip_embeddings (file_id, scene_id, embedding, model_name) VALUES (?, ?, ?, ?)"
        params = (file_id, scene_id, embedding_bytes, model_name)
        await execute_db_query(conn, query, params)

    async def _insert_scenes(self, conn: aiosqlite.Connection, file_id, scenes_data, clip_model_name):
        for scene in scenes_data:
            query = "INSERT INTO scenes (file_id, start_timecode, end_timecode, transcript, phash) VALUES (?, ?, ?, ?, ?)"
            params = (file_id, scene.get('start'), scene.get('end'), scene.get('transcript'), scene.get('phash'))
            scene_id = await execute_db_query(conn, query, params, return_last_row_id=True)

            if scene_embedding := scene.get('embedding'):
                await self._insert_embedding(conn, scene_embedding, clip_model_name, scene_id=scene_id)
            
            if scene_tags := scene.get('tags'):
                await self._insert_tags_for_entity(conn, scene_tags, tag_type='tags', scene_id=scene_id)

    async def _insert_tags_for_entity(self, conn: aiosqlite.Connection, tags: List[str], tag_type: str, *, file_id: Optional[int] = None, scene_id: Optional[int] = None):
        if not (file_id or scene_id):
            raise ValueError("Either file_id or scene_id must be provided.")
        
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
            
            if file_id:
                await execute_db_query(conn, f"INSERT OR IGNORE INTO {tables['file_junction']} (file_id, tag_id) VALUES (?, ?)", (file_id, tag_id))

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

        # --- Core Media Tables ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY, file_hash TEXT NOT NULL UNIQUE, file_type TEXT,
            extension TEXT, stored_path TEXT NOT NULL, file_name TEXT, phash TEXT,
            is_encrypted INTEGER NOT NULL DEFAULT 0, metadata TEXT, transcript TEXT,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP,
            ingest_source TEXT, transcoded_path TEXT, duration_seconds REAL,
            original_created_at TIMESTAMP, original_modified_at TIMESTAMP,
            uploader_id INTEGER, 
            size_in_bytes INTEGER, -- <-- NEW
            FOREIGN KEY (uploader_id) REFERENCES users (id) ON DELETE SET NULL
        );""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_files_size_in_bytes ON files (size_in_bytes);") # <-- NEW INDEX

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_files_discovered_at_id ON files (discovered_at DESC, id DESC);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_files_file_type ON files (file_type);")

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS scenes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, file_id INTEGER NOT NULL,
            start_timecode TEXT NOT NULL, end_timecode TEXT NOT NULL,
            transcript TEXT, phash TEXT,
            FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE
        );""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_scenes_file_id ON scenes (file_id);")
        
        # --- Embedding & Similarity Tables ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS clip_embeddings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER UNIQUE, scene_id INTEGER UNIQUE,
            embedding BLOB NOT NULL, model_name TEXT,
            FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE,
            FOREIGN KEY (scene_id) REFERENCES scenes (id) ON DELETE CASCADE
        );""")

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
        
        # --- Junction table for file-role visibility ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS file_visibility_roles (
            file_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE,
            FOREIGN KEY (role_id) REFERENCES roles (id) ON DELETE CASCADE,
            PRIMARY KEY (file_id, role_id)
        );""")
        # Add an index on role_id for efficient lookups of all files a role can see.
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_visibility_roles_role_id ON file_visibility_roles (role_id);")
        
        await conn.commit()
    logger.debug(f"Database '{db_file}' is set up and schema is verified.")
