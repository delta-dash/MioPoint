import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
import os
from typing import List, Optional
import aiosqlite

from ConfigMedia import get_config
from ImageTagger import Tagger
from src.db_utils import AsyncDBContext, execute_db_query
from src.media.HelperMedia import extract_video_scene_data, get_file_details, is_hash_in_db_async
from src.media.MediaDB import DatabaseService
from src.media.file_persistence import FilePersistenceService
from src.models import MediaData

logger = logging.getLogger("MediaActions")
config = get_config()


async def update_tags_for_file(file_id: int, tags: List[str], model_name: Optional[str] = None, conn: Optional[aiosqlite.Connection] = None):
    """Updates the tags for a specific file."""
    async with AsyncDBContext(conn) as db_conn:
        # Start a transaction
        async with db_conn.execute("BEGIN"):
            # Delete existing tags for the file
            await execute_db_query(db_conn, "DELETE FROM file_tags WHERE file_id = ?", (file_id,))

            # Insert new tags
            db_service = DatabaseService()
            await db_service._insert_tags_for_entity(db_conn, tags, tag_type='tags', file_id=file_id)

            if model_name:
                meta_tag_name = f"Auto Tagged By: {model_name}"
                await db_service._insert_tags_for_entity(db_conn, [meta_tag_name], tag_type='meta_tags', file_id=file_id)

async def update_scenes_for_file(file_id: int, scenes_data: List[dict], clip_model_name: str, conn: Optional[aiosqlite.Connection] = None):
    """Updates the scenes for a specific file."""
    async with AsyncDBContext(conn) as db_conn:
        # Start a transaction
        async with db_conn.execute("BEGIN"):
            # Delete existing scenes for the file
            await execute_db_query(db_conn, "DELETE FROM scenes WHERE file_id = ?", (file_id,))

            # Insert new scenes
            db_service = DatabaseService()
            await db_service._insert_scenes(db_conn, file_id, scenes_data, clip_model_name)


# This is the EXACT same function, just moved from FileProcessors.py
async def retag_file(file_id: int, executor: ThreadPoolExecutor, tagger_model_name: str, clip_model_name: str, tagger: Tagger, clip_model, clip_preprocess, progress_cache: dict):
    logger.info(f"Starting auto-tagging for file ID: {file_id}")
    progress_cache[file_id] = 0
    file_details = await get_file_details(file_id)
    if not file_details:
        logger.error(f"Could not find file with ID {file_id} for re-tagging.")
        progress_cache[file_id] = {"status": "error", "message": "File not found."}
        return

    stored_path = file_details.get('stored_path')
    if not stored_path or not os.path.exists(stored_path):
        logger.error(f"Stored path for file ID {file_id} not found or file does not exist.")
        progress_cache[file_id] = {"status": "error", "message": "File not found on disk."}
        return

    loop = asyncio.get_running_loop()
    try:
        if file_details['file_type'] == 'image':
            # Note: This part needs Image from PIL.
            from PIL import Image
            img_pil = await loop.run_in_executor(executor, Image.open, stored_path)
            with img_pil:
                img_rgb = await loop.run_in_executor(executor, img_pil.convert, "RGB")

            new_tags = tagger.tag(img_rgb, 0.3)
            progress_cache[file_id] = 50

            await update_tags_for_file(file_id, new_tags, model_name=tagger_model_name)
            logger.info(f"Successfully re-tagged file ID: {file_id} with tags: {new_tags}")
            progress_cache[file_id] = 100

        elif file_details['file_type'] == 'video':
            # We must import _extract_video_scene_data, which means FileProcessors must not
            # have circular dependencies itself. This is a potential issue.
            # For now, let's assume it's a helper that can be moved or imported.
            scenes_data, _ = await loop.run_in_executor(
                executor,
                extract_video_scene_data,
                stored_path,
                tagger,
                clip_model,
                clip_preprocess,
                True, # should_tag
                progress_cache,
                file_id
            )
            await update_scenes_for_file(file_id, scenes_data, clip_model_name)
            logger.info(f"Successfully re-tagged video file ID: {file_id}")
            progress_cache[file_id] = 100

    except Exception as e:
        logger.exception(f"An error occurred during re-tagging of file ID {file_id}: {e}")
        progress_cache[file_id] = {"status": "error", "message": str(e)}


async def process_media(media_data: MediaData) -> Optional[int]:
    """
    Orchestrates the ingestion of a media file by coordinating persistence and database services.
    """
    if await is_hash_in_db_async(media_data.file_hash):
        logger.info(f"Content with hash {media_data.file_hash} already exists. Skipping.")
        return None

    persistence = FilePersistenceService(config['FILESTORE_DIR'], config['THUMBNAILS_DIR'])
    db_service = DatabaseService()

    try:
        # Step 1: Persist the file to storage (sync)
        # This step might change media_data.is_encrypted if encryption fails
        stored_path = persistence.store(media_data)
        media_data.stored_path = stored_path

        # Step 2: Write all metadata to the database in a single transaction (async)
        file_id = await db_service.add_media(media_data)

        # Step 3: Create thumbnail only after DB transaction is successful (sync)
        if file_id is not None:
            persistence.create_thumbnail(media_data)
        
        return file_id

    except Exception as e:
        logger.error(f"Processing failed for {media_data.file_name}: {e}")
        # Cleanup partially stored file if persistence succeeded but DB failed
        if media_data.stored_path and os.path.exists(media_data.stored_path):
            os.remove(media_data.stored_path)
            logger.warning(f"Cleaned up partially stored file: {media_data.stored_path}")
        return None