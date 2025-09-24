# actions.py
import asyncio
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor
import os
from typing import List, Optional
import aiosqlite
import json
from datetime import datetime

from ConfigMedia import get_config
from ImageTagger import Tagger
from src.db_utils import AsyncDBContext, execute_db_query
from src.log.HelperLog import log_event_async
from src.media.HelperMedia import extract_video_scene_data_hybrid
from src.media.MediaDB import DatabaseService
from src.media.file_persistence import FilePersistenceService
from src.tag_utils import _get_table_names
from src.media.schemas import MediaData
from TaskManager import task_registry
from src.media.utilsVideo import get_video_duration, HARDWARE_ENCODERS

logger = logging.getLogger("MediaActions")
logger.setLevel(logging.DEBUG)
config = get_config()


async def update_tags_for_content(content_id: int, tags: List[str], model_name: Optional[str] = None, conn: Optional[aiosqlite.Connection] = None):
    """
    Updates the tags for a specific piece of content, affecting all its instances.
    """
    async with AsyncDBContext(conn) as db_conn:
        db_service = DatabaseService()
        
        async with db_conn.execute("BEGIN"):
            # Delete existing tags for the content
            await execute_db_query(db_conn, f"DELETE FROM {_get_table_names('tags')['file_junction']} WHERE content_id = ?", (content_id,))
            await execute_db_query(db_conn, f"DELETE FROM {_get_table_names('meta_tags')['file_junction']} WHERE content_id = ?", (content_id,))

            # Insert new tags
            await db_service._insert_tags_for_entity(db_conn, tags, tag_type='tags', content_id=content_id)

            if model_name:
                meta_tag_name = f"Auto Tagged By: {model_name}"
                await db_service._insert_tags_for_entity(db_conn, [meta_tag_name], tag_type='meta_tags', content_id=content_id)
        
        logger.info(f"Updated tags for content_id: {content_id}")

async def update_scenes_for_content(content_id: int, scenes_data: List[dict], clip_model_name: str, conn: Optional[aiosqlite.Connection] = None):
    """
    Updates the scenes for a specific piece of content, affecting all its instances.
    """
    async with AsyncDBContext(conn) as db_conn:
        async with db_conn.execute("BEGIN"):
            # Delete existing scenes and their related embeddings for the content
            # This is safe due to ON DELETE CASCADE on the foreign keys
            await execute_db_query(db_conn, "DELETE FROM scenes WHERE content_id = ?", (content_id,))

            # Insert new scenes
            db_service = DatabaseService()
            await db_service._insert_scenes(db_conn, content_id, scenes_data, clip_model_name)

        logger.info(f"Updated scenes for content_id: {content_id}")

from processing_status import status_manager

# This decorator was incorrect. The function is now a proper async coroutine.
# The blocking (CPU/GPU-bound) parts inside will be explicitly run in an executor.
# @task_registry.run_in_executor("image")
async def retag_file(instance_id: int, tagger_model_name: str, clip_model_name: str, tagger: Tagger, clip_model, clip_preprocess):
    """
    Re-processes a file instance to update its underlying content data (tags, scenes).
    This operation will affect all other instances that share the same content.
    """
    logger.info(f"Starting re-tagging process for instance ID: {instance_id}")
    loop = asyncio.get_running_loop()

    query = """
        SELECT
            fi.stored_path,
            fi.file_name,
            fc.id as content_id,
            fc.file_type
        FROM file_instances fi
        JOIN file_content fc ON fi.content_id = fc.id
        WHERE fi.id = ?
    """
    async with AsyncDBContext() as db:
        content_details = await execute_db_query(db, query, (instance_id,), fetch_one=True)
    
    if not content_details:
        logger.error(f"Could not find instance with ID {instance_id} for re-tagging.")
        status_manager.set_error(instance_id, "File instance not found in database.")
        return

    status_manager.start_processing(
        instance_id,
        "Initializing re-tagging...",
        filename=content_details['file_name'],
        ingest_source='retagging'
    )

    content_id = content_details['content_id']
    stored_path = content_details['stored_path']

    if not stored_path or not os.path.exists(stored_path):
        logger.error(f"Stored path for instance ID {instance_id} not found or file does not exist.")
        status_manager.set_error(instance_id, "File not found on disk.")
        return

    try:
        # Determine which executor to use based on file type.
        # The task was scheduled on the 'image' or 'video' manager, so we can get its executor.
        manager_name = 'image' if content_details['file_type'] == 'image' else 'video'
        executor = task_registry.get_manager(manager_name).executor

        if content_details['file_type'] == 'image':
            
            def _blocking_image_tagging():
                """Synchronous function to run in the executor."""
                status_manager.update_progress(instance_id, 10.0, "Opening image...")
                from PIL import Image
                img_pil = Image.open(stored_path)
                with img_pil:
                    img_rgb = img_pil.convert("RGB")

                status_manager.update_progress(instance_id, 30.0, "Tagging with AI model...")
                return tagger.tag(img_rgb, 0.3)

            # Run the blocking image processing in the correct thread pool
            new_tags = await loop.run_in_executor(executor, _blocking_image_tagging)
            
            status_manager.update_progress(instance_id, 80.0, "Updating database...")
            # Update tags on the CONTENT record, not the instance
            await update_tags_for_content(content_id, new_tags, model_name=tagger_model_name)
            logger.info(f"Successfully re-tagged content (ID: {content_id}) for instance ID: {instance_id}")
            status_manager.set_completed(instance_id, f"Successfully tagged with {len(new_tags)} tags.")

        elif content_details['file_type'] == 'video':
            def on_progress_callback(progress: float, message: str):
                # Scale progress from the extractor (0-100) to the retagging task's range (0-90)
                scaled_progress = progress * 0.9
                status_manager.update_progress(instance_id, scaled_progress, message)

            # The hybrid function is already designed to be run in an executor.
            # We pass the correct executor from the manager.
            scenes_data, _ = await loop.run_in_executor(
                executor,
                extract_video_scene_data_hybrid,
                stored_path, tagger, clip_model, clip_preprocess,
                True, # should_tag
                on_progress_callback
            )
            
            if scenes_data is not None:
                # Update scenes on the CONTENT record, not the instance
                await update_scenes_for_content(content_id, scenes_data, clip_model_name)
                logger.info(f"Successfully re-processed scenes for content (ID: {content_id}) from instance ID: {instance_id}")
                status_manager.set_completed(instance_id, "Successfully re-processed video scenes.")
            else:
                logger.error(f"Scene extraction failed for instance ID: {instance_id}")

    except Exception as e:
        logger.exception(f"An error occurred during re-tagging of instance ID {instance_id}: {e}")
        status_manager.set_error(instance_id, str(e))

# This function is now called via task_registry.schedule_task, not a decorator.
async def run_media_conversion_and_ingest(
    instance_id: int,
    source_path: str,
    original_filename: str,
    file_type: str,
    conversion_options: dict,
    scanner_app_state: dict,
    user_id: int,
    task_type: str = 'conversion'
):
    """
    Converts a media file using ffmpeg and queues the result for ingestion.
    This is a background task run by a TaskManager worker.
    """
    status_manager.start_processing(
        instance_id,
        f"Starting {task_type}...",
        filename=original_filename,
        ingest_source=task_type
    )
    
    if not scanner_app_state or 'intake_queue' not in scanner_app_state:
        logger.error("Scanner service is not available for ingestion after conversion.")
        status_manager.set_error(instance_id, "Scanner service unavailable.")
        return

    upload_dir = os.path.join(config.get("FILESTORE_DIR"), "_incoming")
    os.makedirs(upload_dir, exist_ok=True)
    
    base_name, _ = os.path.splitext(original_filename)
    
    output_path = None
    cmd = ['ffmpeg', '-progress', 'pipe:1', '-y']
    # Let ffmpeg use CPU for decoding to avoid driver conflicts, only use GPU for encoding.
    cmd.extend(['-i', source_path])
    if file_type == 'video' and conversion_options.get('video_codec'):
        codec = conversion_options['video_codec']
        new_filename = ""  # Initialize to prevent unbound local error

        if codec in ['libx265', 'hevc']:
            new_filename = f"{base_name}_hevc.mp4"
            encoder = HARDWARE_ENCODERS['hevc']
            if encoder == 'hevc_nvenc': cmd.extend(['-c:v', 'hevc_nvenc', '-preset', 'p5', '-cq', '24'])
            elif encoder == 'hevc_qsv': cmd.extend(['-c:v', 'hevc_qsv', '-preset', 'fast', '-global_quality', '24'])
            else: cmd.extend(['-c:v', 'libx265', '-crf', '26', '-preset', 'fast'])
            cmd.extend(['-c:a', 'aac', '-b:a', '192k'])

        elif codec in ['libaom-av1', 'av1']:
            new_filename = f"{base_name}_av1.mkv"
            encoder = HARDWARE_ENCODERS['av1']
            if encoder == 'av1_nvenc': cmd.extend(['-c:v', 'av1_nvenc', '-preset', 'p5', '-cq', '30'])
            elif encoder == 'av1_qsv': cmd.extend(['-c:v', 'av1_qsv', '-preset', 'fast', '-global_quality', '30'])
            else: cmd.extend(['-c:v', 'libaom-av1', '-crf', '30', '-b:v', '0', '-cpu-used', '8'])
            cmd.extend(['-c:a', 'libopus'])

        elif codec in ['libx264', 'h264']:
            new_filename = f"{base_name}_h264.mp4"
            encoder = HARDWARE_ENCODERS['h264']
            if encoder == 'h264_nvenc': cmd.extend(['-c:v', 'h264_nvenc', '-preset', 'p5', '-cq', '23'])
            elif encoder == 'h264_qsv': cmd.extend(['-c:v', 'h264_qsv', '-preset', 'fast', '-global_quality', '23'])
            else: cmd.extend(['-c:v', 'libx264', '-crf', '23', '-preset', 'fast'])
            cmd.extend(['-c:a', 'aac', '-b:a', '192k'])

        else:  # Generic fallback for any other codec name passed from the API.
            new_filename = f"{base_name}_{codec}.mkv"
            logger.warning(f"Requested codec '{codec}' is not explicitly handled. Attempting generic conversion. "
                           "Success depends on providing a valid ffmpeg encoder name.")
            cmd.extend(['-c:v', codec])
            # Using a generic quality scale. `-q:v 3` is a reasonable default for many codecs.
            cmd.extend(['-q:v', '3'])
            # Opus is a good default audio codec for mkv, which is a flexible container.
            cmd.extend(['-c:a', 'libopus'])

        output_path = os.path.join(upload_dir, new_filename)
        cmd.extend([output_path])

    elif file_type == 'image' and conversion_options.get('image_format'):
        img_format = conversion_options['image_format'].lower()
        new_filename = f"{base_name}.{img_format}"
        output_path = os.path.join(upload_dir, new_filename)
        # -progress is not useful for images, so we rebuild the command.
        cmd = ['ffmpeg', '-y', '-i', source_path, output_path]
    
    if not output_path:
        status_manager.set_error(instance_id, "Invalid conversion options provided.")
        return

    duration_seconds = await get_video_duration(source_path) if file_type == 'video' else 0
    proc = None
    try:
        status_manager.update_progress(instance_id, 5.0, f"Converting with ffmpeg...")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        while proc.returncode is None:
            if file_type == 'video' and duration_seconds > 0 and 'pipe:1' in cmd and proc.stdout:
                line = await proc.stdout.readline()
                if not line: break
                line_str = line.decode().strip()
                if line_str.startswith('out_time_ms='):
                    try:
                        current_ms_str = line_str.split('=')[1].strip()
                        if current_ms_str != 'N/A':
                            current_ms = int(current_ms_str)
                            progress = 5 + (current_ms / (duration_seconds * 1000000)) * 85
                            status_manager.update_progress(instance_id, min(progress, 90.0), "Converting...")
                    except (ValueError, IndexError):
                        pass # Ignore malformed lines
            else:
                await asyncio.sleep(0.5)
        
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd, output=stdout, stderr=stderr)

        # Get the content_id of the original file to pass it along for scene preservation.
        async with AsyncDBContext() as db:
            content_details = await execute_db_query(db, "SELECT content_id FROM file_instances WHERE id = ?", (instance_id,), fetch_one=True)
        original_content_id = content_details['content_id'] if content_details else None

        status_manager.update_progress(instance_id, 95.0, "Queueing for ingestion...")
        # The payload includes the original content_id to allow for scene data to be copied.
        ingest_payload = (output_path, "conversion", user_id, new_filename, original_content_id)
        await scanner_app_state['intake_queue'].put(ingest_payload)
        status_manager.set_completed(instance_id, "Conversion complete, pending ingestion.")
    except subprocess.CalledProcessError as e:
        err_output = e.stderr.decode(errors='ignore') if e.stderr else "No stderr output."
        logger.error(f"FFmpeg conversion failed for {original_filename}: {err_output}")
        status_manager.set_error(instance_id, f"FFmpeg error: {err_output[:250]}")
    except Exception as e:
        logger.exception(f"An error occurred during conversion of instance ID {instance_id}: {e}")
        status_manager.set_error(instance_id, str(e))
    finally:
        if proc and proc.returncode is None:
            proc.kill()
            await proc.wait()

async def process_media(media_data: MediaData) -> Optional[int]:
    """
    Orchestrates the ingestion of a media file. It persists the file, then adds
    its content and instance data to the database. Returns the new instance ID.
    """
    persistence = FilePersistenceService(config['FILESTORE_DIR'], config['THUMBNAILS_DIR'])
    db_service = DatabaseService()

    try:
        # Step 1: Persist the file to storage (sync)
        # The persistence service handles whether to copy the file or use the original path.
        stored_path = persistence.store(media_data)
        media_data.stored_path = stored_path

        # Step 2: Write all metadata to the database (async)
        # The add_media method now handles the logic for creating content and/or instances.
        instance_id = await db_service.add_media(media_data)

        # Step 3: Create thumbnail only after DB transaction is successful (sync)
        if instance_id is not None:
            persistence.create_thumbnail(media_data)
        
        return instance_id

    except Exception as e:
        logger.error(f"Processing failed for {media_data.file_name}: {e}")
        # Cleanup partially stored file if persistence succeeded but DB failed.
        # This is important if your store() method copies files.
        if config.get('COPY_FILE_TO_FOLDER') and media_data.stored_path and os.path.exists(media_data.stored_path):
            os.remove(media_data.stored_path)
            logger.warning(f"Cleaned up partially stored file: {media_data.stored_path}")
        return None


# async def update_media_record(file_id: int, data: MediaData, old_file_hash: str):
#     """
#     Updates an existing media record in the database.
#     NOTE: This function appears to be obsolete and based on the old database schema.
#     The current update strategy is a "delete and re-create" handled by `update_media_task`
#     in FileProcessors.py. This function is preserved but commented out to avoid confusion.
#     """
#     logger.info(f"Updating database record for file ID: {file_id}")
#     db_service = DatabaseService()
#     persistence = FilePersistenceService(config['FILESTORE_DIR'], config['THUMBNAILS_DIR'])
#
#     async with AsyncDBContext() as db_conn:
#         try:
#             # Start transaction
#             async with db_conn.execute("BEGIN"):
#                 # 1. Update the files table
#                 query = """
#                     UPDATE files
#                     SET file_hash = ?, file_type = ?, extension = ?, stored_path = ?, file_name = ?, phash = ?,
#                         is_encrypted = ?, metadata = ?, processed_at = ?, ingest_source = ?,
#                         original_modified_at = ?, transcript = ?, duration_seconds = ?, size_in_bytes = ?
#                     WHERE id = ?
#                 """
#                 params = (data.file_hash, data.file_type, data.extension, data.stored_path, data.file_name, data.phash,
#                           int(data.is_encrypted), json.dumps(data.metadata), datetime.now().isoformat(),
#                           data.ingest_source, data.original_modified_at, data.transcript, data.duration_seconds,
#                           data.size_in_bytes, file_id)
#                 await execute_db_query(db_conn, query, params)
#
#                 # 2. Delete old related data
#                 await execute_db_query(db_conn, "DELETE FROM scenes WHERE file_id = ?", (file_id,))
#                 await execute_db_query(db_conn, "DELETE FROM file_tags WHERE file_id = ?", (file_id,))
#                 await execute_db_query(db_conn, "DELETE FROM clip_embeddings WHERE file_id = ?", (file_id,))
#
#                 # 3. Insert new related data
#                 if data.clip_embedding:
#                     await db_service._insert_embedding(db_conn, data.clip_embedding, data.clip_model_name, file_id=file_id)
#
#                 if data.scenes_data:
#                     await db_service._insert_scenes(db_conn, file_id, data.scenes_data, data.clip_model_name)
#
#                 if data.tags_list:
#                     await db_service._insert_tags_for_entity(
#                         db_conn, data.tags_list, tag_type='tags', file_id=file_id
#                     )
#
#                 if data.model_name:
#                     meta_tag_name = f"Auto Tagged By: {data.model_name}"
#                     await db_service._insert_tags_for_entity(
#                         db_conn, [meta_tag_name], tag_type='meta_tags', file_id=file_id
#                     )
#                
#                 await log_event_async(
#                     conn=db_conn,
#                     event_type='file_update',
#                     actor_type='system',
#                     actor_name=data.ingest_source,
#                     target_type='file',
#                     target_id=file_id,
#                     details={'file_name': data.file_name, 'new_hash': data.file_hash}
#                 )
#
#             # 4. Handle thumbnails (after DB transaction)
#             if old_file_hash:
#                 thumb_format = config.get('THUMBNAIL_FORMAT', 'WEBP').lower()
#                 old_thumb_path = os.path.join(config['THUMBNAILS_DIR'], f"{old_file_hash}.{thumb_format}")
#                 if os.path.exists(old_thumb_path):
#                     os.remove(old_thumb_path)
#                     logger.info(f"Deleted old thumbnail: {old_thumb_path}")
#
#             persistence.create_thumbnail(data)
#
#             logger.info(f"File ID {file_id} successfully updated in the database.")
#
#         except Exception as e:
#             logger.exception(f"Database transaction failed for file update ID {file_id}. Error: {e}")
#             raise
