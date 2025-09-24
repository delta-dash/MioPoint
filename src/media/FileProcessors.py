"""
Handles the detection and processing of media files.

This module is responsible for the "extract" part of the ETL (Extract, Transform, Load) process.
It extracts all relevant metadata, hashes, tags, and embeddings from a file. This extracted 
information is packaged into a MediaData object and passed to the MediaDB module for storage.
"""
import asyncio
import logging
import os
import sqlite3
import hashlib
import functools
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- Core Libraries ---
from PIL import Image, UnidentifiedImageError
import numpy as np

# --- Custom Modules ---
from src.media.actions import process_media
from processing_status import status_manager
from src.media.schemas import MediaData, ProcessingJob
from utils import get_original_filename
from utilsEncryption import calculate_file_hash
from src.media.utilsVideo import is_video_codec_compatible
from ConfigMedia import get_config
from Searcher import get_searcher

# Your HelperMedia file must be updated to work with the new content/instance schema.
from src.media.HelperMedia import (
    delete_instance_and_thumbnail_async,  # This is a new required helper
    extract_image_data_sync,
    extract_video_metadata_sync,
    extract_video_scene_data_hybrid,
    get_file_details_by_path_async,
    get_content_by_hash_async,
)

# --- Initialization ---
CONFIG = get_config()
logger = logging.getLogger("FileProcessor")
logger.setLevel(logging.DEBUG)


# ==============================================================================
# 1. UTILITY AND DECORATOR FUNCTIONS
# ==============================================================================

def _open_and_resize_image_if_needed(path: str, max_dimension: int) -> Image.Image:
    """
    Opens an image and resizes it if it exceeds the max dimension,
    conserving memory.
    """
    img = Image.open(path)
    if max_dimension > 0 and (img.width > max_dimension or img.height > max_dimension):
        logger.info(f"Image '{os.path.basename(path)}' ({img.width}x{img.height}) exceeds max dimension ({max_dimension}). Downscaling.")
        # Use thumbnail to resize in-place, which is memory efficient.
        # It uses a high-quality downsampling filter.
        img.thumbnail((max_dimension, max_dimension), Image.LANCZOS)
        logger.info(f"Downscaled to {img.width}x{img.height}.")
    return img

def get_file_type_and_extension(path: str, file_type_map: dict) -> tuple[str, str]:
    """
    Determines file type from a mapping. Handles both old and new config formats.
    - New format: {'image': ['.jpg', '.png'], ...}
    - Old format: {'.jpg': 'image', '.png': 'image', ...}
    """
    _, ext = os.path.splitext(path)
    ext = ext.lower()

    # Heuristic to detect format: check if a value is a list.
    is_new_format = False
    if file_type_map:
        first_value = next(iter(file_type_map.values()), None)
        if isinstance(first_value, list):
            is_new_format = True

    if is_new_format:
        for file_type, extensions in file_type_map.items():
            if ext in extensions:
                return file_type, ext
        return "generic", ext
    else: # Old format
        return file_type_map.get(ext, 'generic'), ext

async def _handle_content_deduplication(job: ProcessingJob, file_hash: str, file_size: int, is_webready: bool, sanitized_path_for_db: str) -> bool:
    """
    Handles content deduplication and race conditions for processing.

    Returns:
        bool: True if the content was already processed or an instance was created,
              indicating the caller should stop. False if the caller has claimed the
              hash and should proceed with full processing.
    """
    while True:
        # Check 1: Is content already in DB? (Fastest check)
        if await get_content_by_hash_async(file_hash):
            logger.info(f"Content for '{job.original_filename}' (hash: {file_hash[:10]}...) already exists in DB. Creating instance only.")
            file_type_map = job.app_state["config"].get("FILE_TYPE_MAPPINGS", {})
            file_type, extension = get_file_type_and_extension(job.original_filename, file_type_map)
            media_data = MediaData(
                file_name=job.original_filename, initial_physical_path=job.path,
                sanitized_content_path=sanitized_path_for_db, file_hash=file_hash,
                size_in_bytes=file_size, file_type=file_type, extension=extension, is_webready=is_webready,
                ingest_source=job.ingest_source,
                original_created_at=datetime.fromtimestamp(os.path.getctime(job.path)).isoformat(),
                original_modified_at=datetime.fromtimestamp(os.path.getmtime(job.path)).isoformat(),
                uploader_id=job.uploader_id, visibility_roles=job.default_visibility_roles or []
            )
            await process_media(media_data)
            # Increment session counter for creating a new instance, even if content is a duplicate.
            async with job.app_state['session_stats_lock']:
                job.app_state['files_processed_session'] += 1
            return True # Indicates processing is done

        # Check 2: Is content being processed by another worker?
        async with job.app_state['processing_lock']:
            if file_hash in job.app_state['processing_hashes']:
                logger.info(f"Content hash for '{job.original_filename}' is being processed by another worker. Will re-check in 5s.")
            else:
                logger.info(f"Claiming hash {file_hash[:10]}... for full processing of '{job.original_filename}'.")
                job.app_state['processing_hashes'].add(file_hash)
                return False # Indicates we should proceed with processing
        
        await asyncio.sleep(5)

def handle_processing_errors(media_type: str):
    """Decorator factory to handle common exceptions during media processing."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except (FileExistsError, sqlite3.IntegrityError):
                logger.debug("Content hash already exists. Skipping.")
            except (UnidentifiedImageError, OSError, IOError) as e:
                logger.info(f"Not a valid {media_type} file or I/O error, skipping: {e}")
            except asyncio.TimeoutError as e:
                logger.info(f"Video processing failed for {media_type}, skipping: {e}")
            except Exception as e:
                logger.exception(f"An unexpected error occurred during {media_type} processing: {e}")
        return wrapper
    return decorator


async def _extract_image_specific_data(job: ProcessingJob, loop: asyncio.AbstractEventLoop, on_progress: callable) -> dict:
    """Extracts image-specific metadata."""
    on_progress(0, "Opening image")

    max_dim = CONFIG.get('MAX_IMAGE_DIMENSION', 4096)
    open_func = functools.partial(_open_and_resize_image_if_needed, path=job.path, max_dimension=max_dim)
    img_pil = await loop.run_in_executor(job.executor, open_func)

    with img_pil:
        img_rgb = await loop.run_in_executor(job.executor, img_pil.convert, "RGB")
    
    proc_func = functools.partial(
        extract_image_data_sync, img_rgb=img_rgb, file_name=job.path,
        should_tag=CONFIG.get('AUTOMATICALLY_TAG_IMG'),
        tagger=job.tagger, clip_model=job.clip_model, clip_preprocess=job.clip_preprocess,
        on_progress=on_progress
    )
    image_tags, p_hash, clip_embedding, transcript, metadata, created_at, modified_at = await loop.run_in_executor(job.executor, proc_func)

    data = {
        "tags_list": image_tags or [],
        "phash": p_hash,
        "clip_embedding": clip_embedding,
        "transcript": transcript,
        "metadata": metadata,
        "model_name": job.tagger_model_name if CONFIG.get('AUTOMATICALLY_TAG_IMG') else None,
        "clip_model_name": job.clip_model_name,
    }
    # If EXIF data provided a creation/modification date, use it.
    # This will override the filesystem-based dates later in the pipeline.
    if created_at:
        data["original_created_at"] = created_at
    if modified_at:
        data["original_modified_at"] = modified_at
        
    return data

async def _extract_video_specific_data(job: ProcessingJob, loop: asyncio.AbstractEventLoop, on_progress: callable) -> dict:
    """Extracts video-specific metadata."""
    # --- NEW: Handle converted videos by copying scenes instead of re-analyzing ---
    original_content_id = job.context.get('original_content_id')
    if job.ingest_source == 'conversion' and original_content_id:
        logger.info(f"'{job.original_filename}' is a conversion. Copying scenes from original content ID: {original_content_id}")
        on_progress(10, "Copying scene data...")

        from src.media.HelperMedia import get_scenes_for_content
        from src.media.utilsVideo import get_video_duration
        import tempfile
        from src.media.thumbnails import create_video_thumbnail
        import torch

        DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

        # 1. Copy scene data from the original content record
        scenes_data = await get_scenes_for_content(original_content_id)

        # 2. Get duration and metadata for the NEW converted file
        on_progress(30, "Getting new file metadata...")
        duration = await get_video_duration(job.path)
        metadata_proc_func = functools.partial(extract_video_metadata_sync, path=job.path)
        metadata, created_at, modified_at = await loop.run_in_executor(job.executor, metadata_proc_func)

        data = {
            "scenes_data": scenes_data,
            "duration_seconds": duration,
            "metadata": metadata,
            "model_name": None, # No new AI tagging was performed on scenes
            "clip_model_name": job.clip_model_name, # The model name is needed for the new content embedding
        }
        if created_at: data["original_created_at"] = created_at
        if modified_at: data["original_modified_at"] = modified_at

        # 3. Generate a new CLIP embedding for the overall converted file content
        on_progress(60, "Generating new content embedding...")
        with tempfile.NamedTemporaryFile(suffix=".png", delete=True) as temp_thumb:
            if create_video_thumbnail(job.path, temp_thumb.name):
                img_pil = Image.open(temp_thumb.name)
                with img_pil:
                    img_rgb = await loop.run_in_executor(job.executor, img_pil.convert, "RGB")
                image_input = job.clip_preprocess(img_rgb).unsqueeze(0).to(DEVICE)
                with torch.no_grad():
                    features = job.clip_model.encode_image(image_input).cpu()
                    features /= features.norm(dim=-1, keepdim=True)
                data["clip_embedding"] = features.numpy().tobytes()
        return data

    # --- Fallback to original logic for non-conversion jobs ---
    proc_func = functools.partial(
        extract_video_scene_data_hybrid, video_path=job.path,
        tagger=job.tagger, clip_model=job.clip_model, clip_preprocess=job.clip_preprocess,
        should_tag=CONFIG.get('AUTOMATICALLY_TAG_VIDEOS'),
        on_progress=on_progress
    )
    scenes_data, duration = await loop.run_in_executor(job.executor, proc_func)

    # Separately, extract metadata like creation time using ffprobe
    metadata_proc_func = functools.partial(extract_video_metadata_sync, path=job.path)
    metadata, created_at, modified_at = await loop.run_in_executor(job.executor, metadata_proc_func)
    data = {
        "scenes_data": scenes_data,
        "duration_seconds": duration,
        "metadata": metadata,
        "model_name": job.tagger_model_name if CONFIG.get('AUTOMATICALLY_TAG_VIDEOS') else None,
        "clip_model_name": job.clip_model_name,
    }

    # If ffprobe found a creation date, use it.
    if created_at:
        data["original_created_at"] = created_at
    if modified_at: # This is unlikely to be set, but handle it just in case
        data["original_modified_at"] = modified_at

    return data

async def _extract_text_specific_data(job: ProcessingJob, loop: asyncio.AbstractEventLoop, on_progress: callable) -> dict:
    """Extracts text-specific metadata."""
    on_progress(50, "Reading snippet")
    def read_snippet():
        with open(job.path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read(1000)
    snippet = await loop.run_in_executor(job.executor, read_snippet)
    return {"transcript": snippet}

async def _extract_generic_specific_data(job: ProcessingJob, loop: asyncio.AbstractEventLoop, on_progress: callable) -> dict:
    """Extracts generic file metadata (which is none)."""
    return {}


# ==============================================================================
# 3. GENERIC PROCESSING ORCHESTRATOR
# ==============================================================================

async def _process_new_content_with_strategy(job: ProcessingJob, extraction_strategy: callable):
    """
    A generic worker that handles all the boilerplate for processing new content.
    It calculates hashes, handles deduplication, and manages processing locks.
    """
    loop = asyncio.get_running_loop()
    job_id = job.path
    try:
        status_manager.start_processing(
            job_id,
            "Starting...",
            ingest_source=job.ingest_source,
            priority=job.priority.name,
            filename=job.original_filename
        )
        
        status_manager.update_progress(job_id, 5, "Calculating hash...")
        file_size = os.path.getsize(job.path)
        file_hash = await loop.run_in_executor(job.executor, calculate_file_hash, job.path)
        file_type_map = job.app_state["config"].get("FILE_TYPE_MAPPINGS", {})
        # Use original_filename for type detection as job.path may not have an extension (e.g., uploads)
        file_type, extension = get_file_type_and_extension(job.original_filename, file_type_map)

        # Determine the logical path for folder assignment. For uploads/conversions,
        # this creates a virtual path in a default folder, so they don't get
        # assigned to the temporary '_incoming' directory.
        sanitized_path_for_db = job.path
        if job.ingest_source in ['upload', 'conversion']:
            default_create_dir = job.app_state["config"].get('DEFAULT_CREATE_FOLDER')
            if default_create_dir:
                sanitized_path_for_db = os.path.join(default_create_dir, job.original_filename)
            else:
                # Fallback if default create folder isn't set
                watched_dirs = job.app_state["config"].get('WATCH_DIRS', [])
                if watched_dirs:
                    sanitized_path_for_db = os.path.join(watched_dirs[0], job.original_filename)

        is_webready = True
        if file_type == 'video':
            is_webready = await is_video_codec_compatible(job.path)
        elif file_type in ['generic', 'pdf']:
            is_webready = False

        status_manager.update_progress(job_id, 10, "Checking for duplicates...")
        if await _handle_content_deduplication(job, file_hash, file_size, is_webready=is_webready, sanitized_path_for_db=sanitized_path_for_db):
            status_manager.set_completed(job_id, "Duplicate content, instance created.")
            return

        job.app_state['currently_processing'].add(job.path)
        try:
            logger.info(f"New content for '{job.original_filename}' (hash: {file_hash[:10]}...). Performing full processing.")
            
            def on_progress(progress, message):
                # Scale progress from extraction (0-100) to a portion of the total job (10-90)
                scaled_progress = 10 + (progress * 0.85)
                status_manager.update_progress(job_id, scaled_progress, message)

            extra_data = await extraction_strategy(job, loop, on_progress)

            status_manager.update_progress(job_id, 95, "Writing to database...")
            default_data = {
                'file_name': job.original_filename,
                'initial_physical_path': job.path,
                'sanitized_content_path': sanitized_path_for_db,
                'file_hash': file_hash,
                'size_in_bytes': file_size,
                'file_type': file_type,
                'extension': extension,
                'is_webready': is_webready,
                'ingest_source': job.ingest_source,
                'original_created_at': datetime.fromtimestamp(os.path.getctime(job.path)).isoformat(),
                'original_modified_at': datetime.fromtimestamp(os.path.getmtime(job.path)).isoformat(),
                'uploader_id': job.uploader_id,
                'visibility_roles': job.default_visibility_roles or [],
            }
            default_data.update(extra_data)
            media_data = MediaData(**default_data)
            await process_media(media_data)

            async with job.app_state['session_stats_lock']:
                job.app_state['files_processed_session'] += 1
            
            status_manager.set_completed(job_id, "Processing complete.")
        finally:
            job.app_state['currently_processing'].discard(job.path)
            async with job.app_state['processing_lock']:
                if file_hash in job.app_state['processing_hashes']:
                    job.app_state['processing_hashes'].remove(file_hash)
                    logger.info(f"Releasing hash {file_hash[:10]}... after processing '{job.original_filename}'.")
    except Exception as e:
        logger.exception(f"Error processing {job.path}: {e}")
        status_manager.set_error(job_id, str(e))
        async with job.app_state['processing_lock']:
            if file_hash in job.app_state['processing_hashes']:
                job.app_state['processing_hashes'].remove(file_hash)
                logger.info(f"Releasing hash {file_hash[:10]}... after processing '{job.original_filename}'.")


# ==============================================================================
# 4. ASYNCHRONOUS MEDIA PROCESSING TASKS
# ==============================================================================

@handle_processing_errors('image')
async def process_image_task(job: ProcessingJob):
    """Processes a single image file using the generic content processor."""
    await _process_new_content_with_strategy(job, _extract_image_specific_data)

@handle_processing_errors('video')
async def process_video_task(job: ProcessingJob):
    """Processes a single video file using the generic content processor."""
    await _process_new_content_with_strategy(job, _extract_video_specific_data)

@handle_processing_errors('text')
async def process_text_task(job: ProcessingJob):
    """Processes a single text file using the generic content processor."""
    await _process_new_content_with_strategy(job, _extract_text_specific_data)

@handle_processing_errors('generic')
async def process_generic_task(job: ProcessingJob):
    """Processes a generic file using the generic content processor."""
    await _process_new_content_with_strategy(job, _extract_generic_specific_data)


# ==============================================================================
# 5. UPDATE TASK
# ==============================================================================
async def update_media_task(job: ProcessingJob):
    """
    Handles a modified file from a watch directory using a "delete and re-create" strategy.
    """
    logger.info(f"Processing modification for file: {job.path}")
    loop = asyncio.get_running_loop()

    # 1. Get existing instance details from the database using the file path.
    existing_instance = await get_file_details_by_path_async(job.path)
    if not existing_instance:
        logger.warning(f"File '{job.path}' marked for update, but not in DB. Processing as new.")
        file_type_map = job.app_state["config"].get("FILE_TYPE_MAPPINGS", {})
        file_type, _ = get_file_type_and_extension(job.path, file_type_map)
        if file_type == 'image': await process_image_task(job)
        elif file_type == 'video': await process_video_task(job)
        elif file_type == 'text': await process_text_task(job)
        elif file_type == 'generic': await process_generic_task(job)
        return

    # 2. Calculate the new hash of the modified file.
    new_hash = await loop.run_in_executor(job.executor, calculate_file_hash, job.path)

    # 3. If the hash is the same, do nothing (e.g., only metadata/timestamp changed).
    old_hash = existing_instance.get('file_hash')
    if new_hash == old_hash:
        logger.info(f"File '{job.path}' modified, but content hash is unchanged. No update needed.")
        return

    logger.info(f"Content hash has changed for '{job.path}'. Re-ingesting...")

    # 4. Delete the old instance record and its associated thumbnail.
    instance_id = existing_instance.get('id')
    deleted = await delete_instance_and_thumbnail_async(instance_id, old_hash)
    if not deleted:
        logger.error(f"Failed to delete old instance {instance_id} for path {job.path}. Aborting update.")
        return

    # 5. Process the file as a completely new ingestion.
    file_type_map = job.app_state["config"].get("FILE_TYPE_MAPPINGS", {})
    file_type, _ = get_file_type_and_extension(job.path, file_type_map)
    logger.info(f"Old instance {instance_id} deleted. Submitting '{job.path}' for new processing as type '{file_type}'.")
    if file_type == 'image':
        await process_image_task(job)
    elif file_type == 'video':
        await process_video_task(job)
    elif file_type == 'text':
        await process_text_task(job)
    elif file_type == 'generic':
        await process_generic_task(job)
    else:
        logger.warning(f"Unhandled file type '{file_type}' for update at '{job.path}'.")