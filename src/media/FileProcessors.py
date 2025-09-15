# FileProcessors.py
"""
Handles the detection and processing of media files.

This module is responsible for the "extract" part of the ETL (Extract, Transform, Load) process.
It uses Watchdog to monitor directories, sanitizes files, and then extracts all relevant
metadata, hashes, tags, and embeddings. This extracted information is packaged into a
MediaData object and passed to the MediaDB module for storage and database insertion.
"""
import asyncio
import logging
import os
import sqlite3
import hashlib
import tempfile
import shutil
import functools
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

# --- Core Libraries ---
from PIL import Image, UnidentifiedImageError, ImageSequence
import numpy as np
import cv2
import imagehash
import torch
from tqdm import tqdm
from PIL.ExifTags import TAGS
from PIL.TiffImagePlugin import IFDRational


# --- Custom Modules ---
import OCR # This module now has an async get_textgrouper function
from src.media.actions import process_media
from src.models import MediaData
from utils import truncate_string, get_original_filename
from ConfigMedia import get_config
from ImageTagger import Tagger
from Searcher import get_searcher

from src.media.HelperMedia import delete_file_async, extract_image_data_sync, extract_video_scene_data, get_file_details, get_file_details_async, get_file_details_by_hash_async, get_hashes_by_size_async
from ImageTagger import Tagger

# --- Initialization ---

# Define the global device for Torch models once
CONFIG = get_config()

# Use Python's standard logging module
logger = logging.getLogger("FileProcessor")
logger.setLevel(logging.DEBUG)





# ==============================================================================
# 1. UTILITY AND DECORATOR FUNCTIONS
# ==============================================================================

def _get_file_type_and_extension(path: str) -> tuple[str, str]:
    """Determines file type and returns a canonical type and extension."""
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
        return "image", ext
    if ext in ['.mp4', '.mkv', '.avi', '.mov', '.webm']:
        return "video", ".mkv" # Standardize to MKV for storage
    if ext == '.pdf':
        return "pdf", ext
    return "unknown", ext





def _save_animated_image(image_obj: Image.Image, save_path: str):
    """
    Saves an animated image (like a GIF) by iterating through each frame,
    preserving individual frame durations and loop settings for accurate playback.
    """
    frames = []
    durations = []
    for frame in ImageSequence.Iterator(image_obj):
        duration = frame.info.get('duration', 100)
        durations.append(duration)
        frames.append(frame.copy())
    
    if not frames:
        return

    frames[0].save(
        save_path,
        format='GIF',
        save_all=True,
        append_images=frames[1:] if len(frames) > 1 else None,
        duration=durations,
        loop=image_obj.info.get('loop', 0),
        disposal=2
    )

def calculate_file_hash(filepath: str) -> str:
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

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
            except asyncio.TimeoutError as e: # <-- REMOVED ffmpeg.Error
                logger.info(f"Video processing failed for {media_type}, skipping: {e}")
            except Exception:
                logger.exception(f"An unexpected error occurred during {media_type} processing")
        return wrapper
    return decorator




# ==============================================================================
# 3. ASYNCHRONOUS MEDIA PROCESSING TASKS
# ==============================================================================

@handle_processing_errors('image')
async def process_image_task(executor: ThreadPoolExecutor, path: str, ingest_source: str, tagger_model_name: str, clip_model_name: str, tagger: Tagger, clip_model, clip_preprocess, uploader_id: Optional[int] = None, default_visibility_roles: Optional[List[int]] = None):
    """
    Processes a single image file with a multi-level duplicate check, including a quality check.
    """
    loop = asyncio.get_running_loop()
    temp_dir = None
    config = get_config()

    try:
        # --- LEVEL 1: File Size Check ---
        file_size = os.path.getsize(path)
        possible_hashes = await get_hashes_by_size_async(file_size)

        # --- LEVEL 2: Full Hash Check & Quality Upgrade Logic ---
        if possible_hashes:
            original_file_hash = await loop.run_in_executor(executor, calculate_file_hash, path)
            if original_file_hash in possible_hashes:
                logger.warning(f"DUPLICATE (strict): Hash {original_file_hash[:12]} already exists. Checking for quality upgrade...")
                
                # Get details of the existing file
                existing_file_details = await get_file_details_by_hash_async(original_file_hash)
                
                # Get details of the new file
                with Image.open(path) as new_img:
                    new_resolution = new_img.width * new_img.height
                
                old_meta = existing_file_details.get('metadata', {})
                old_resolution = old_meta.get('width', 0) * old_meta.get('height', 0)

                
                if new_resolution > old_resolution:
                    if config.get('DELETE_PERCEPTUAL_LOWER_QUALITY_DUPLICATE'):
                        logger.info(f"QUALITY UPGRADE: New image ({new_resolution}px) is higher resolution than existing ({old_resolution}px). Replacing old file.")
                        
                        await delete_file_async(existing_file_details['id'])
                        # Let processing continue to ingest the new file
                else:
                    logger.info("NO UPGRADE: New image is not higher quality. Discarding.")
                    #if ingest_source == "upload":
                        #os.remove(path) 
                    
                    return # Stop processing
        
        # If we pass, proceed with full processing. First, sanitize the image.
        temp_dir = tempfile.mkdtemp(prefix="img_proc_")
        sanitized_path = os.path.join(temp_dir, "sanitized_image.webp") # Standardize to webp
        
        img_pil = await loop.run_in_executor(executor, Image.open, path)
        with img_pil:
            # Save a sanitized version (e.g., as WebP)
            await loop.run_in_executor(executor, lambda: img_pil.save(sanitized_path, format='WEBP', quality=95))
            img_rgb = await loop.run_in_executor(executor, img_pil.convert, "RGB")
        
        sanitized_hash = await loop.run_in_executor(executor, calculate_file_hash, sanitized_path)

        # --- Data Extraction ---
        proc_func = functools.partial(
            extract_image_data_sync,
            img_rgb=img_rgb, file_name=path,
            should_tag=config.get('AUTOMATICALLY_TAG_IMG'),
            tagger=tagger, clip_model=clip_model, clip_preprocess=clip_preprocess
        )
        image_tags, p_hash, clip_embedding, transcript, metadata = await loop.run_in_executor(executor, proc_func)
        
        # --- LEVEL 3: Perceptual Duplicate Check & Quality Check ---
        if config.get('ENABLE_PERCEPTUAL_DUPLICATE_CHECK'):
            try:
                searcher = await get_searcher()
                faiss_index = searcher.faiss_index
                faiss_id_map = searcher.faiss_to_db_id

                if faiss_index and faiss_index.ntotal > 0:
                    embedding_np = np.frombuffer(clip_embedding, dtype=np.float32).reshape(1, -1)
                    distances, indices = faiss_index.search(embedding_np, k=1)
                    
                    nearest_distance = distances[0][0]
                    threshold = config.get('PERCEPTUAL_DUPLICATE_THRESHOLD')

                    if nearest_distance < threshold:
                        db_id_str = faiss_id_map[str(indices[0][0])]
                        similar_file_details = await get_file_details_async(int(db_id_str))
                        
                        if similar_file_details:
                            logger.warning(f"DUPLICATE (perceptual): Image is visually similar to file ID {db_id_str} (distance: {nearest_distance:.4f}). Checking quality...")
                            
                            new_resolution = metadata.get('width', 0) * metadata.get('height', 0)
                            old_meta = similar_file_details.get('metadata', {})
                            old_resolution = old_meta.get('width', 0) * old_meta.get('height', 0)

                            if new_resolution <= old_resolution:
                                logger.info("NO UPGRADE: New image is not of higher quality than the existing similar one. Discarding.")
                                #if ingest_source == "upload":
                                    #os.remove(path)
                                return # Stop processing
                            else:
                                logger.info("HIGHER QUALITY: New image is of higher quality. Proceeding with ingestion.")
            except Exception as e:
                logger.error(f"Perceptual duplicate check for image failed: {e}")

        # --- Populate DTO and Ingest ---
        file_type, extension = "image", ".webp"
        media_data = MediaData(
            file_name=get_original_filename(path),
            sanitized_content_path=sanitized_path,
            file_hash=sanitized_hash,
            size_in_bytes=file_size,
            file_type=file_type,
            extension=extension,
            tags_list=image_tags or [],
            phash=p_hash,
            clip_embedding=clip_embedding,
            transcript=transcript,
            metadata=metadata,
            model_name=tagger_model_name if config.get('AUTOMATICALLY_TAG_IMG') else None,
            clip_model_name=clip_model_name,
            ingest_source=ingest_source,
            original_created_at=datetime.fromtimestamp(os.path.getctime(path)).isoformat(),
            original_modified_at=datetime.fromtimestamp(os.path.getmtime(path)).isoformat(),
            uploader_id=uploader_id,
            visibility_roles=default_visibility_roles or []
        )
        
        await process_media(media_data)

        if config.get('DELETE_ON_PROCESS'):
            os.remove(path)

    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@handle_processing_errors('video')
async def process_video_task(executor: ThreadPoolExecutor, path: str, ingest_source: str, tagger_model_name: str, clip_model_name: str, tagger: Tagger, clip_model, clip_preprocess, uploader_id: Optional[int] = None, default_visibility_roles: Optional[List[int]] = None):
    """
    Processes a single video file with a multi-level duplicate check strategy.
    """
    loop = asyncio.get_running_loop()
    temp_dir = None
    config = get_config()
    
    try:
        original_file_hash = None
        
        # --- LEVEL 1: File Size Check (Fast Filter) ---
        file_size = os.path.getsize(path)
        possible_hashes = await get_hashes_by_size_async(file_size)

        # --- LEVEL 2: Full Hash Check (Strict, if necessary) ---
        if possible_hashes:
            logger.info(f"Found {len(possible_hashes)} existing file(s) with the same size. Performing full hash check.")
            original_file_hash = await loop.run_in_executor(executor, calculate_file_hash, path)
            
            if original_file_hash in possible_hashes:
                logger.warning(f"DUPLICATE (strict): File is byte-for-byte identical to an existing file. Hash: {original_file_hash[:12]}... Skipping.")
                
                #if ingest_source == "upload":
                   # os.remove(path)
                
                return

        # If we pass, proceed. Calculate original hash if not already done.
        if original_file_hash is None:
            original_file_hash = await loop.run_in_executor(executor, calculate_file_hash, path)

        temp_dir = tempfile.mkdtemp(prefix="vid_proc_")
        
        logger.info("Sanitizing video file...")
        sanitized_path = os.path.join(temp_dir, "sanitized_video.mkv")
        command = [
            'ffmpeg', '-y', '-i', path,
            '-c', 'copy',
            '-map', '0:v?',
            '-map', '0:a?',
            '-map', '0:s?',
            '-map_chapters', '0',
            sanitized_path
        ]
        process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise Exception(f"FFmpeg error: {stderr.decode('utf-8', errors='ignore').strip()}")

        # --- Scene Data Extraction ---
        logger.debug("Detecting and processing scenes...")
        proc_func = functools.partial(
            extract_video_scene_data,
            video_path=sanitized_path, tagger=tagger, clip_model=clip_model, clip_preprocess=clip_preprocess,
            should_tag=config.get('AUTOMATICALLY_TAG_VIDEOS')
        )
        scenes_data, duration = await loop.run_in_executor(executor, proc_func)
        
        # --- LEVEL 3: Perceptual Duplicate Check (Optional) ---
        if config.get('ENABLE_PERCEPTUAL_DUPLICATE_CHECK') and scenes_data:
            try:
                searcher = await get_searcher()
                faiss_index = searcher.faiss_index
                faiss_id_map = searcher.faiss_to_db_id

                if faiss_index and faiss_index.ntotal > 0:
                    scene_embeddings = [np.frombuffer(s['embedding'], dtype=np.float32) for s in scenes_data if 'embedding' in s]
                    if scene_embeddings:
                        global_embedding = np.mean(scene_embeddings, axis=0, dtype=np.float32).reshape(1, -1)
                        distances, indices = faiss_index.search(global_embedding, k=1)
                        
                        nearest_distance = distances[0][0]
                        db_id_str = faiss_id_map[str(indices[0][0])]
                        threshold = config.get('PERCEPTUAL_DUPLICATE_THRESHOLD')
                        
                        if nearest_distance < threshold:
                            logger.warning(f"DUPLICATE (perceptual): Video is visually similar to file ID {db_id_str} (distance: {nearest_distance:.4f}). Ingesting anyway as quality check is complex for videos.")
            except Exception as e:
                logger.error(f"Perceptual duplicate check for video failed: {e}")

        # --- Populate DTO and Ingest ---
        file_type, extension = _get_file_type_and_extension(sanitized_path)
        media_data = MediaData(
            file_name=get_original_filename(path),
            sanitized_content_path=sanitized_path,
            file_hash=original_file_hash,
            size_in_bytes=file_size,
            file_type=file_type,
            extension=extension,
            scenes_data=scenes_data,
            duration_seconds=duration,
            model_name=tagger_model_name if config.get('AUTOMATICALLY_TAG_VIDEOS') else None,
            clip_model_name=clip_model_name,
            ingest_source=ingest_source,
            original_created_at=datetime.fromtimestamp(os.path.getctime(path)).isoformat(),
            original_modified_at=datetime.fromtimestamp(os.path.getmtime(path)).isoformat(),
            uploader_id=uploader_id,
            visibility_roles=default_visibility_roles or []
        )
        await process_media(media_data)

        if config.get('DELETE_ON_PROCESS'):
            os.remove(path)
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)