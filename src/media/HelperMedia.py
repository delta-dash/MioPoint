# HelperMedia.py
import asyncio
import json
import os
import shutil
import aiosqlite
import tempfile
from typing import Any, Dict, List, Optional

from pypdf import PdfReader


from ConfigMedia import get_config
from EncryptionManager import decrypt_to_memory
from ImageTagger import Tagger
from src.db_utils import AsyncDBContext, execute_db_query
from src.media.thumbnails import THUMBNAIL_FORMAT, create_image_thumbnail, create_pdf_thumbnail, create_video_thumbnail
import logging
from src.roles.rbac_manager import get_roles_for_user
from utils import truncate_string

logger = logging.getLogger("HelperMedia")
logger.setLevel(logging.DEBUG)
config = get_config()


async def can_user_view_file(user_id: int, file_id: int, conn: aiosqlite.Connection) -> bool:
    """Checks if a user has access to a file via their roles in the database."""
    query = """
        SELECT 1 FROM file_visibility_roles fvr
        JOIN user_roles ur ON fvr.role_id = ur.role_id
        WHERE ur.user_id = ? AND fvr.file_id = ?
        LIMIT 1
    """
    result = await execute_db_query(conn, query, (user_id, file_id), fetch_one=True)
    return result is not None

async def get_cards_with_cursor(
    user_id: int,
    search: Optional[str] = None,
    filename: Optional[str] = None,
    tags: Optional[str] = None,
    exclude_tags: Optional[str] = None,
    meta_tags: Optional[str] = None,
    exclude_meta_tags: Optional[str] = None,
    
    # --- MODIFIED LINE ---
    # The function now accepts a list of strings
    file_type: Optional[List[str]] = None,
    
    is_encrypted: Optional[bool] = None,
    duration_min: Optional[int] = None,
    duration_max: Optional[int] = None,
    page_size: int = 24,
    cursor: Optional[str] = None,
    conn: Optional[aiosqlite.Connection] = None
) -> Dict[str, Any]:
    """
    Search and paginate files with advanced filtering and role-based visibility.
    The database query itself enforces that only files visible to the user's roles are returned.
    """
    params: List[Any] = [user_id]
    # The query joins against visibility and user roles to filter results at the database level.
    base_query = """
        SELECT DISTINCT
            f.id, f.file_name, f.file_type, f.is_encrypted, f.file_hash,
            f.duration_seconds, f.original_created_at, strftime('%s', f.discovered_at) * 1.0 as timestamp
        FROM files f
        JOIN file_visibility_roles fvr ON f.id = fvr.file_id
        JOIN user_roles ur ON fvr.role_id = ur.role_id
    """
    conditions = ["ur.user_id = ?"]  # Start with the mandatory visibility check

    if cursor:
        try:
            last_timestamp, last_id = map(float, cursor.split(':', 1))
            conditions.append("(timestamp < ? OR (timestamp = ? AND f.id < ?))")
            params.extend([last_timestamp, last_timestamp, int(last_id)])
        except (ValueError, IndexError):
            logger.warning(f"Malformed cursor received: {cursor}")

    # Standard filters
    if search: conditions.append("(f.file_name || ' ' || COALESCE(f.transcript, '') || ' ' || COALESCE(f.metadata, '')) LIKE ?"); params.append(f"%{search}%")
    if filename: conditions.append("f.file_name LIKE ?"); params.append(f"%{filename}%")
    if file_type: # Checks if the list is not None and not empty
        placeholders = ','.join(['?'] * len(file_type))
        conditions.append(f"f.file_type IN ({placeholders})")
        params.extend(file_type)

    # Regular Tag filters
    if tags:
        tag_list = [t.strip().lower() for t in tags.split(',') if t.strip()]
        if tag_list:
            p = ','.join(['?']*len(tag_list)); conditions.append(f"f.id IN (SELECT ft.file_id FROM file_tags ft JOIN tags t ON ft.tag_id=t.id WHERE t.name IN ({p}) GROUP BY ft.file_id HAVING COUNT(DISTINCT t.id)=?)"); params.extend(tag_list); params.append(len(tag_list))
    if exclude_tags:
        ex_list = [t.strip().lower() for t in exclude_tags.split(',') if t.strip()]
        if ex_list: p = ','.join(['?']*len(ex_list)); conditions.append(f"f.id NOT IN (SELECT ft.file_id FROM file_tags ft JOIN tags t ON ft.tag_id=t.id WHERE t.name IN ({p}))"); params.extend(ex_list)

    # Meta Tag Filters
    if meta_tags:
        meta_tag_list = [mt.strip().lower() for mt in meta_tags.split(',') if mt.strip()]
        if meta_tag_list:
            p = ','.join(['?'] * len(meta_tag_list))
            subquery = f"f.id IN (SELECT fmt.file_id FROM file_meta_tags fmt JOIN meta_tags mt ON fmt.tag_id = mt.id WHERE mt.name IN ({p}) GROUP BY fmt.file_id HAVING COUNT(DISTINCT mt.id) = ?)"
            conditions.append(subquery); params.extend(meta_tag_list); params.append(len(meta_tag_list))
    if exclude_meta_tags:
        exclude_meta_list = [mt.strip().lower() for mt in exclude_meta_tags.split(',') if mt.strip()]
        if exclude_meta_list:
            p = ','.join(['?'] * len(exclude_meta_list))
            subquery = f"f.id NOT IN (SELECT fmt.file_id FROM file_meta_tags fmt JOIN meta_tags mt ON fmt.tag_id = mt.id WHERE mt.name IN ({p}))"
            conditions.append(subquery); params.extend(exclude_meta_list)

    # Other filters
    if duration_min is not None: conditions.append("f.duration_seconds >= ?"); params.append(duration_min)
    if duration_max is not None: conditions.append("f.duration_seconds <= ?"); params.append(duration_max)
    if is_encrypted is not None: conditions.append("f.is_encrypted = ?"); params.append(1 if is_encrypted else 0)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    order_by_clause = "ORDER BY timestamp DESC, f.id DESC"
    limit_clause = "LIMIT ?"
    params.append(page_size)

    query = f"{base_query} {where_clause} {order_by_clause} {limit_clause}"

    async with AsyncDBContext(external_conn=conn) as db_conn:
        rows = await execute_db_query(db_conn, query, tuple(params), fetch_all=True)
        files_list = [dict(row) for row in rows]
        for file_dict in files_list:
            file_dict['display_name'] = file_dict['file_name']

    next_cursor = f"{files_list[-1]['timestamp']}:{files_list[-1]['id']}" if len(files_list) == page_size else None
    return {"items": files_list, "next_cursor": next_cursor}

async def get_tags_for_scene(scene_id: int, conn: Optional[aiosqlite.Connection] = None):
    """Retrieves all tags for a specific scene, including tags from the parent file."""
    query = """
        WITH scene_info(sid, fid) AS (SELECT id, file_id FROM scenes WHERE id = ?)
        SELECT DISTINCT t.name FROM tags t
        LEFT JOIN scene_tags st ON t.id = st.tag_id AND st.scene_id = (SELECT sid FROM scene_info)
        LEFT JOIN file_tags ft ON t.id = ft.tag_id AND ft.file_id = (SELECT fid FROM scene_info)
        WHERE st.scene_id IS NOT NULL OR ft.file_id IS NOT NULL ORDER BY t.name;
    """
    async with AsyncDBContext(external_conn=conn) as db_conn:
        rows = await execute_db_query(db_conn, query, (scene_id,), fetch_all=True)
        return [row['name'] for row in rows]

async def get_meta_tags_for_file(file_id: int, conn: Optional[aiosqlite.Connection] = None):
    """Retrieves all meta-tags associated with a specific file from the database."""
    query = """
        SELECT mt.name FROM meta_tags mt
        JOIN file_meta_tags fmt ON mt.id = fmt.tag_id
        WHERE fmt.file_id = ?
        ORDER BY mt.name;
    """
    async with AsyncDBContext(external_conn=conn) as db_conn:
        rows = await execute_db_query(db_conn, query, (file_id,), fetch_all=True)
        return [row['name'] for row in rows]
    
async def get_tags_for_file(file_path: str, conn: Optional[aiosqlite.Connection] = None):
    """Retrieves all tags associated with a specific file from the database."""
    query = "SELECT t.name FROM tags t JOIN file_tags ft ON t.id=ft.tag_id JOIN files f ON ft.file_id=f.id WHERE f.stored_path=? ORDER BY t.name;"
    async with AsyncDBContext(external_conn=conn) as db_conn:
        rows = await execute_db_query(db_conn, query, (file_path,), fetch_all=True)
        return [row['name'] for row in rows]

async def get_file_details(file_id: int, user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """
    Retrieves a complete, structured record for a file, but ONLY if the user has permission to view it.
    """
    try:
        async with AsyncDBContext(external_conn=conn) as db_conn:
            # First, perform the visibility check.
            if not await can_user_view_file(user_id, file_id, db_conn):
                logger.warning(f"Access Denied: User {user_id} attempted to view file {file_id}.")
                return None

            # User has access, proceed to fetch all details.
            file_row = await execute_db_query(db_conn, "SELECT * FROM files WHERE id = ?", (file_id,), fetch_one=True)
            if not file_row:
                return None
            details = dict(file_row)
            
            visibility_roles = await get_visibility_roles_for_file(file_id, user_id, conn=db_conn)
            details['visibility_roles'] = visibility_roles if visibility_roles is not None else []

            details['uploader'] = None
            if uploader_id := details.get('uploader_id'):
                uploader_info = await execute_db_query(db_conn, "SELECT id, username FROM users WHERE id = ?", (uploader_id,), fetch_one=True)
                if uploader_info:
                    uploader_roles = await get_roles_for_user(uploader_id, conn=db_conn)
                    details['uploader'] = {"id": uploader_info['id'], "username": uploader_info['username'], "roles": uploader_roles}

            details['tags'] = await get_tags_for_file(details['stored_path'], conn=db_conn) 
            details['meta_tags'] = await get_meta_tags_for_file(file_id, conn=db_conn)

            scene_rows = await execute_db_query(db_conn, "SELECT * FROM scenes WHERE file_id = ?", (file_id,), fetch_all=True)
            scenes_data = []
            for scene_row in scene_rows:
                scene_dict = dict(scene_row)
                scene_id = scene_dict['id']
                scene_dict['tags'] = await get_tags_for_scene(scene_id, conn=db_conn)
                scenes_data.append(scene_dict)
            
            details['scenes'] = scenes_data
            return details
            
    except aiosqlite.Error as e:
        logger.exception(f"Database error while fetching details for file_id {file_id}: {e}")
        return None
    except KeyError as e:
        logger.error(f"Schema mismatch: Missing expected column '{e}' while fetching details for file_id {file_id}.")
        return None

async def get_visibility_roles_for_file(file_id: int, user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[List[Dict]]:
    """
    Gets the list of roles assigned to a file's visibility, first checking
    if the requesting user is even allowed to see the file.
    """
    async with AsyncDBContext(external_conn=conn) as db_conn:
        # First, ensure the user can view the file at all.
        if not await can_user_view_file(user_id, file_id, db_conn):
            logger.warning(f"Access Denied: User {user_id} attempted to get visibility for file {file_id}.")
            return None # Return None to indicate access denied

        # If they can, get the list of roles.
        query = """
            SELECT r.id, r.name, r.rank FROM roles r
            JOIN file_visibility_roles fvr ON r.id = fvr.role_id
            WHERE fvr.file_id = ?
            ORDER BY r.rank, r.name
        """
        rows = await execute_db_query(db_conn, query, (file_id,), fetch_all=True)
        return [dict(row) for row in rows]
    
async def get_thumbnail_by_id(
    file_id: int,
    as_bytes: bool = False,
    password: Optional[str] = None, # <-- ADDED
    conn: Optional[aiosqlite.Connection] = None
) -> Optional[Dict[str, Any]]:
    """Retrieves/regenerates the thumbnail. Requires password for encrypted files."""
    if not isinstance(file_id, int) or file_id <= 0: return None
    try:
        async with AsyncDBContext(external_conn=conn) as db_conn:
            query = "SELECT file_hash, is_encrypted, stored_path, file_type FROM files WHERE id = ?"
            file_info = await execute_db_query(db_conn, query, (file_id,), fetch_one=True)
        
        if not file_info:
            return None
        file_hash, is_encrypted, stored_path, file_type = file_info['file_hash'], bool(file_info['is_encrypted']), file_info['stored_path'], file_info['file_type']

        # Pass the password to the synchronous logic handler
        return _get_and_regenerate_thumbnail_logic(file_hash, file_type, stored_path, is_encrypted, as_bytes, password)
    except aiosqlite.Error as e:
        logger.exception(f"Database error for file_id {file_id}: {e}")
        return None


def _regenerate_thumbnail(
    file_hash: str,
    file_type: str,
    stored_path: str,
    is_encrypted: bool,
    password: Optional[str] # <-- ADDED
) -> bool:
    """Regenerates a thumbnail, using the provided password for decryption."""
    logger.info(f"Regenerating thumbnail for hash {file_hash[:10]}...")
    source_for_thumb = stored_path
    temp_file_path = None
    try:
        if is_encrypted:
            # --- MODIFIED: Use the provided password ---
            if not password:
                logger.error("Password not provided for encrypted thumbnail regeneration.")
                return False
            
            decrypted_content = decrypt_to_memory(stored_path, password)
            if not decrypted_content:
                logger.warning(f"Decryption failed for thumbnail generation (hash: {file_hash[:10]}). Invalid password?")
                return False
            
            # Write to a temp file for the thumbnail creator functions to use
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(stored_path)[1]) as temp_file:
                temp_file.write(decrypted_content)
                temp_file_path = temp_file.name
            source_for_thumb = temp_file_path
        
        # This function now points to the decrypted temp file if needed
        create_thumbnail(source_for_thumb, file_hash, file_type)

        thumb_path = os.path.join(config.data['THUMBNAILS_DIR'], f"{file_hash}.{THUMBNAIL_FORMAT.lower()}")
        return os.path.exists(thumb_path)
    finally:
        # Clean up the temp file if one was created
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

def create_thumbnail(source_path: str, file_hash: str, file_type: str):
    thumb_filename = f"{file_hash}.{THUMBNAIL_FORMAT.lower()}"
    thumb_path = os.path.join(config.data['THUMBNAILS_DIR'], thumb_filename)
    if file_type == 'image': create_image_thumbnail(source_path, thumb_path)
    elif file_type == 'video': create_video_thumbnail(source_path, thumb_path)
    elif file_type == 'pdf': create_pdf_thumbnail(source_path, thumb_path)
    else: logger.info(f"Thumbnail generation not supported for type '{file_type}'")

def _get_and_regenerate_thumbnail_logic(
    file_hash: str,
    file_type: str,
    stored_path: str,
    is_encrypted: bool,
    as_bytes: bool = False,
    password: Optional[str] = None # <-- ADDED
) -> Optional[Dict[str, Any]]:
    """The main synchronous logic for getting or creating a thumbnail."""
    thumbnail_dir = os.path.abspath(config.data['THUMBNAILS_DIR'])
    thumb_path = os.path.join(thumbnail_dir, f"{file_hash}.{THUMBNAIL_FORMAT.lower()}")

    if not os.path.exists(thumb_path):
        # Pass the password to the regeneration function
        if not _regenerate_thumbnail(file_hash, file_type, stored_path, is_encrypted, password):
            logger.warning(f"Could not find or create thumbnail for hash {file_hash[:10]}")
            return None

    thumbnail_data = thumb_path
    if as_bytes:
        try:
            with open(thumb_path, 'rb') as f: thumbnail_data = f.read()
        except IOError as e:
            logger.exception(f"Error reading thumbnail file {thumb_path}: {e}")
            return None
            
    # Return is_encrypted status so the client knows what kind of file it belongs to.
    return {'data': thumbnail_data, 'is_encrypted': is_encrypted}

async def get_file_details_async(file_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[dict]:
    """Retrieves core details for a single file by its ID."""
    query = "SELECT id, file_hash, stored_path, metadata FROM files WHERE id = ?"
    async with AsyncDBContext(conn) as db_conn:
        row = await execute_db_query(db_conn, query, (file_id,), fetch_one=True)
    if not row:
        return None
    
    details = dict(row)
    try:
        details['metadata'] = json.loads(details['metadata']) if details['metadata'] else {}
    except (json.JSONDecodeError, TypeError):
        details['metadata'] = {} # Gracefully handle malformed JSON
    return details

async def get_file_details_by_hash_async(file_hash: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[dict]:
    """Retrieves core details for a single file by its hash."""
    query = "SELECT id, file_hash, stored_path, metadata FROM files WHERE file_hash = ?"
    async with AsyncDBContext(conn) as db_conn:
        row = await execute_db_query(db_conn, query, (file_hash,), fetch_one=True)
    if not row:
        return None
    
    details = dict(row)
    try:
        details['metadata'] = json.loads(details['metadata']) if details['metadata'] else {}
    except (json.JSONDecodeError, TypeError):
        details['metadata'] = {}
    return details

async def delete_file_async(file_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """
    Deletes a file record from the database and its associated physical files (stored file and thumbnail).
    Relies on ON DELETE CASCADE for related data cleanup in the DB.
    """
    logger.info(f"Attempting to delete file with ID: {file_id}")
    file_details = await get_file_details_async(file_id, conn)
    if not file_details:
        logger.warning(f"File with ID {file_id} not found in database. Cannot delete.")
        return False

    async with AsyncDBContext(conn) as db_conn:
        try:
            # The database transaction ensures that we only delete physical files if the DB record is also deleted.
            await execute_db_query(db_conn, "DELETE FROM files WHERE id = ?", (file_id,))
            logger.info(f"Successfully deleted database record for file ID {file_id}.")

            # If DB deletion is successful, proceed to delete physical files.
            # 1. Delete stored file
            stored_path = file_details.get('stored_path')
            if stored_path and os.path.exists(stored_path):
                os.remove(stored_path)
                logger.info(f"Deleted stored file: {stored_path}")

            # 2. Delete thumbnail
            thumbnail_dir = get_config().get('THUMBNAILS_DIR')
            file_hash = file_details.get('file_hash')
            thumb_path = os.path.join(thumbnail_dir, f"{file_hash}.{THUMBNAIL_FORMAT.lower()}")
            if os.path.exists(thumb_path):
                os.remove(thumb_path)
                logger.info(f"Deleted thumbnail: {thumb_path}")

            return True

        except Exception as e:
            logger.error(f"An error occurred during file deletion for ID {file_id}: {e}", exc_info=True)
            # The transaction will be rolled back by the context manager.
            return False

# ==============================================================================
# 2. SYNCHRONOUS DATA EXTRACTION HELPERS
# ==============================================================================



async def is_hash_in_db_async(file_hash: str, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """Checks if a file hash already exists in the database asynchronously."""
    query = "SELECT 1 FROM files WHERE file_hash = ? LIMIT 1"
    async with AsyncDBContext(conn) as db_conn:
        result = await execute_db_query(db_conn, query, (file_hash,), fetch_one=True)
    return result is not None

async def get_hashes_by_size_async(size_in_bytes: int, conn: Optional[aiosqlite.Connection] = None) -> List[str]:
    """
    Finds all file hashes for a given file size. 
    This is a fast first-pass check for duplicate files.
    """
    query = "SELECT file_hash FROM files WHERE size_in_bytes = ?"
    async with AsyncDBContext(conn) as db_conn:
        results = await execute_db_query(db_conn, query, (size_in_bytes,), fetch_all=True)
    return [row['file_hash'] for row in results] if results else []



# --- Metadata Extraction Helpers ---
def _extract_pdf_metadata(file_path):
    metadata = {}
    try:
        reader = PdfReader(file_path)
        info = reader.metadata
        if info:
            for key, value in info.items():
                metadata[key.replace('/', '')] = value
    except Exception as e:
        logger.warning(f"Could not extract PDF metadata from {file_path}: {e}")

    return metadata

# --- Scene Detection ---
import scenedetect
from scenedetect import open_video, SceneManager
from scenedetect.detectors import ContentDetector
import OCR
from PIL.ExifTags import TAGS
from PIL.TiffImagePlugin import IFDRational

# --- Core Libraries ---
from PIL import Image, UnidentifiedImageError, ImageSequence
import numpy as np
import cv2
import imagehash
import torch


from utils import truncate_string
from ImageTagger import Tagger

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

def _decode_exif_value(value):
    if isinstance(value, bytes): return value.decode('utf-8', errors='ignore')
    if isinstance(value, IFDRational): return float(value)
    return value

def _extract_image_metadata(path: str) -> dict:
    try:
        with Image.open(path) as img:
            exif_data = img.getexif()
            metadata = {TAGS.get(tag_id, tag_id): _decode_exif_value(value) for tag_id, value in exif_data.items()}
            metadata.update({'width': img.width, 'height': img.height, 'format': img.format})
            return metadata
    except Exception:
        return {}
    

def extract_image_data_sync(
    img_rgb: Image.Image,
    file_name: str,
    should_tag: bool,
    tagger: Tagger,
    clip_model,
    clip_preprocess,
) -> tuple:
    """
    A synchronous, CPU/GPU-bound function to extract all data from an image.
    It must receive all required objects as arguments.
    """
    # 1. Tags
    tags = []
    if should_tag:
        tags = tagger.tag(img_rgb, 0.3)
    
    # 2. Perceptual Hash
    p_hash = imagehash.phash(img_rgb)
    
    # 3. CLIP Embedding
    image_input = clip_preprocess(img_rgb).unsqueeze(0).to(DEVICE)
    with torch.no_grad():
        features = clip_model.encode_image(image_input)
        features /= features.norm(dim=-1, keepdim=True)
    embedding_bytes = features.cpu().numpy().tobytes()
    
    # 4. OCR Transcript
    transcript = None
    if "english text" in tags:
        logger.debug("-> Found 'english text', performing OCR...")
        img_np = np.array(img_rgb)
        grouped = OCR.get_textgrouper().group_text(
            image_data=img_np,
            start_direction=OCR.StartDirection.TOP_RIGHT,
        )
        group_texts = [" ".join(item['text'] for item in group) for group in grouped]
        transcript = "\n".join(group_texts)
        if transcript:
            logger.debug(f"  -> OCR: {truncate_string(transcript, 50)}")
            
    # 5. EXIF Metadata
    metadata = _extract_image_metadata(file_name)
    
    return tags, str(p_hash), embedding_bytes, transcript, metadata


def extract_video_scene_data(video_path: str, tagger: Tagger, clip_model, clip_preprocess, should_tag: bool, progress_cache: Optional[dict] = None, file_id: Optional[int] = None) -> tuple[list[dict], float | None]:
    """
    A synchronous, CPU/GPU-bound function to extract all data from video scenes.
    It must receive all required objects as arguments.
    """
    short_name = truncate_string(os.path.basename(video_path))
    logger.info(f"Starting scene processing for: {short_name}")

    video = open_video(video_path)
    duration_seconds = video.duration.get_seconds()
    logger.debug(f"Video duration: {duration_seconds:.2f} seconds")
    if should_tag:
        scene_manager = SceneManager()
        scene_manager.add_detector(ContentDetector())
        show_progress = logger.isEnabledFor(logging.DEBUG)

        logger.info("Phase 1: Detecting scenes...")
        scene_manager.detect_scenes(video=video, show_progress=show_progress)
        scene_list = scene_manager.get_scene_list()

        if not scene_list:
            logger.info(f"No scenes to process for {short_name}.")
            if progress_cache is not None and file_id is not None:
                progress_cache[file_id] = 100
            return [], duration_seconds
        
        logger.info(f"Phase 2: Extracting data from {len(scene_list)} scenes...")
        processed_scenes = []
        total_scenes = len(scene_list)
        
        for i, (start_time, end_time) in enumerate(scene_list):
            if progress_cache is not None and file_id is not None:
                progress = (i + 1) / total_scenes * 100
                progress_cache[file_id] = progress

            middle_frame_num = start_time.get_frames() + (end_time.get_frames() - start_time.get_frames()) // 2
            video.seek(middle_frame_num)
            frame_bgr = video.read()
            
            if frame_bgr is not None:
                frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
                pil_image = Image.fromarray(frame_rgb)

                # --- Extract all data for the scene's representative frame ---
                
                tags = tagger.tag(pil_image, 0.3)
                p_hash = imagehash.phash(pil_image)
                
                image_input = clip_preprocess(pil_image).unsqueeze(0).to(DEVICE)
                with torch.no_grad():
                    features = clip_model.encode_image(image_input)
                    features /= features.norm(dim=-1, keepdim=True)
                embedding_bytes = features.cpu().numpy().tobytes()
                
                transcript = None
                if "english text" in tags:
                    logger.debug("-> Found 'english text', performing OCR...")
                    grouped = OCR.get_textgrouper().group_text(
                        image_data=frame_rgb,
                        start_direction=OCR.StartDirection.TOP_RIGHT,
                    )
                    group_texts = [" ".join(item['text'] for item in group) for group in grouped]
                    transcript = "\n".join(group_texts)
                
                    if transcript: logger.debug(f"  -> OCR: {truncate_string(transcript, 50)}")
                
                processed_scenes.append({
                    "start": start_time.get_timecode(),
                    "end": end_time.get_timecode(),
                    "tags": tags,
                    "phash": str(p_hash),
                    "embedding": embedding_bytes,
                    "transcript": transcript
                })
        logger.info(f"Phase 2 complete. Finished processing {len(processed_scenes)} scenes for {short_name}.")
    else:
        processed_scenes = []
        
    if progress_cache is not None and file_id is not None:
        progress_cache[file_id] = 100
        
    return processed_scenes, duration_seconds

