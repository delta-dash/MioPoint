# HelperMedia.py

# ==============================================================================
# 1. IMPORTS
# ==============================================================================

# --- Standard Library ---
import json
import logging
import os, threading
import time
import math
from datetime import datetime, timedelta
from queue import Queue
import subprocess
from threading import Thread
from typing import Any, Callable, Dict, List, Optional

# --- Third-Party Libraries ---
import aiosqlite
import cv2
import imagehash
import numpy as np
import torch
import torchvision.models as models
import torchvision.transforms as transforms
from PIL import Image
from PIL.ExifTags import TAGS
from PIL.TiffImagePlugin import IFDRational
from scipy.signal import find_peaks

# --- Local Project Imports ---
import OCR
from ConfigMedia import get_config
from EncryptionManager import decrypt_to_memory
from ImageTagger import Tagger
from src.db_utils import AsyncDBContext, execute_db_query
from src.roles.rbac_manager import get_roles_for_user
from utils import truncate_string
from src.reactions.HelperReactions import get_reactions_for_instance
from utils import logging_handler

# ==============================================================================
# 2. GLOBALS & SETUP
# ==============================================================================

logger = logging.getLogger("HelperMedia")
logger.setLevel(logging.WARN)
logger.addHandler(logging_handler)

config = get_config()

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
logger.info(f"Using device: {DEVICE}")

# ResNet model for scene change detection (lazy loaded)
_resnet_model = None
_resnet_preprocess = None
_resnet_last_used = 0
_resnet_lock = threading.Lock()

class CV2Preprocess:
    """
    A custom transform using OpenCV for faster image preprocessing (resize and crop)
    than the standard PIL-based torchvision transforms, as it operates on numpy arrays directly.
    """
    def __init__(self, resize_to: int = 256, crop_size: int = 224):
        self.resize_to = resize_to
        self.crop_size = crop_size

    def __call__(self, np_image: np.ndarray) -> np.ndarray:
        """
        Args:
            np_image (np.ndarray): An HxWxC image in RGB order.
        Returns:
            np.ndarray: A cropped image.
        """
        h, w, _ = np_image.shape

        # Resize smaller edge to `resize_to` while maintaining aspect ratio
        if h < w:
            new_h = self.resize_to
            new_w = int(w * (self.resize_to / h))
        else:
            new_w = self.resize_to
            new_h = int(h * (self.resize_to / w))
        
        resized_img = cv2.resize(np_image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)

        # Center crop
        top = (new_h - self.crop_size) // 2
        left = (new_w - self.crop_size) // 2
        return resized_img[top:top+self.crop_size, left:left+self.crop_size]

def _get_resnet_model():
    """Lazy loader for the ResNet model to avoid loading on module import."""
    global _resnet_model, _resnet_preprocess, _resnet_last_used
    with _resnet_lock:
        if _resnet_model is None:
            logger.info("Lazily loading ResNet model for scene detection...")
            _resnet_model = models.resnet18(weights=models.ResNet18_Weights.IMAGENET1K_V1).to(DEVICE)
            _resnet_model = torch.nn.Sequential(*(list(_resnet_model.children())[:-1]))
            _resnet_model.eval()
            if DEVICE == "cuda":
                logger.info("Compiling ResNet model with torch.compile()... (This may take a moment on first run)")
                _resnet_model = torch.compile(_resnet_model)
                logger.info("ResNet model compilation finished.")

            _resnet_preprocess = transforms.Compose([
                CV2Preprocess(resize_to=256, crop_size=224),
                transforms.ToTensor(),
                transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
            ])
        _resnet_last_used = time.time()
    return _resnet_model, _resnet_preprocess

def unload_resnet_model():
    """Synchronously unloads the ResNet model to free VRAM."""
    global _resnet_model, _resnet_preprocess
    with _resnet_lock:
        if _resnet_model is not None:
            logger.info("Unloading ResNet model to free VRAM.")
            _resnet_model = None
            _resnet_preprocess = None
            if DEVICE == "cuda":
                torch.cuda.empty_cache()

def check_and_unload_idle_resnet(idle_timeout_seconds: int):
    """Checks if the ResNet model is idle and unloads it."""
    global _resnet_model, _resnet_preprocess
    with _resnet_lock:
        if _resnet_model is not None and (time.time() - _resnet_last_used) > idle_timeout_seconds:
            logger.info(f"ResNet model has been idle for over {idle_timeout_seconds}s. Unloading.")
            _resnet_model = None
            _resnet_preprocess = None
            if DEVICE == "cuda":
                torch.cuda.empty_cache()

async def can_user_view_instance(user_id: int, instance_id: int, conn: aiosqlite.Connection) -> bool:
    """Checks if a user can view a specific file instance via their roles."""
    query = """
        SELECT 1 FROM instance_visibility_roles ivr
        JOIN user_roles ur ON ivr.role_id = ur.role_id
        WHERE ur.user_id = ? AND ivr.instance_id = ?
        LIMIT 1
    """
    result = await execute_db_query(conn, query, (user_id, instance_id), fetch_one=True)
    return result is not None

async def get_instance_details(instance_id: int, user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[Dict[str, Any]]:
    """
    Retrieves a complete, structured record for a file instance, ONLY if the user has permission.
    """
    async with AsyncDBContext(external_conn=conn) as db_conn:
        if not await can_user_view_instance(user_id, instance_id, db_conn):
            logger.warning(f"Access Denied: User {user_id} attempted to view instance {instance_id}.")
            return None

        query = """
            SELECT fi.*, fc.*, fld.path as folder_path, fld.name as folder_name
            FROM file_instances fi
            JOIN file_content fc ON fi.content_id = fc.id
            LEFT JOIN folders fld ON fi.folder_id = fld.id
            WHERE fi.id = ?
        """
        row = await execute_db_query(db_conn, query, (instance_id,), fetch_one=True)
        if not row: return None
        
        details = dict(row)
        content_id = details['content_id']

        # Get instance-specific data
        details['visibility_roles'] = await get_visibility_roles_for_instance(instance_id, user_id, conn=db_conn) or []
        if uploader_id := details.get('uploader_id'):
            uploader_info = await execute_db_query(db_conn, "SELECT id, username FROM users WHERE id = ?", (uploader_id,), fetch_one=True)
            if uploader_info:
                uploader_roles = await get_roles_for_user(uploader_id, conn=db_conn)
                details['uploader'] = {"id": uploader_info['id'], "username": uploader_info['username'], "roles": uploader_roles}

        # Get content-specific data
        details['tags'] = await get_tags_for_content(content_id, 'tags', conn=db_conn)
        details['meta_tags'] = await get_tags_for_content(content_id, 'meta_tags', conn=db_conn)
        
        scene_rows = await execute_db_query(db_conn, "SELECT * FROM scenes WHERE content_id = ?", (content_id,), fetch_all=True)
        scenes_data = []
        for scene_row in scene_rows:
            scene_dict = dict(scene_row)
            scene_dict['tags'] = await get_tags_for_scene(scene_dict['id'], conn=db_conn)
            scenes_data.append(scene_dict)
        details['scenes'] = scenes_data
        
        # Get reaction summary
        details['reactions'] = await get_reactions_for_instance(instance_id, user_id, conn=db_conn)
        
        return details

async def get_file_details_by_path_async(path: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[dict]:
    """Retrieves instance and content details for a file by its stored_path."""
    query = """
        SELECT fi.id, fi.stored_path, fc.file_hash
        FROM file_instances fi
        JOIN file_content fc ON fi.content_id = fc.id
        WHERE fi.stored_path = ?
    """
    async with AsyncDBContext(conn) as db_conn:
        row = await execute_db_query(db_conn, query, (path,), fetch_one=True)
    return dict(row) if row else None

async def get_content_by_hash_async(file_hash: str, conn: Optional[aiosqlite.Connection] = None) -> Optional[dict]:
    """Retrieves content details by file_hash."""
    query = "SELECT id FROM file_content WHERE file_hash = ?"
    async with AsyncDBContext(conn) as db_conn:
        row = await execute_db_query(db_conn, query, (file_hash,), fetch_one=True)
    return dict(row) if row else None

async def delete_instance_and_thumbnail_async(instance_id: int, file_hash: str) -> bool:
    """
    Deletes a file instance record and its thumbnail if it's the last one for that content.
    Does NOT delete the physical file from storage.
    """
    async with AsyncDBContext() as db_conn:
        try:
            async with db_conn.execute("BEGIN"):
                await execute_db_query(db_conn, "DELETE FROM file_instances WHERE id = ?", (instance_id,))

                other_instances = await execute_db_query(
                    db_conn, "SELECT 1 FROM file_instances fi JOIN file_content fc ON fi.content_id = fc.id WHERE fc.file_hash = ? LIMIT 1",
                    (file_hash,), fetch_one=True
                )

                if not other_instances and file_hash:
                    thumb_format = config.get('THUMBNAIL_FORMAT', 'WEBP').lower()
                    thumb_path = os.path.join(config['THUMBNAILS_DIR'], f"{file_hash}.{thumb_format}")
                    if os.path.exists(thumb_path):
                        os.remove(thumb_path)
                        logger.info(f"Deleted orphan thumbnail: {thumb_path}")
            
            logger.info(f"Successfully deleted instance record ID: {instance_id}")
            return True
        except Exception as e:
            logger.exception(f"Failed to delete instance record ID {instance_id}: {e}")
            return False

async def delete_instance_and_content_if_orphan(instance_id: int, conn: Optional[aiosqlite.Connection] = None) -> bool:
    """
    Deletes a file instance, its physical file, and the associated content record and thumbnail
    ONLY IF it's the last instance pointing to that content.
    """
    async with AsyncDBContext(conn) as db_conn:
        try:
            # Get all necessary info in one go
            query = "SELECT fi.content_id, fi.stored_path, fc.file_hash FROM file_instances fi JOIN file_content fc ON fi.content_id = fc.id WHERE fi.id = ?"
            details = await execute_db_query(db_conn, query, (instance_id,), fetch_one=True)
            if not details:
                logger.warning(f"Instance {instance_id} not found for deletion.")
                return False
            
            content_id, stored_path, file_hash = details['content_id'], details['stored_path'], details['file_hash']

            async with db_conn.execute("BEGIN"):
                # Delete the instance first
                await execute_db_query(db_conn, "DELETE FROM file_instances WHERE id = ?", (instance_id,))

                # Check if any OTHER instances point to the same content
                other_instances = await execute_db_query(db_conn, "SELECT 1 FROM file_instances WHERE content_id = ? LIMIT 1", (content_id,), fetch_one=True)

                if not other_instances:
                    logger.info(f"Instance {instance_id} was the last one for content {content_id}. Deleting content record and physical files.")
                    await execute_db_query(db_conn, "DELETE FROM file_content WHERE id = ?", (content_id,))
                    
                    # Delete physical file from filestore
                    if stored_path and os.path.exists(stored_path):
                        os.remove(stored_path)
                        logger.info(f"Deleted stored file: {stored_path}")
                    
                    # Delete thumbnail
                    thumb_format = config.get('THUMBNAIL_FORMAT', 'WEBP').lower()
                    thumb_path = os.path.join(config['THUMBNAILS_DIR'], f"{file_hash}.{thumb_format}")
                    if os.path.exists(thumb_path):
                        os.remove(thumb_path)
                        logger.info(f"Deleted orphan thumbnail: {thumb_path}")
                else:
                    logger.info(f"Instance {instance_id} deleted. Content record {content_id} remains as it has other instances.")

            return True
        except Exception as e:
            logger.exception(f"An error occurred during full deletion for instance {instance_id}: {e}")
            return False

# --- Helper Functions for Data Retrieval ---

async def get_tags_for_content(content_id: int, tag_type: str, conn: Optional[aiosqlite.Connection] = None) -> List[str]:
    """Retrieves tags or meta_tags for a specific content ID."""
    from src.tag_utils import _get_table_names # Local import to avoid circular dependency
    tables = _get_table_names(tag_type)
    query = f"SELECT t.name FROM {tables['main']} t JOIN {tables['file_junction']} j ON t.id=j.tag_id WHERE j.content_id=? ORDER BY t.name;"
    async with AsyncDBContext(external_conn=conn) as db_conn:
        rows = await execute_db_query(db_conn, query, (content_id,), fetch_all=True)
        return [row['name'] for row in rows]

async def get_tags_for_scene(scene_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[str]:
    """Retrieves all tags for a specific scene."""
    query = "SELECT t.name FROM tags t JOIN scene_tags st ON t.id = st.tag_id WHERE st.scene_id = ? ORDER BY t.name;"
    async with AsyncDBContext(external_conn=conn) as db_conn:
        rows = await execute_db_query(db_conn, query, (scene_id,), fetch_all=True)
        return [row['name'] for row in rows]

async def get_visibility_roles_for_instance(instance_id: int, user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[List[Dict]]:
    """Gets the list of roles assigned to an instance's visibility."""
    async with AsyncDBContext(external_conn=conn) as db_conn:
        if not await can_user_view_instance(user_id, instance_id, db_conn):
            return None
        query = "SELECT r.id, r.name, r.rank FROM roles r JOIN instance_visibility_roles ivr ON r.id = ivr.role_id WHERE ivr.instance_id = ? ORDER BY r.rank, r.name"
        rows = await execute_db_query(db_conn, query, (instance_id,), fetch_all=True)
        return [dict(row) for row in rows]

async def get_an_instance_for_content(content_id: int, user_id: int, conn: Optional[aiosqlite.Connection] = None) -> Optional[int]:
    """
    Finds a single, visible file_instance.id for a given content_id.
    This is useful for watch parties where the party is tied to content, but navigation
    requires a specific instance. This function provides a "best guess" for which instance to use.
    It prioritizes the instance uploaded by the user, otherwise takes the first one found.
    """
    async with AsyncDBContext(external_conn=conn) as db_conn:
        # Prioritize an instance uploaded by the user themselves.
        query = """
            SELECT fi.id FROM file_instances fi
            JOIN instance_visibility_roles ivr ON fi.id = ivr.instance_id
            JOIN user_roles ur ON ivr.role_id = ur.role_id
            WHERE fi.content_id = ? AND ur.user_id = ? AND fi.uploader_id = ?
            ORDER BY fi.id DESC
            LIMIT 1
        """
        result = await execute_db_query(db_conn, query, (content_id, user_id, user_id), fetch_one=True)
        if result:
            return result['id']

        # If the user didn't upload any, get the most recently created visible instance.
        query_fallback = """
            SELECT fi.id FROM file_instances fi
            JOIN instance_visibility_roles ivr ON fi.id = ivr.instance_id
            JOIN user_roles ur ON ivr.role_id = ur.role_id
            WHERE fi.content_id = ? AND ur.user_id = ?
            ORDER BY fi.id DESC
            LIMIT 1
        """
        result_fallback = await execute_db_query(db_conn, query_fallback, (content_id, user_id), fetch_one=True)
        return result_fallback['id'] if result_fallback else None

async def get_scenes_for_content(content_id: int, conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """
    Retrieves all scene data for a given content ID, including tags and embeddings.
    This is structured to be re-insertable for a new content record.
    """
    async with AsyncDBContext(external_conn=conn) as db_conn:
        # Get all scenes for the content
        scene_rows = await execute_db_query(db_conn, "SELECT * FROM scenes WHERE content_id = ? ORDER BY start_timecode", (content_id,), fetch_all=True)
        if not scene_rows:
            return []

        scenes_data = []
        for scene_row in scene_rows:
            scene_dict = dict(scene_row)
            scene_id = scene_dict['id']
            
            # Get tags for the scene
            scene_dict['tags'] = await get_tags_for_scene(scene_id, conn=db_conn)
            
            # Get embedding for the scene
            embedding_row = await execute_db_query(db_conn, "SELECT embedding FROM clip_embeddings WHERE scene_id = ?", (scene_id,), fetch_one=True)
            if embedding_row:
                scene_dict['embedding'] = embedding_row['embedding']
            
            # Remove old IDs that are not needed for re-insertion
            del scene_dict['id']
            del scene_dict['content_id']
            scenes_data.append(scene_dict)
        return scenes_data

# ==============================================================================
# 4. SYNCHRONOUS DATA EXTRACTION (UNCHANGED)
# These functions generate data for the MediaData DTO and do not interact with the DB.
# ==============================================================================

def extract_image_data_sync(img_rgb: Image.Image, file_name: str, should_tag: bool, tagger: Tagger, clip_model: Any, clip_preprocess: Any, on_progress: Optional[Callable[[float, str], None]] = None) -> tuple:
    """
    A synchronous, CPU/GPU-bound function to extract all data from an image.
    Returns a tuple containing: (tags, p_hash, embedding, transcript, metadata, created_at, modified_at)
    """
    progress_callback = on_progress or (lambda p, m: None)

    progress_callback(10, "Tagging image...")
    tags = tagger.tag(img_rgb, 0.3) if should_tag else []

    progress_callback(40, "Calculating perceptual hash...")
    p_hash = str(imagehash.phash(img_rgb))
    
    progress_callback(50, "Generating embedding...")
    image_input = clip_preprocess(img_rgb).unsqueeze(0).to(DEVICE)
    with torch.no_grad():
        features = clip_model.encode_image(image_input).cpu()
        features /= features.norm(dim=-1, keepdim=True)
    embedding_bytes = features.numpy().tobytes()
    
    progress_callback(75, "Checking for text (OCR)...")
    transcript = ""
    if "english text" in tags:
        img_np = np.array(img_rgb)
        grouped = OCR.get_textgrouper().group_text(image_data=img_np)
        group_texts = [" ".join(item['text'] for item in group) for group in grouped]
        transcript = "\n".join(group_texts)
            
    progress_callback(90, "Extracting EXIF data...")
    metadata = _extract_image_metadata(file_name)

    created_at = None
    modified_at = None
    if metadata:
        # 'DateTimeOriginal' is the date the photo was taken.
        # 'DateTime' is often the last modification date.
        created_at_str = metadata.get('DateTimeOriginal')
        modified_at_str = metadata.get('DateTime')

        if created_at_str and isinstance(created_at_str, str):
            try:
                # Format is 'YYYY:MM:DD HH:MM:SS'
                dt = datetime.strptime(created_at_str, '%Y:%m:%d %H:%M:%S')
                created_at = dt.isoformat()
            except (ValueError, TypeError):
                pass # Ignore if format is unexpected
        
        if modified_at_str and isinstance(modified_at_str, str):
            try:
                dt = datetime.strptime(modified_at_str, '%Y:%m:%d %H:%M:%S')
                modified_at = dt.isoformat()
            except (ValueError, TypeError):
                pass

    progress_callback(100, "Extraction complete")
    return tags, p_hash, embedding_bytes, transcript, metadata, created_at, modified_at

def _extract_image_metadata(path: str) -> dict:
    """Extracts EXIF and basic metadata from an image file."""
    try:
        with Image.open(path) as img:
            exif_data = img.getexif()
            metadata = {TAGS.get(tag, tag): _decode_exif_value(val) for tag, val in exif_data.items()}
            metadata.update({'width': img.width, 'height': img.height, 'format': img.format})
            return metadata
    except Exception:
        return {}

def _decode_exif_value(value):
    """Helper to decode EXIF data types into JSON-serializable formats."""
    if isinstance(value, bytes): return value.decode('utf-8', errors='ignore')
    if isinstance(value, IFDRational): return float(value)
    return value

def extract_video_metadata_sync(path: str) -> tuple:
    """
    Extracts metadata from a video file using ffprobe.
    Returns a tuple of (metadata_dict, created_at_iso, modified_at_iso).
    """
    command = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', path]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        data = json.loads(result.stdout)
        
        metadata = data.get('format', {}).get('tags', {})
        created_at = None
        
        creation_time_str = metadata.get('creation_time')
        if creation_time_str:
            try:
                # ffprobe often gives ISO 8601 with 'Z' for UTC. Python < 3.11 needs help with 'Z'.
                if creation_time_str.endswith('Z'):
                    creation_time_str = creation_time_str[:-1] + '+00:00'
                dt = datetime.fromisoformat(creation_time_str)
                created_at = dt.isoformat()
            except (ValueError, TypeError):
                logger.warning(f"Could not parse video creation_time: {creation_time_str}")
        
        return metadata, created_at, None # No reliable modified_at from ffprobe format tags

    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError):
        # Don't log error if ffprobe is just not installed
        return {}, None, None

def extract_video_scene_data_hybrid(video_path: str, tagger: Any, clip_model: Any, clip_preprocess: Any, should_tag: bool = True, on_progress: Optional[Callable[[float, str], None]] = None) -> tuple[List[Dict[str, Any]], Optional[float]]:
    """ "Detect Fast, Extract Smart" pipeline for robust video analysis. """
    progress_callback = on_progress or (lambda p, m: None)

    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened(): raise IOError("Cannot open video for metadata.")
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        cap.release()
        if not fps > 0 or not total_frames > 0: raise ValueError("Invalid video metadata.")
        duration_seconds = total_frames / fps
    except Exception as e:
        logger.error(f"Could not read video metadata: {e}")
        progress_callback(100.0, "Error: Could not read metadata.")
        return [], None

    if not should_tag:
        return [], duration_seconds

    progress_callback(0.0, "Detecting scenes...")
    scene_start_frames = detect_scenes_gpu_ai(video_path, threshold=config.get("SCENE_DETECTION_THRESHOLD", 0.3))
    progress_callback(25.0, f"Scene detection complete ({len(scene_start_frames)} boundaries).")

    is_fallback = len(scene_start_frames) <= 1
    frames_to_extract, scenes_info = _plan_frame_extraction(scene_start_frames, total_frames, fps, is_fallback)
    if not frames_to_extract:
        return [], duration_seconds

    progress_callback(25.0, f"Extracting {len(frames_to_extract)} frames...")
    extracted_frames = _extract_frames_from_video(video_path, frames_to_extract)
    if not extracted_frames:
        progress_callback(100.0, "Error: Could not extract frames.")
        return [], duration_seconds
    progress_callback(30.0, "Frame extraction complete.")

    pil_images = [Image.fromarray(frame) for frame in extracted_frames]
    
    # --- NEW: Process frames in smaller, memory-safe batches ---
    per_frame_data = []
    # This batch size can be tuned via config, with 32 as a safe default.
    processing_batch_size = config.get("VIDEO_FRAME_BATCH_SIZE", 32)
    
    total_images = len(pil_images)
    for i in range(0, total_images, processing_batch_size):
        batch_pil_images = pil_images[i:i+processing_batch_size]
        
        # Define a progress callback that maps the inner batch progress (0-100)
        # to the correct segment of the overall progress bar (30-100).
        base_progress = 30.0 + ((i / total_images) * 70.0)
        progress_range = (len(batch_pil_images) / total_images) * 70.0
        def inner_progress_callback(p, m):
            progress_callback(base_progress + (p / 100.0 * progress_range), m)

        batch_data = _batch_process_frames(batch_pil_images, tagger, clip_model, clip_preprocess, inner_progress_callback)
        per_frame_data.extend(batch_data)

    processed_scenes = _assemble_final_scenes(scenes_info, per_frame_data, is_fallback)
    
    progress_callback(100.0, "Processing complete")
    return processed_scenes, duration_seconds

# --- Sub-functions for Video Processing (to de-nest logic) ---

def _plan_frame_extraction(scene_boundaries: List[int], total_frames: int, fps: float, is_fallback: bool, fallback_samples: int = 3) -> tuple[List[int], List[Dict]]:
    """
    Determines which frames to extract.
    - For scenes, it samples multiple frames for keyframe analysis.
    - For fallback, it samples frames across the whole video.
    It returns a flat list of frame indices to extract, and a list of scene_info dicts
    that map scenes to their corresponding indices in the flat list.
    """
    from scenedetect.frame_timecode import FrameTimecode # Local import
    frames_to_extract: List[int] = []
    scenes_info: List[Dict] = []
    
    seconds_per_sample = config.get("SCENE_SECONDS_PER_KEYFRAME_SAMPLE", 60)
    max_samples_per_scene = config.get("SCENE_KEYFRAME_MAX_SAMPLES", 10)

    if is_fallback:
        logger.info(f"No significant scenes found. Sampling {fallback_samples} frames.")
        frame_indices = []
        for i in range(fallback_samples):
            frame_indices.append(int(total_frames * (i + 1) / (fallback_samples + 1)))
        
        frames_to_extract.extend(frame_indices)
        scenes_info.append({
            "start": "00:00:00.000", 
            "end": FrameTimecode(total_frames - 1, fps).get_timecode(),
            "frame_indices_in_master_list": list(range(len(frame_indices)))
        })
    else:
        scene_list = list(zip(scene_boundaries, scene_boundaries[1:] + [total_frames]))
        master_index_counter = 0
        for start, end in scene_list:
            scene_duration_frames = end - start
            if scene_duration_frames > 1:
                scene_info = {
                    "start": FrameTimecode(start, fps).get_timecode(), 
                    "end": FrameTimecode(end - 1, fps).get_timecode(),
                    "frame_indices_in_master_list": []
                }
                
                if fps > 0 and seconds_per_sample > 0:
                    scene_duration_seconds = scene_duration_frames / fps
                    # Sample one frame per X seconds, with a minimum of 1.
                    num_samples = max(1, math.ceil(scene_duration_seconds / seconds_per_sample))
                else:
                    # Fallback to a single middle frame if no FPS or sampling is disabled.
                    num_samples = 1
                
                # Apply the absolute maximum cap.
                num_samples = min(num_samples, max_samples_per_scene)
                
                for i in range(num_samples):
                    # Distribute samples evenly throughout the scene
                    frame_index = start + int((i + 0.5) * (scene_duration_frames / num_samples))
                    
                    scene_info["frame_indices_in_master_list"].append(master_index_counter)
                    frames_to_extract.append(frame_index)
                    master_index_counter += 1
                
                if scene_info["frame_indices_in_master_list"]:
                    scenes_info.append(scene_info)

    return frames_to_extract, scenes_info

def _extract_frames_from_video(video_path: str, frame_indices: List[int]) -> List[np.ndarray]:
    """Efficiently extracts a list of specific frames from a video by reading sequentially."""
    if not frame_indices:
        return []

    # Use a dict to store frames by index, ensuring we can return them in the correct order.
    extracted_frames: Dict[int, np.ndarray] = {}
    cap = None
    try:
        cap = cv2.VideoCapture(video_path, cv2.CAP_FFMPEG)
        if not cap.isOpened(): raise IOError("Could not open video for frame extraction.")

        # Create a sorted list and a set for efficient lookup.
        sorted_indices = sorted(list(set(frame_indices)))
        indices_to_find = set(sorted_indices)
        
        current_frame_idx = 0
        
        # Sequentially read the video, which is much faster than seeking.
        while indices_to_find:
            # cap.grab() is fast as it doesn't decode the frame.
            ret = cap.grab()
            if not ret:
                logger.warning(f"Reached end of video before extracting all frames. Got {len(extracted_frames)}/{len(frame_indices)}.")
                break

            # If the current frame is one we need, decode it with cap.retrieve().
            if current_frame_idx in indices_to_find:
                ret_retrieve, frame = cap.retrieve()
                if ret_retrieve:
                    extracted_frames[current_frame_idx] = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    indices_to_find.remove(current_frame_idx)
                else:
                    logger.warning(f"Failed to retrieve frame {current_frame_idx} after a successful grab.")
            
            current_frame_idx += 1
        
        # Return the extracted frames in the same order as the original sorted list.
        return [extracted_frames[i] for i in sorted_indices if i in extracted_frames]
    except Exception as e:
        logger.error(f"OpenCV frame extraction failed: {e}", exc_info=True)
        return []
    finally:
        if cap: cap.release()

def _batch_process_frames(pil_images: List[Image.Image], tagger: Tagger, clip_model: Any, clip_preprocess: Any, on_progress: Callable) -> List[Dict]:
    """Processes a batch of images with CLIP and Tagger models."""
    try:
        # --- Pre-resize large frames on CPU to conserve GPU memory ---
        max_dim = config.get("MAX_IMAGE_DIMENSION", 4096)
        if max_dim > 0:
            resized_images = []
            for img in pil_images:
                if img.width > max_dim or img.height > max_dim:
                    logger.debug(f"Video frame ({img.width}x{img.height}) exceeds max dimension ({max_dim}). Downscaling on CPU.")
                    img_copy = img.copy()
                    # thumbnail resizes in-place and preserves aspect ratio
                    img_copy.thumbnail((max_dim, max_dim), Image.LANCZOS)
                    resized_images.append(img_copy)
                else:
                    resized_images.append(img)
            pil_images = resized_images # Use the potentially resized list for further processing

        on_progress(0.0, "Encoding with CLIP...")
        image_input_batch = torch.stack([clip_preprocess(img) for img in pil_images]).to(DEVICE)
        with torch.no_grad():
            features = clip_model.encode_image(image_input_batch).cpu()
            features /= features.norm(dim=-1, keepdim=True)

        on_progress(50.0, f"Tagging {len(pil_images)} frames in a batch...")
        # Use the new batch tagging method for much better performance
        batch_tags = tagger.tag_batch(pil_images, 0.3)

        per_frame_data = []
        for i, pil_image in enumerate(pil_images):
            on_progress(80.0 + (i / len(pil_images) * 20.0), f"Hashing frame {i+1}...")
            per_frame_data.append({
                "tags": batch_tags[i],
                "phash": str(imagehash.phash(pil_image)),
                "embedding": features[i].numpy().tobytes(),
            })
        return per_frame_data
    except Exception as e:
        logger.error(f"AI model processing failed: {e}", exc_info=True)
        return []

def _assemble_final_scenes(scenes_info: List[Dict], per_frame_data: List[Dict], is_fallback: bool) -> List[Dict]:
    """
    Combines scene timecodes with extracted frame data.
    If multiple frames are provided for a scene, it filters them by phash to select
    distinct keyframes, then aggregates their data.
    """
    phash_threshold = config.get("SCENE_KEYFRAME_PHASH_THRESHOLD", 5)
    final_scenes = []

    for scene_info in scenes_info:
        # Get all data for frames belonging to this scene
        scene_frame_data = [per_frame_data[i] for i in scene_info.get("frame_indices_in_master_list", []) if i < len(per_frame_data)]
        
        if not scene_frame_data:
            continue

        # --- Keyframe selection via phash ---
        keyframes = []
        last_keyframe_phash = None
        for frame_data in scene_frame_data:
            if not frame_data.get('phash'): continue
            try:
                current_phash = imagehash.hex_to_hash(frame_data['phash'])
            except (ValueError, TypeError):
                continue

            if last_keyframe_phash is None or (current_phash - last_keyframe_phash) > phash_threshold:
                keyframes.append(frame_data)
                last_keyframe_phash = current_phash
        
        if not keyframes:
            keyframes = [scene_frame_data[0]]

        # --- Aggregation ---
        aggregated_tags = sorted(list(set(tag for frame in keyframes for tag in frame.get('tags', []))))
        
        valid_embeddings = [np.frombuffer(frame['embedding'], dtype=np.float32) for frame in keyframes if frame.get('embedding')]
        if not valid_embeddings:
            avg_embedding_bytes = None
        else:
            avg_embedding = np.mean(valid_embeddings, axis=0)
            avg_embedding_bytes = avg_embedding.tobytes()
        
        final_scene = {
            "start": scene_info["start"],
            "end": scene_info["end"],
            "tags": aggregated_tags,
            "phash": keyframes[len(keyframes) // 2]['phash'],
            "embedding": avg_embedding_bytes,
        }
        final_scenes.append(final_scene)
        
    return final_scenes

def detect_scenes_gpu_ai(video_path: str, batch_size: int = 500, threshold: float = 0.5, peak_distance: int = 10, max_fps: int = 30) -> List[int]:
    """Detects scene boundaries using a GPU-accelerated AI model."""
    logger.info(f"Starting AI scene detection for '{os.path.basename(video_path)}' with threshold={threshold}, peak_dist={peak_distance}, max_fps={max_fps}")

    # Probe video with OpenCV *before* loading the GPU model. This can prevent
    # deadlocks caused by conflicting CUDA context initializations between libraries.
    logger.debug("Probing video with OpenCV before loading GPU model...")
    # Explicitly use the FFMPEG backend for VideoCapture, as it's more robust
    # and can prevent hangs with certain complex video files (e.g., MKV).
    cap = cv2.VideoCapture(video_path, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        logger.error(f"Failed to open video '{video_path}' in main thread for probing.")
        return [0]
    original_fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    cap.release()
    logger.debug("Video probed successfully. Now loading AI model...")

    resnet_model, resnet_preprocess = _get_resnet_model()
    frame_step = int(round(original_fps / max_fps)) if original_fps > max_fps else 1
    logger.info(f"Processing Video on path: {video_path}")
    logger.info(f"Original FPS: {original_fps:.2f}, processing at max {max_fps} FPS (frame step: {frame_step}).")

    if total_frames <= 0 or frame_step <= 0:
        logger.warning(f"Video has {total_frames} frames or frame_step is {frame_step}. Cannot process.")
        return [0]
    
    total_frames_to_process = math.ceil(total_frames / frame_step)
    total_batches = math.ceil(total_frames_to_process / batch_size)
    logger.info(f"Total frames to process: ~{total_frames_to_process} ({total_batches} batches of size {batch_size})")

    batch_queue = Queue(maxsize=4)
    reader_thread = Thread(target=_frame_reader_worker, args=(video_path, batch_size, resnet_preprocess, batch_queue, frame_step))
    reader_thread.start()

    all_features_gpu = []
    batch_count = 0
    start_time = time.time()
    try:
        logger.debug("Main thread waiting for frame batches from worker...")
        while True:
            batch = batch_queue.get()
            if batch is None:
                logger.debug("Received sentinel 'None' from worker. Finishing up.")
                break
            if isinstance(batch, Exception): raise batch
            batch_count += 1
            
            elapsed_time = time.time() - start_time
            if batch_count > 0 and elapsed_time > 1: # Start showing ETA after 1s for stability
                time_per_batch = elapsed_time / batch_count
                remaining_batches = total_batches - batch_count
                eta_seconds = remaining_batches * time_per_batch
                eta_formatted = str(timedelta(seconds=int(eta_seconds)))
                progress_percent = (batch_count / total_batches) * 100
                logger.info(f"Processing feature batch {batch_count}/{total_batches} ({progress_percent:.1f}%)... ETA: {eta_formatted}")
            else:
                logger.info(f"Processing feature batch {batch_count}/{total_batches}...")

            with torch.no_grad():
                features = resnet_model(batch.to(DEVICE))
                all_features_gpu.append(features.squeeze())
    finally:
        logger.debug("Joining frame reader thread...")
        reader_thread.join()
        logger.debug("Frame reader thread joined.")

    if not all_features_gpu:
        logger.warning("No features were extracted from the video. Returning a single scene.")
        return [0]
    
    all_features = torch.cat(all_features_gpu)
    logger.debug(f"Extracted {all_features.shape[0]} feature vectors for scene analysis.")
    similarities = torch.nn.functional.cosine_similarity(all_features[:-1], all_features[1:])
    dissimilarity = (1 - similarities).cpu().numpy()
    peaks, _ = find_peaks(dissimilarity, height=threshold, distance=peak_distance)
    
    scene_boundaries = sorted(list(set([0] + [int(p + 1) * frame_step for p in peaks])))
    logger.info(f"Found {len(peaks)} scene changes, resulting in {len(scene_boundaries)} scene boundaries.")
    logger.debug(f"Scene boundary frames: {truncate_string(str(scene_boundaries), 150)}")
    
    return scene_boundaries

def _frame_reader_worker(video_path: str, batch_size: int, preprocess_fn: Callable, queue: Queue, frame_step: int = 1):
    """Worker thread to read and preprocess video frames."""
    cap = None
    logger.debug("Frame reader worker thread started.")
    try:
        logger.debug(f"Worker opening video with FFMPEG backend: '{os.path.basename(video_path)}'")
        # Explicitly use the FFMPEG backend for robustness.
        cap = cv2.VideoCapture(video_path, cv2.CAP_FFMPEG)
        if not cap.isOpened(): raise IOError("Cannot open video in worker thread.")
        logger.debug("Worker successfully opened video.")
        
        frame_buffer = []
        frames_processed = 0
        
        # This loop is optimized to avoid slow `cap.set()` seeking. It sequentially
        # grabs frames, which is much faster. It only decodes the frames it needs.
        while True:
            # Grab is faster than read as it doesn't decode.
            ret = cap.grab()
            if not ret: break # End of video

            # We are at a frame we want to process. Now retrieve (decode) it.
            ret, frame = cap.retrieve()
            if not ret: continue # Should not happen if grab() succeeded

            frames_processed += 1
            frame_buffer.append(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            
            if len(frame_buffer) == batch_size:
                queue.put(torch.stack([preprocess_fn(f) for f in frame_buffer]))
                frame_buffer = []

            # Skip the next 'frame_step - 1' frames by just grabbing them
            if frame_step > 1:
                for _ in range(frame_step - 1):
                    if not cap.grab(): break # End of video

        if frame_buffer:
            queue.put(torch.stack([preprocess_fn(f) for f in frame_buffer]))
        logger.debug(f"Frame reader finished. Processed {frames_processed} frames from '{os.path.basename(video_path)}'.")
    except Exception as e:
        logger.error(f"Error in frame reader worker: {e}", exc_info=True)
        queue.put(e)
    finally:
        if cap:
            logger.debug("Worker releasing video capture.")
            cap.release()
        logger.debug("Worker putting sentinel 'None' on queue and exiting.")
        queue.put(None)