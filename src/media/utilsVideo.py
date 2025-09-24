import asyncio
import json
import os
import shutil
import subprocess
import tempfile
from typing import Optional
from src.websocket.HelperWebsocket import get_websocket_manager

# Project-specific imports
import logging
from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query
from processing_status import status_manager
import functools
from utilsEncryption import calculate_file_hash

logger = logging.getLogger("UtilsVideo")
logger.setLevel(logging.DEBUG)
config = get_config()


# ==============================================================================
# 1. FFmpeg System Utilities (No Changes Needed)
# These functions check the system's capabilities and are independent of the DB schema.
# ==============================================================================

@functools.lru_cache(maxsize=1)
def get_hardware_encoders() -> dict:
    """Checks for available hardware encoders and returns a dictionary of the best available codec for each type."""
    # Default to high-quality CPU encoders
    encoders = {
        'h264': 'libx264',
        'hevc': 'libx265',
        'av1': 'libaom-av1',
        'hw_accel_type': None  # 'nvidia', 'intel_qsv', etc.
    }
    try:
        # Running ffmpeg -encoders can be slow, so we cache the result.
        result = subprocess.run(['ffmpeg', '-encoders'], capture_output=True, text=True, check=True)
        available_encoders = result.stdout

        # Prioritize NVIDIA NVENC
        if 'nvenc' in available_encoders:
            encoders['hw_accel_type'] = 'nvidia'
            logger.info("NVIDIA NVENC hardware acceleration available.")
            if 'h264_nvenc' in available_encoders:
                encoders['h264'] = 'h264_nvenc'
                logger.info("-> H.264 (h264_nvenc) supported.")
            if 'hevc_nvenc' in available_encoders:
                encoders['hevc'] = 'hevc_nvenc'
                logger.info("-> HEVC (hevc_nvenc) supported.")
            if 'av1_nvenc' in available_encoders:
                encoders['av1'] = 'av1_nvenc'
                logger.info("-> AV1 (av1_nvenc) supported.")
            return encoders

        # Then check for Intel QSV
        if 'qsv' in available_encoders:
            encoders['hw_accel_type'] = 'intel_qsv'
            logger.info("Intel QSV hardware acceleration available.")
            if 'h264_qsv' in available_encoders:
                encoders['h264'] = 'h264_qsv'
                logger.info("-> H.264 (h264_qsv) supported.")
            if 'hevc_qsv' in available_encoders:
                encoders['hevc'] = 'hevc_qsv'
                logger.info("-> HEVC (hevc_qsv) supported.")
            if 'av1_qsv' in available_encoders:
                encoders['av1'] = 'av1_qsv'
                logger.info("-> AV1 (av1_qsv) supported.")
            return encoders

        logger.info("No supported hardware encoders found. Defaulting to CPU (libx264/libx265/libaom-av1).")
        return encoders

    except Exception as e:
        logger.error(f"Could not check for ffmpeg encoders. Defaulting to CPU. Error: {e}")
        return encoders

HARDWARE_ENCODERS = get_hardware_encoders()


# ==============================================================================
# 2. Video Probing & Streaming Utilities (No Changes Needed)
# These functions operate on file paths and are independent of the DB schema.
# ==============================================================================

async def select_best_streams(video_path: str) -> tuple[Optional[int], Optional[int]]:
    """Probes a video file to find the best video and audio streams."""
    logger.info(f"Probing '{os.path.basename(video_path)}' to select best streams...")
    args = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path]
    try:
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(f"ffprobe failed for {video_path}: {stderr.decode()}")
            return 0, 1 # Fallback
        
        data = json.loads(stdout)
        streams = data.get('streams', [])
        
        video_streams = [s for s in streams if s.get('codec_type') == 'video' and s.get('disposition', {}).get('attached_pic', 0) == 0]
        best_video_index = max(video_streams, key=lambda s: float(s.get('duration', 0)), default={}).get('index')

        audio_streams = [s for s in streams if s.get('codec_type') == 'audio' and s.get('disposition', {}).get('comment', 0) == 0]
        if audio_streams:
            def score_lang(s):
                lang = s.get('tags', {}).get('language', 'und').lower()
                return 3 if lang == 'eng' else 2 if lang in ('jpn', 'jp') else 1
            best_audio_index = max(audio_streams, key=score_lang).get('index')
        else:
            best_audio_index = next((s.get('index') for s in streams if s.get('codec_type') == 'audio'), None)

        logger.info(f"Selected streams: Video Index={best_video_index}, Audio Index={best_audio_index}")
        if best_video_index is None:
            raise ValueError("No valid video stream found.")
        return best_video_index, best_audio_index
    except Exception as e:
        logger.exception(f"Error probing streams in {video_path}: {e}")
        return None, None

async def is_video_codec_compatible(video_path: str) -> bool:
    """
    Checks if a video's codec AND container are web-compatible (H.264 in an MP4 container).
    """
    if not os.path.exists(video_path): return False
    
    # --- NEW: Add a check for the container type ---
    # If the container isn't mp4, it's not compatible for direct playback, regardless of codec.
    if not video_path.lower().endswith('.mp4'):
        logger.info(f"'{os.path.basename(video_path)}' is in an incompatible container. Requires transcoding.")
        return False
    # --- END NEW ---

    args = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path]
    try:
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(f"ffprobe failed for {video_path}: {stderr.decode()}")
            return False
        data = json.loads(stdout)
        video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
        if not video_stream:
            logger.warning(f"No video stream found in '{os.path.basename(video_path)}'.")
            return True # Assuming non-video files are 'compatible' to be served directly
        codec = video_stream.get('codec_name')
        logger.info(f"Detected codec for '{os.path.basename(video_path)}': {codec}")
        
        # The original check is now the final confirmation
        is_h264 = (codec == 'h264')
        if not is_h264:
             logger.info(f"'{os.path.basename(video_path)}' has incompatible codec '{codec}'. Requires transcoding.")

        return is_h264

    except Exception as e:
        logger.exception(f"Error probing {video_path}: {e}")
        return False

async def get_video_duration(video_path: str) -> float:
    """Probes a video file to get its duration in seconds."""
    args = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
    try:
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(f"ffprobe failed to get duration for {video_path}: {stderr.decode()}")
            return 0.0
        duration_str = stdout.decode().strip()
        return float(duration_str) if duration_str and duration_str != 'N/A' else 0.0
    except Exception as e:
        logger.exception(f"Error getting duration for {video_path}: {e}")
        return 0.0



# ==============================================================================
# 3. BACKGROUND CONVERSION TASK (UPDATED FOR NEW SCHEMA)
# ==============================================================================

# This task is now assumed to be decorated by the TaskManager in a separate `tasks.py` file.
async def convert_and_update_video_content(instance_id_for_cache: int, content_id: int, source_path: str):
    """
    Worker function to transcode a video. Operates on a content_id but uses the
    instance_id for progress tracking. The resulting transcoded file is associated
    with the content, benefiting all its instances.
    """
    new_temp_path = None
    job_id = instance_id_for_cache

    try:
        # --- Pre-flight check: Has this content been converted already? ---
        async with AsyncDBContext() as conn:
            check = await execute_db_query(conn, "SELECT transcoded_path FROM file_content WHERE id = ?", (content_id,), fetch_one=True)
        if check and check['transcoded_path'] and os.path.exists(check['transcoded_path']):
            logger.info(f"Skipping conversion for content ID {content_id}; a valid transcoded file already exists.")
            return

        # --- Initial DB read for duration ---
        async with AsyncDBContext() as conn:
            content_data = await execute_db_query(
                conn, 
                "SELECT fc.duration_seconds, fi.file_name FROM file_content fc JOIN file_instances fi ON fc.id = fi.content_id WHERE fc.id = ? LIMIT 1", 
                (content_id,), 
                fetch_one=True
            )
        duration_seconds = content_data['duration_seconds'] if content_data and content_data['duration_seconds'] else 0
        original_filename = content_data['file_name'] if content_data else os.path.basename(source_path)

        status_manager.start_processing(
            job_id,
            "Starting web-ready transcode...",
            filename=original_filename,
            ingest_source='transcoding'
        )

        # --- Build FFmpeg command (Corrected and Hardened Logic) ---
        transcoded_cache_dir = os.path.abspath(config["TRANSCODED_CACHE_DIR"])
        os.makedirs(transcoded_cache_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(suffix="_reencoded.mp4", dir=transcoded_cache_dir, delete=False) as temp_f:
            new_temp_path = temp_f.name

        # 1. Base command and robust input flags
        args = [
            'ffmpeg',
            '-y',  # Overwrite output file if it exists
            '-progress', 'pipe:1', # For progress tracking
            '-fflags', '+genpts',  # **FIX**: Regenerate timestamps to fix potential source issues
            '-i', source_path
        ]

        # 2. Stream Mapping (More robust than custom selection)
        # Map the first video stream, and the first audio stream if it exists.
        args.extend(['-map', '0:v:0'])
        args.extend(['-map', '0:a:0?']) # The '?' makes the audio stream optional, preventing errors on video-only files

        # 3. Video encoding options
        h264_encoder = HARDWARE_ENCODERS.get('h264', 'libx264')
        
        # **CRITICAL FIX**: Define robust video filters for maximum compatibility.
        # - format=yuv420p: Converts 10-bit color (yuv420p10) to web-standard 8-bit.
        # - scale=...: Ensures video dimensions are even, as required by H.264.
        video_filters = 'format=yuv420p,scale=trunc(iw/2)*2:trunc(ih/2)*2'
        
        # **CRITICAL FIX**: Do NOT use `-hwaccel cuda` for decoding. Let the robust CPU decoder
        # handle the input, then pass the clean frames to the GPU for encoding.
        if h264_encoder == 'h264_nvenc':
            args.extend(['-c:v', 'h264_nvenc', '-preset', 'p5', '-cq', '23', '-profile:v', 'main'])
        elif h264_encoder == 'h264_qsv':
            args.extend(['-c:v', 'h264_qsv', '-preset', 'fast', '-global_quality', '23', '-profile:v', 'main'])
        else: # libx264 (CPU)
            args.extend(['-c:v', 'libx264', '-preset', 'fast', '-crf', '23'])

        args.extend(['-vf', video_filters])

        # 4. Audio handling for compatibility
        # -ac 2: Downmix any multi-channel audio (5.1, 7.1) to stereo for web compatibility.
        args.extend(['-c:a', 'aac', '-b:a', '192k', '-ac', '2'])
        
        # 5. Final container options and the OUTPUT FILE
        args.extend(['-movflags', '+faststart', new_temp_path])

        logger.info(f"Executing FFmpeg for content ID {content_id}: {' '.join(args)}")
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        
        status_manager.update_progress(job_id, 0.0, "Transcoding...")
        
        # --- Progress reporting and process waiting ---
        while line := await proc.stdout.readline():
            line_str = line.decode()
            if line_str.startswith('out_time_ms=') and duration_seconds > 0:
                try:
                    current_ms_str = line_str.split('=')[1].strip()
                    if current_ms_str != 'N/A':
                        current_ms = int(current_ms_str)
                        progress = min(round((current_ms / (duration_seconds * 1000000)) * 100, 2), 95.0)
                        status_manager.update_progress(job_id, progress, "Transcoding...")
                except (ValueError, IndexError):
                    pass

        await proc.wait()
        if proc.returncode != 0:
            stderr = (await proc.stderr.read()).decode()
            logger.error(f"FFmpeg failed for content {content_id}. Stderr:\n{stderr}")
            raise RuntimeError(f"FFmpeg failed for content {content_id}. See logs for details.")

        # --- Update Database and Filesystem ---
        final_hash = (await asyncio.to_thread(calculate_file_hash, new_temp_path))
        final_cache_path = os.path.join(transcoded_cache_dir, f"{final_hash}.mp4")

        if os.path.exists(final_cache_path):
            logger.warning(f"Transcoded file {final_cache_path} already exists (race condition). Removing temporary file.")
            os.remove(new_temp_path)
        else:
            os.rename(new_temp_path, final_cache_path)
        new_temp_path = None

        async with AsyncDBContext() as conn:
            await execute_db_query(conn, "UPDATE file_content SET transcoded_path = ?, is_webready = 1 WHERE id = ?", (final_cache_path, content_id))

        logger.info(f"Successfully cached transcoded file for content ID {content_id} at {final_cache_path}.")
        status_manager.set_completed(job_id, "Transcoding complete.")

        # --- Notify clients via WebSocket ---
        manager = get_websocket_manager()
        room_name = f"instance-viewers:{instance_id_for_cache}"
        new_url = f"/api/instance/{instance_id_for_cache}/media"
        message = {
            "type": "transcoding_complete",
            "payload": {"instance_id": instance_id_for_cache, "url": new_url}
        }
        await manager.broadcast_to_room(room_name, message)

    except Exception as e:
        logger.error(f"Error during conversion for content ID {content_id}: {e}", exc_info=True)
        status_manager.set_error(job_id, str(e))
        raise # Re-raise the exception to be handled by the calling worker manager
    finally:
        if new_temp_path and os.path.exists(new_temp_path):
            try:
                os.remove(new_temp_path)
            except OSError as e:
                logger.error(f"Failed to remove temporary file {new_temp_path}: {e}")




# --- ASYNC UTILITY FUNCTIONS ---

async def stream_transcoded_video(source_path: str, password: Optional[str] = None, details: Optional[dict] = None):
    """
    Async generator to transcode a video on-the-fly for streaming.
    Handles decryption to a temporary file if a password is provided.
    """
    logger.info(f"Starting on-the-fly stream for: '{os.path.basename(source_path)}'")
    
    temp_path = None
    stream_source_path = source_path
    proc = None

    try:
        if password:
            if not details:
                raise ValueError("Details dictionary must be provided for encrypted streaming.")
            
            from EncryptionManager import decrypt_to_memory
            decrypted_content = await asyncio.to_thread(decrypt_to_memory, source_path, password)
            if not decrypted_content:
                logger.error(f"Invalid password for streaming encrypted file: {source_path}")
                return # Stop generator

            with tempfile.NamedTemporaryFile(delete=False, suffix=details.get('extension', '.tmp')) as temp_f:
                temp_f.write(decrypted_content)
                temp_path = temp_f.name
            stream_source_path = temp_path
            logger.debug(f"Streaming from temporary decrypted file: {temp_path}")

        # Use a more robust, simplified ffmpeg command structure.
        # This avoids custom stream selection and relies on ffmpeg's defaults,
        # which is more reliable. It also adds flags to handle problematic source files.
        args = [
            'ffmpeg',
            '-hide_banner', '-loglevel', 'error',
            '-fflags', '+genpts',  # Regenerate timestamps to fix source issues
            '-i', stream_source_path,
            '-map', '0:v:0',      # Select first video stream
            '-map', '0:a:0?',      # Select first audio stream (optional)
            '-f', 'mp4'
        ]

        # On-the-fly streaming should always use H.264 for maximum compatibility.
        h264_encoder = HARDWARE_ENCODERS['h264']
        if h264_encoder == 'h264_nvenc':
            # Use a faster preset for live streaming
            args.extend(['-c:v', 'h264_nvenc', '-preset', 'p4', '-cq', '23', '-profile:v', 'main'])
        elif h264_encoder == 'h264_qsv':
            args.extend(['-c:v', 'h264_qsv', '-preset', 'veryfast', '-global_quality', '23', '-profile:v', 'main'])
        else: # libx264
            args.extend(['-c:v', 'libx264', '-preset', 'veryfast'])

        # Combine pixel format conversion into the video filter for robustness.
        # This handles more edge cases like 10-bit color inputs before they reach the encoder.
        video_filters = 'format=yuv420p,scale=trunc(iw/2)*2:trunc(ih/2)*2'
        args.extend(['-vf', video_filters])

        # Audio arguments are now independent of the stream selection logic.
        # ffmpeg will only apply them if an audio stream is mapped via `-map 0:a:0?`.
        # Downmix to stereo for maximum compatibility.
        args.extend(['-c:a', 'aac', '-ac', '2'])
        
        # Final streaming arguments
        args.extend(['-movflags', 'frag_keyframe+empty_moov+faststart', '-y', 'pipe:1'])
        
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        
        while chunk := await proc.stdout.read(65536):
            yield chunk
        
        await proc.wait() # Wait for process to finish to check return code

    except asyncio.CancelledError:
        logger.warning(f"Client disconnected. Terminating stream for '{os.path.basename(source_path)}'")
    finally:
        if proc and proc.returncode is None: 
            proc.terminate()
            await proc.wait()

        if proc:
            stderr = (await proc.stderr.read()).decode()
            if proc.returncode != 0 and "Broken pipe" not in stderr:
                logger.error(f"FFmpeg streaming error for '{os.path.basename(source_path)}': {stderr.strip()}")
        
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


async def get_subtitle_streams(video_path: str) -> list:
    """Probes a video file using ffprobe and returns a list of its subtitle streams."""
    args = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path]
    try:
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(f"Failed to probe subtitles in {video_path}: {stderr.decode()}")
            return []
        data = json.loads(stdout)
        return [{
            'index': s['index'],
            'language': s.get('tags', {}).get('language', 'und'),
            'title': s.get('tags', {}).get('title', f"Track {s['index']}")
        } for s in data.get('streams', []) if s.get('codec_type') == 'subtitle']
    except Exception as e:
        logger.exception(f"Error probing subtitles in {video_path}: {e}")
        return []


# --- SYNCHRONOUS UTILITY FUNCTION ---

def reencode_to_h264(source_path: str) -> Optional[str]:
    """
    SYNCHRONOUSLY re-encodes a video to H.264/AAC.
    Intended to be run in a separate thread via asyncio.to_thread.
    """
    output_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix="_reencoded.mp4", delete=False) as temp_file:
            output_path = temp_file.name
        
        logger.info(f"Starting synchronous re-encode of '{os.path.basename(source_path)}'...")
        # Do NOT use -hwaccel. Let ffmpeg use the robust software decoder,
        # and only use the GPU for encoding via the -c:v flag.
        args = ['ffmpeg', '-y'] 
        args.extend(['-i', source_path])

        h264_encoder = HARDWARE_ENCODERS['h264']
        if h264_encoder == 'h264_nvenc':
            args.extend(['-c:v', 'h264_nvenc', '-preset', 'p5', '-cq', '23', '-profile:v', 'main'])
        elif h264_encoder == 'h264_qsv':
            args.extend(['-c:v', 'h264_qsv', '-preset', 'fast', '-global_quality', '23', '-profile:v', 'main'])
        else: # libx264
            args.extend(['-c:v', 'libx264', '-preset', 'fast', '-crf', '23'])

        args.extend(['-acodec', 'aac', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', output_path])
        subprocess.run(args, capture_output=True, check=True, encoding='utf-8', errors='ignore')
        logger.info(f"Successfully re-encoded video. Temp file: {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg failed to re-encode {source_path}. Error: {e.stderr.strip()}")
        if output_path and os.path.exists(output_path): os.remove(output_path)
        return None
    except Exception as e:
        logger.exception(f"Unexpected error during re-encoding of {source_path}: {e}")
        if output_path and os.path.exists(output_path): os.remove(output_path)
        return None