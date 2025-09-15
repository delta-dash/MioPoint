# utilsVideo.py
import asyncio
import json
import os
import shutil
import subprocess
import tempfile
from typing import Optional

# Project-specific imports
import logging
from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query
from utilsEncryption import calculate_file_hash
from src.websocket.HelperWebsocket import get_websocket_manager

# Import the TaskManager for decorating the worker function
from TaskManager import task_registry, Priority

logger = logging.getLogger("UtilsVideo")
logger.setLevel(logging.DEBUG)
config = get_config()

# --- SHARED STATE FOR TASK DEDUPLICATION ---
# This set holds file IDs that are either waiting in the queue or are actively being processed.
# It's used by the scheduling function (in tasks.py) to prevent queuing the same job multiple times.
QUEUED_OR_PROCESSING_VIDEO_IDS = set()

# in a new utility file, or at the top of utilsVideo.py

import subprocess
import functools

@functools.lru_cache(maxsize=1) # Cache the result so we only check once
def get_best_ffmpeg_accel() -> str:
    """
    Checks the installed ffmpeg for available hardware encoders and returns the best option.
    Preference Order: NVIDIA NVENC -> Intel QSV -> CPU.
    """

    try:
        # 1. Check for NVIDIA NVENC
        result = subprocess.run(
            ['ffmpeg', '-encoders'], 
            capture_output=True, 
            text=True, 
            check=True
        )
        if 'h264_nvenc' in result.stdout:
            logger.info("NVIDIA NVENC encoder found. Will use for hardware acceleration.")
            return 'nvidia'
            
        # 2. Check for Intel Quick Sync Video (QSV)
        if 'h264_qsv' in result.stdout:
            logger.info("Intel QSV encoder found. Will use for hardware acceleration.")
            return 'intel_qsv'
            
        # 3. Default to CPU
        logger.info("No supported hardware encoders found. Defaulting to CPU (libx24).")
        return 'cpu'

    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        logger.error(f"Could not run ffmpeg to check for encoders. Defaulting to CPU. Error: {e}")
        return 'cpu'

# Get the best available mode when the module is loaded.
BEST_ACCEL_MODE = get_best_ffmpeg_accel()
# In utilsVideo.py

async def select_best_streams(video_path: str) -> tuple[Optional[int], Optional[int]]:
    """
    Probes a video file using ffprobe to find the best video and audio streams.
    
    'Best' is determined by:
    - Video: The one that is NOT an attached picture and has the longest duration.
    - Audio: The one that is not a commentary track, preferring English, then Japanese, then the first available.

    Returns a tuple of (video_stream_index, audio_stream_index).
    """
    logger.info(f"Probing '{os.path.basename(video_path)}' to select the best streams...")
    args = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path]
    
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            logger.error(f"ffprobe failed for {video_path}: {stderr.decode()}")
            # Fallback to default streams if probe fails
            return 0, 1

        data = json.loads(stdout)
        streams = data.get('streams', [])

        # --- Video Stream Selection Logic ---
        video_streams = []
        for s in streams:
            # Must be a video stream and not an attached picture (like cover art)
            if s.get('codec_type') == 'video' and s.get('disposition', {}).get('attached_pic', 0) == 0:
                # Get duration, default to 0 if not present
                duration = float(s.get('duration', 0))
                video_streams.append({'index': s['index'], 'duration': duration})
        
        best_video_stream = max(video_streams, key=lambda x: x['duration']) if video_streams else None
        best_video_index = best_video_stream['index'] if best_video_stream else None

        # --- Audio Stream Selection Logic ---
        audio_streams = []
        for s in streams:
            if s.get('codec_type') == 'audio' and s.get('disposition', {}).get('comment', 0) == 0:
                lang = s.get('tags', {}).get('language', 'und').lower()
                # Assign a score for sorting: eng=3, jpn=2, others=1
                score = 3 if lang == 'eng' else 2 if lang in ('jpn', 'jp') else 1
                audio_streams.append({'index': s['index'], 'score': score})

        # Sort by score (descending), so the best language is first
        if audio_streams:
            audio_streams.sort(key=lambda x: x['score'], reverse=True)
            best_audio_index = audio_streams[0]['index']
        else:
            best_audio_index = None

        logger.info(f"Selected streams: Video Index={best_video_index}, Audio Index={best_audio_index}")
        
        # We must return an integer index for the map to work.
        # If no audio is found, we can't map it. Let's find the first audio stream as a fallback.
        if best_audio_index is None:
            first_audio = next((s['index'] for s in streams if s.get('codec_type') == 'audio'), None)
            best_audio_index = first_audio

        if best_video_index is None:
            logger.error(f"No valid video stream found in {video_path}")
            return None, None # Signal failure
            
        return best_video_index, best_audio_index

    except Exception as e:
        logger.exception(f"Error probing streams in {video_path}: {e}")
        return None, None
def timecode_to_seconds(timecode: str) -> float:
    """
    Converts a timecode string (HH:MM:SS.ms, MM:SS.ms, or SS.ms) to seconds.
    This version is robust and handles multiple formats.
    """
    if not timecode:
        return 0.0

    seconds = 0.0
    try:
        parts = timecode.split(':')
        sec_parts = parts[-1].split('.')
        seconds += int(sec_parts[0])
        if len(sec_parts) > 1:
            seconds += float(f"0.{sec_parts[1]}")
        if len(parts) > 1:
            seconds += int(parts[-2]) * 60
        if len(parts) > 2:
            seconds += int(parts[-3]) * 3600
    except (ValueError, IndexError):
        logger.info(f"Warning: Could not parse malformed timecode '{timecode}'")
        return 0.0
    return seconds

def format_timecode_for_display(timecode: str) -> str:
    """
    Removes the leading '00:' hour part from a timecode if it exists.
    """
    if not timecode:
        return ""
    if timecode.startswith("00:"):
        return timecode[3:]
    else:
        return timecode


# --- TASK MANAGER WORKER FUNCTION ---

# In utilsVideo.py, replace your existing function with this one

@task_registry.queue_task("video", priority=Priority.HIGH)
async def convert_and_update_video(file_id: int, progress_cache: dict):
    """
    Finds a video, transcodes it to H.264 using the best available encoder
    (auto-detecting GPU acceleration), reports progress, updates its DB record,
    and caches the new file. This is the main worker function run by the TaskManager.
    """
    new_temp_path = None
    try:
        # --- Pre-flight check: Has this been converted already? ---
        async with AsyncDBContext() as conn:
            check = await execute_db_query(
                conn, "SELECT transcoded_path FROM files WHERE id = ?", (file_id,), fetch_one=True
            )
        if check and check[0] and os.path.exists(check[0]):
            logger.info(f"Skipping conversion for file ID {file_id}; a valid transcoded file already exists.")
            return

        # --- Initial DB read for source path and duration ---
        cfg = config.data
        transcoded_cache_dir = os.path.abspath(cfg["TRANSCODED_CACHE_DIR"])
        os.makedirs(transcoded_cache_dir, exist_ok=True)

        async with AsyncDBContext() as conn:
            result = await execute_db_query(
                conn, "SELECT stored_path, duration_seconds FROM files WHERE id = ?", (file_id,), fetch_one=True
            )
        if not result or not result[0]:
            logger.error(f"Conversion failed: File ID {file_id} not found or has no stored path.")
            return

        original_stored_path, duration_seconds = result
        if not duration_seconds or duration_seconds <= 0:
            logger.warning(f"Cannot calculate progress for file ID {file_id}: duration is unknown.")
            duration_seconds = 0

        # --- Probe for the best video and audio streams ---
        video_idx, audio_idx = await select_best_streams(original_stored_path)
        if video_idx is None:
            raise Exception(f"Could not find a valid video stream to convert in file ID {file_id}.")

        # --- Build FFmpeg command with Auto-Detected Hardware Acceleration ---
        new_temp_path = tempfile.mktemp(suffix="_reencoded.mp4")
        
        # Base arguments (pre-input)
        args = ['ffmpeg', '-y', '-progress', 'pipe:1']

        # Add hardware decoding for a performance boost if using NVIDIA
        if BEST_ACCEL_MODE == 'nvidia':
            args.extend(['-hwaccel', 'cuda'])
        
        # Input file
        args.extend(['-i', original_stored_path])

        # Use the dynamically selected streams from ffprobe
        args.extend(['-map', f'0:{video_idx}'])
        if audio_idx is not None:
            args.extend(['-map', f'0:{audio_idx}'])
        else:
            args.extend(['-an']) # -an = audio-no
            logger.warning(f"No audio stream found for file ID {file_id}. Transcoding video-only.")

        # Encoder-specific arguments (refactored for clarity)
        video_encoder_args = []
        if BEST_ACCEL_MODE == 'nvidia':
            logger.info(f"Using auto-detected NVIDIA NVENC for file ID {file_id}.")
            video_encoder_args.extend(['-c:v', 'h264_nvenc', '-preset', 'fast', '-cq', '23'])
        elif BEST_ACCEL_MODE == 'intel_qsv':
            logger.info(f"Using auto-detected Intel QSV for file ID {file_id}.")
            video_encoder_args.extend(['-c:v', 'h264_qsv', '-preset', 'fast', '-global_quality', '23'])
        else: # Default to CPU
            logger.info(f"Using auto-detected CPU (libx264) for file ID {file_id}.")
            video_encoder_args.extend(['-c:v', 'libx264', '-preset', 'fast', '-crf', '23'])
        
        args.extend(video_encoder_args)
        
        # Common video/audio/container arguments
        args.extend(['-pix_fmt', 'yuv420p', '-vf', 'scale=trunc(in_w/2)*2:trunc(in_h/2)*2'])
        if audio_idx is not None:
            args.extend(['-c:a', 'aac']) # Add audio codec only if an audio stream is mapped
        args.extend(['-movflags', '+faststart'])
        
        # Output file
        args.append(new_temp_path)

        logger.info(f"Executing FFmpeg command: {' '.join(args)}")

        # --- Run and Monitor FFmpeg ---
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        progress_cache[file_id] = 0.0

        while True:
            line = await proc.stdout.readline()
            if not line: break
            line = line.decode('utf-8').strip()
            if line.startswith('out_time_ms=') and duration_seconds > 0:
                try:
                    current_ms = int(line.split('=')[1].strip())
                    progress = round((current_ms / (duration_seconds * 1000000)) * 100, 2)
                    progress_cache[file_id] = min(progress, 100.0)
                except (ValueError, IndexError):
                    continue

        await proc.wait()
        if proc.returncode != 0:
            stderr_data = await proc.stderr.read()
            raise Exception(f"FFmpeg process failed. Stderr: {stderr_data.decode(errors='ignore')}")

        # --- Update Database and Filesystem ---
        new_hash = calculate_file_hash(new_temp_path)
        new_cache_path = os.path.join(transcoded_cache_dir, f"{new_hash}.mp4")
        shutil.move(new_temp_path, new_cache_path)
        new_temp_path = None # Prevent cleanup of the final file in `finally`

        async with AsyncDBContext() as conn:
            await execute_db_query(
                conn, "UPDATE files SET transcoded_path = ? WHERE id = ?", (new_cache_path, file_id)
            )

        logger.info(f"Successfully created transcoded cache for file ID {file_id} at {new_cache_path}.")

        # --- Notify clients via WebSocket ---
        manager = get_websocket_manager()
        room_name = f"file-viewers:{file_id}"
        
        cache_busted_url = f"/api/{file_id}/media?_v=nocache"

        message = {"type": "transcoding_complete", "payload": {"file_id": file_id, "url": cache_busted_url}}
        await manager.broadcast_to_room(room_name, message)
        logger.info(f"Sent transcoding_complete notification to room {room_name}")

    except Exception as e:
        logger.error(f"Error during conversion of file ID {file_id}: {e}", exc_info=True)
        if file_id in progress_cache:
            progress_cache[file_id] = {"status": "error", "message": str(e)}
        raise # Re-raise exception so the TaskManager Future reflects the failure
    finally:
        # --- CRITICAL: Release the lock and clean up ---
        if file_id in QUEUED_OR_PROCESSING_VIDEO_IDS:
            QUEUED_OR_PROCESSING_VIDEO_IDS.remove(file_id)
            logger.info(f"Conversion task for file ID {file_id} finished or failed. Lock released.")
        
        if new_temp_path and os.path.exists(new_temp_path):
            logger.warning(f"Cleaning up failed temporary file: {new_temp_path}")
            os.remove(new_temp_path)

# --- ASYNC UTILITY FUNCTIONS ---

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


async def stream_transcoded_video(source_path: str):
    """
    An async generator that uses ffmpeg to transcode a video on-the-fly to a
    web-compatible format (H.264/AAC in a fragmented MP4 container).

    This function is designed to be robust:
    - It intelligently selects the best video and audio streams using a helper.
    - It gracefully handles probing failures on corrupt or invalid files.
    - It uses ffmpeg settings optimized for real-time streaming and compatibility.
    - It properly cleans up the ffmpeg process and provides clear error logging.
    """
    logger.info(f"Starting on-the-fly stream for: '{os.path.basename(source_path)}'")

    # --- Step 1: Probe the file to find the best streams to use ---
    # This is a critical pre-flight check.
    video_idx, audio_idx = await select_best_streams(source_path)

    # --- Step 2: Handle probing failure gracefully ---
    # If no valid video stream can be found, we cannot proceed.
    if video_idx is None:
        logger.error(
            f"On-the-fly stream failed for '{os.path.basename(source_path)}': "
            "Could not find a valid video stream during probing. File may be corrupt."
        )
        return  # Stop the generator. FastAPI will send a 0-byte response.

    # --- Step 3: Build the full ffmpeg command ---
    args = [
        'ffmpeg',
        '-i', source_path,
        '-hide_banner',  # Cleans up stderr logging
        '-loglevel', 'error', # Only log actual errors to stderr
    ]

    # Map the dynamically selected streams
    args.extend(['-map', f'0:{video_idx}'])
    if audio_idx is not None:
        args.extend(['-map', f'0:{audio_idx}'])
    else:
        # If no audio stream was found, explicitly tell ffmpeg not to include audio
        args.extend(['-an'])
        logger.warning(f"No valid audio stream found for '{os.path.basename(source_path)}'. Streaming video-only.")

    # Add transcoding and streaming arguments
    args.extend([
        '-f', 'mp4',
        '-vcodec', 'libx264',
        '-preset', 'veryfast',  # Prioritize speed for on-the-fly transcoding
        '-pix_fmt', 'yuv420p',  # Maximum browser compatibility
        # Ensures video dimensions are divisible by 2, a requirement for H.264
        '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
        # CRITICAL: These flags create a fragmented MP4 ideal for streaming
        '-movflags', 'frag_keyframe+empty_moov+faststart',
        '-y',      # Overwrite output (not strictly needed for a pipe, but good practice)
        'pipe:1'   # Direct output to standard output (stdout)
    ])

    # Add audio codec arguments only if an audio stream is being processed
    if audio_idx is not None:
        args.extend(['-acodec', 'aac', '-ac', '2']) # aac is the standard for web video

    logger.info(f"Executing on-the-fly streaming command: {' '.join(args)}")

    # --- Step 4: Execute the command and stream the output ---
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE  # Capture stderr to log real errors
    )

    try:
        # Read from ffmpeg's stdout in chunks and yield them to the client
        while True:
            chunk = await proc.stdout.read(65536)  # Read 64KB at a time
            if not chunk:
                break  # End of stream
            yield chunk
    except asyncio.CancelledError:
        # This occurs when the client disconnects. It's expected behavior.
        logger.warning(f"Client disconnected. Terminating stream for '{os.path.basename(source_path)}'")
    finally:
        # --- Step 5: Cleanup and Error Reporting ---
        # Ensure the ffmpeg process is terminated, no matter what.
        if proc.returncode is None:
            proc.terminate()
            await proc.wait()

        # Read any error messages from stderr
        stderr_output = await proc.stderr.read()
        stderr_text = stderr_output.decode('utf-8', 'ignore')

        # Log an error ONLY if ffmpeg exited with an error code AND it wasn't
        # a "Broken pipe" error, which is expected when a client disconnects.
        if proc.returncode != 0 and "Broken pipe" not in stderr_text:
            logger.error(
                f"FFmpeg streaming process for '{os.path.basename(source_path)}' exited with code {proc.returncode}.\n"
                f"FFmpeg Stderr: {stderr_text.strip()}"
            )


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
        args = [
            'ffmpeg', '-y', '-i', source_path, '-vcodec', 'libx264', '-acodec', 'aac',
            '-preset', 'fast', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', output_path
        ]
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