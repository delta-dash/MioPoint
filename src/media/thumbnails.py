
import logging
from PIL import Image, ImageDraw, ImageFont, ImageOps
from pypdf import PdfReader
import subprocess

logger = logging.getLogger("Thumbnails")

THUMBNAIL_SIZE = (156, 156)

def timecode_to_msec(timecode: str) -> float:
    """Converts an 'HH:MM:SS.ms' timecode string to milliseconds."""
    try:
        parts = timecode.split(':')
        h = int(parts[0])
        m = int(parts[1])
        s_ms_parts = parts[2].split('.')
        s = int(s_ms_parts[0])
        ms = int(s_ms_parts[1]) if len(s_ms_parts) > 1 else 0
        return (h * 3600 + m * 60 + s) * 1000 + ms
    except (ValueError, IndexError) as e:
        logger.error(f"Could not parse timecode '{timecode}': {e}")
        return 0.0

def _get_video_duration_sync(video_path: str) -> float:
    """Synchronously probes a video file to get its duration in seconds."""
    args = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
    try:
        result = subprocess.run(args, capture_output=True, text=True, check=True, encoding='utf-8', errors='ignore')
        duration_str = result.stdout.strip()
        return float(duration_str) if duration_str and duration_str != 'N/A' else 0.0
    except (subprocess.CalledProcessError, FileNotFoundError, ValueError) as e:
        logger.error(f"ffprobe failed to get duration for {video_path}: {e}")
        return 0.0


def create_image_thumbnail(src_path: str, thumb_path: str) -> bool:
    try:
        with Image.open(src_path) as img:
            img.thumbnail(THUMBNAIL_SIZE)
            # The format is inferred from the thumb_path extension (e.g., .webp)
            img.convert("RGB").save(thumb_path)
        return True
    except Exception as e:
        logger.error(f"Failed to create image thumbnail for {src_path}: {e}")
        return False

def create_video_thumbnail(src_path: str, thumb_path: str) -> bool:
    """Creates a thumbnail from the start of a video using a direct ffmpeg call for robustness."""
    try:
        # Using ffprobe is more reliable for getting duration than OpenCV
        duration_sec = _get_video_duration_sync(src_path)
        if duration_sec <= 0:
             # Fallback for very short clips or if ffprobe fails
            seek_time = 0.1
        else:
            # Seek to 1 second, or halfway if shorter
            seek_time = min(1.0, duration_sec / 2.0)

        # Use ffmpeg directly to extract the frame. This is more robust than OpenCV
        # for complex codecs (like AV1), as it avoids issues with library-specific
        # hardware acceleration detection. Software decoding is the reliable default.
        cmd = [
            'ffmpeg', '-y', '-ss', str(seek_time), '-i', src_path,
            '-vframes', '1',
            '-vf', f'scale={THUMBNAIL_SIZE[0]}:-1', # Scale by width, maintain aspect
            thumb_path
        ]
        subprocess.run(cmd, check=True, capture_output=True, encoding='utf-8', errors='ignore')

    

        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create video thumbnail for {src_path} using ffmpeg. Stderr: {e.stderr.strip()}")
        return False
    except Exception as e:
        logger.error(f"Failed to create video thumbnail for {src_path}: {e}")
        return False

def create_video_frame_thumbnail(src_path: str, thumb_path: str, timecode: str) -> bool:
    """Creates a thumbnail from a specific timecode in a video using a direct ffmpeg call."""
    try:
        msec = timecode_to_msec(timecode)
        # Add a small offset to get a frame inside the scene, not exactly at the cut
        seek_time_seconds = (msec + 200) / 1000.0

        # Use ffmpeg directly for reliability. Seeking before the input file ('-ss ... -i ...') is fast.
        cmd = [
            'ffmpeg', '-y', '-ss', str(seek_time_seconds), '-i', src_path,
            '-vframes', '1', '-vf', f'scale={THUMBNAIL_SIZE[0]}:-1', thumb_path
        ]
        subprocess.run(cmd, check=True, capture_output=True, encoding='utf-8', errors='ignore')

        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create video frame thumbnail for {src_path} at {timecode}. Stderr: {e.stderr.strip()}")
        return False
    except Exception as e:
        logger.error(f"Failed to create video frame thumbnail for {src_path} at {timecode}: {e}")
        return False

def create_pdf_thumbnail(src_path: str, thumb_path: str) -> bool:
    try:
        reader = PdfReader(src_path)
        if len(reader.pages):
            page = reader.pages[0]
            text = page.extract_text()
            img = Image.new('RGB', THUMBNAIL_SIZE, color = 'grey')
            d = ImageDraw.Draw(img)
            try:
                font = ImageFont.truetype("arial.ttf", 20)
            except IOError:
                font = ImageFont.load_default()
            d.text((10,10), text, fill=(255,255,0), font=font)
            img.save(thumb_path)
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to create PDF placeholder thumbnail for {src_path}: {e}")
        return False

def create_text_thumbnail(src_path: str, thumb_path: str) -> bool:
    """Creates a placeholder thumbnail for a text file with a snippet of its content."""
    try:
        with open(src_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Read a few lines to display on the thumbnail
            lines = [f.readline().strip() for _ in range(10)] # Read up to 10 lines
            text_snippet = "\n".join(filter(None, lines))

        if not text_snippet:
            text_snippet = "[Empty File]"

        # Create a larger image for better text rendering, then downscale
        img = Image.new('RGB', (300, 300), color = (240, 240, 240)) # Light grey background
        d = ImageDraw.Draw(img)
        try:
            # Try to use a common monospaced font
            font = ImageFont.truetype("cour.ttf", 20) # Courier New is common
        except IOError:
            font = ImageFont.load_default()

        # Draw the text snippet onto the image
        d.multiline_text((15, 15), text_snippet, font=font, fill=(20, 20, 20)) # Dark text

        ImageOps.fit(img, THUMBNAIL_SIZE, Image.Resampling.LANCZOS).save(thumb_path)
        return True
    except Exception as e:
        logger.error(f"Failed to create text thumbnail for {src_path}: {e}")
        return False
