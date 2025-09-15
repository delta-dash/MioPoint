
import logging
from PIL import Image, ImageDraw, ImageFont
import cv2
from pypdf import PdfReader

logger = logging.getLogger("Thumbnails")

THUMBNAIL_SIZE = (256, 256)
THUMBNAIL_FORMAT = "WEBP"  # Or JPEG, PNG

def create_image_thumbnail(src_path: str, thumb_path: str) -> bool:
    try:
        with Image.open(src_path) as img:
            img.thumbnail(THUMBNAIL_SIZE)
            img.convert("RGB").save(thumb_path, THUMBNAIL_FORMAT)
        return True
    except Exception as e:
        logger.error(f"Failed to create image thumbnail for {src_path}: {e}")
        return False

def create_video_thumbnail(src_path: str, thumb_path: str) -> bool:
    try:
        cap = cv2.VideoCapture(src_path)
        cap.set(cv2.CAP_PROP_POS_MSEC, 1000) # Get frame at 1 second
        success, image = cap.read()
        cap.release()
        if success:
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            pil_img = Image.fromarray(image_rgb)
            pil_img.thumbnail(THUMBNAIL_SIZE)
            pil_img.save(thumb_path, THUMBNAIL_FORMAT)
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to create video thumbnail for {src_path}: {e}")
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
            img.save(thumb_path, THUMBNAIL_FORMAT)
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to create PDF placeholder thumbnail for {src_path}: {e}")
        return False
