import logging
import os
import shutil

from ConfigMedia import get_config
from EncryptionManager import encrypt
from src.media.thumbnails import THUMBNAIL_FORMAT, create_image_thumbnail, create_pdf_thumbnail, create_video_thumbnail
from src.models import MediaData

config = get_config()
logger = logging.getLogger("FilePersistence")
logger.setLevel(logging.DEBUG)

class FilePersistenceService:
    """Handles storing files and creating thumbnails. (Sync operations)"""
    def __init__(self, filestore_dir: str, thumbnail_dir: str):
        self.filestore_dir = filestore_dir
        self.thumbnail_dir = thumbnail_dir
        os.makedirs(self.filestore_dir, exist_ok=True)
        os.makedirs(self.thumbnail_dir, exist_ok=True)

    def store(self, media_data: MediaData) -> str:
        """
        Copies or encrypts the source file and returns the final stored path.
        Note: This method may modify media_data.is_encrypted if encryption fails.
        """
        source_path = media_data.sanitized_content_path
        stored_filename = f"{media_data.file_hash}{media_data.extension}"
        stored_path = os.path.join(self.filestore_dir, stored_filename)

        try:
            if media_data.is_encrypted:
                password = config.get('password', '')
                if password:
                    encrypt(source_path, stored_path, password)
                    logger.info(f"File encrypted with system password and stored at: {stored_path}")
                else:
                    logger.warning(
                        f"is_encrypted flag is set for {source_path}, but no system password found in config. "
                        "File will be stored UNENCRYPTED."
                    )
                    # IMPORTANT: Update the DTO to reflect the actual state for DB accuracy
                    media_data.is_encrypted = False
                    shutil.copy2(source_path, stored_path)
            else:
                shutil.copy2(source_path, stored_path)
                logger.info(f"File copied and stored at: {stored_path}")
            
            return stored_path
        except Exception as e:
            logger.error(f"Failed to store file from {source_path}: {e}")
            raise

    def create_thumbnail(self, media_data: MediaData):
        """Generates a thumbnail for the stored file."""
        if not media_data.stored_path:
            logger.warning("Cannot create thumbnail, stored_path is not set.")
            return

        thumb_path = os.path.join(self.thumbnail_dir, f"{media_data.file_hash}.{THUMBNAIL_FORMAT.lower()}")
        created = False
        if media_data.file_type == "image":
            created = create_image_thumbnail(media_data.stored_path, thumb_path)
        elif media_data.file_type == "video":
            created = create_video_thumbnail(media_data.stored_path, thumb_path)
        elif media_data.file_type == "pdf":
            created = create_pdf_thumbnail(media_data.stored_path, thumb_path)

        if created:
            logger.info(f"Thumbnail created for hash {media_data.file_hash} at {thumb_path}.")
        else:
            logger.warning(f"Failed to create thumbnail for hash {media_data.file_hash}.")
