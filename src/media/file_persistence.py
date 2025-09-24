# file_persistence.py
import logging
import os
import shutil

from ConfigMedia import get_config
from EncryptionManager import encrypt
from src.media.thumbnails import create_image_thumbnail, create_pdf_thumbnail, create_text_thumbnail, create_video_thumbnail, create_video_frame_thumbnail
from src.media.schemas import MediaData

config = get_config()
logger = logging.getLogger("FilePersistence")
logger.setLevel(logging.DEBUG)

class FilePersistenceService:
    """
    Handles physical file operations: storing files according to configuration
    (in-place or copied to a filestore) and creating content-based thumbnails.
    All operations are synchronous and should be run in a thread pool executor.
    """
    def __init__(self, filestore_dir: str, thumbnail_dir: str):
        self.filestore_dir = filestore_dir
        self.thumbnail_dir = thumbnail_dir
        os.makedirs(self.filestore_dir, exist_ok=True)
        os.makedirs(self.thumbnail_dir, exist_ok=True)

    def store(self, media_data: MediaData) -> str:
        """
        Ensures the media file is in its final storage location.

        Behavior is determined by the `COPY_FILE_TO_FOLDER` config setting:
        - If False: Returns the original path, assuming in-place processing.
        - If True: Copies the file to an organized filestore structure based
                    on its hash, handles optional encryption, and returns the new path.
        """
        source_path = media_data.initial_physical_path

        # Determine if the file MUST be moved/copied because it's from a temporary source.
        is_temporary_source = media_data.ingest_source in ['upload', 'conversion']

        # If COPY_FILE_TO_FOLDER is false AND it's not a temporary source, then process in-place.
        if not config.get('COPY_FILE_TO_FOLDER', False) and not is_temporary_source:
            logger.debug(f"Processing in-place for source '{media_data.ingest_source}'. Final path is original path: {source_path}")
            return source_path

        # --- All other cases require moving/copying to the structured filestore ---
        file_hash = media_data.file_hash

        # Organize files in subdirectories to avoid massive folders (e.g., /ab/cd/abcd...)
        # This is a common and effective strategy for large-scale file storage.
        dest_dir = os.path.join(self.filestore_dir, file_hash[:2], file_hash[2:4])
        os.makedirs(dest_dir, exist_ok=True)

        # Use the full hash and original extension for the filename
        dest_path = os.path.join(dest_dir, f"{file_hash}{media_data.extension}")

        if os.path.exists(dest_path):
            logger.debug(f"File with hash {file_hash} already exists in filestore at {dest_path}. Skipping move/copy.")
            # If the source was temporary, we should clean it up since we're not moving it.
            if is_temporary_source:
                try:
                    os.remove(source_path)
                    logger.debug(f"Removed temporary source file: {source_path}")
                except OSError as e:
                    logger.warning(f"Could not remove temporary source file {source_path}: {e}")
        else:
            # If it's from a temporary source, we MOVE it. Otherwise (from WATCH_DIRS with COPY_FILE_TO_FOLDER=True), we COPY it.
            if is_temporary_source:
                logger.info(f"Moving '{os.path.basename(source_path)}' from temporary location to filestore: {dest_path}")
                shutil.move(source_path, dest_path)
            else:
                logger.info(f"Copying '{os.path.basename(source_path)}' to filestore: {dest_path}")
                shutil.copy2(source_path, dest_path) # copy2 preserves metadata

            # Handle optional encryption after copying
            if config.get('ENCRYPT_FILES', False):
                logger.info(f"Encrypting file in filestore: {dest_path}")
                # The `encrypt` function writes to a new file. To do it "in-place", we need a temp file.
                temp_unencrypted_path = dest_path + ".unenc"
                try:
                    shutil.move(dest_path, temp_unencrypted_path)
                    encrypt(temp_unencrypted_path, dest_path, config['ENCRYPTION_KEY'])
                    media_data.is_encrypted = True
                finally:
                    if os.path.exists(temp_unencrypted_path):
                        os.remove(temp_unencrypted_path)

        return dest_path

    def create_thumbnail(self, media_data: MediaData):
        """
        Generates a thumbnail for the media content, identified by its hash.
        This operation is idempotent; it will not re-create an existing thumbnail.
        """
        if not media_data.stored_path:
            logger.warning("Cannot create thumbnail, stored_path is not set.")
            return

        # Thumbnail name is based on content hash, ensuring one thumbnail per unique file.
        thumb_format = config.get('THUMBNAIL_FORMAT', 'WEBP').lower()
        thumb_path = os.path.join(self.thumbnail_dir, f"{media_data.file_hash}.{thumb_format}")

        # Optimization: If the thumbnail for this content already exists, do nothing.
        if os.path.exists(thumb_path):
            logger.debug(f"Thumbnail for hash {media_data.file_hash} already exists. Skipping creation.")
            return

        created = False
        if media_data.file_type == "image":
            created = create_image_thumbnail(media_data.stored_path, thumb_path)
        elif media_data.file_type == "video":
            created = create_video_thumbnail(media_data.stored_path, thumb_path)
        elif media_data.file_type == "pdf":
            created = create_pdf_thumbnail(media_data.stored_path, thumb_path)
        elif media_data.file_type == "text":
            created = create_text_thumbnail(media_data.stored_path, thumb_path)

        if created:
            logger.info(f"Thumbnail created for hash {media_data.file_hash} at {thumb_path}.")
        # No warning on failure, as the create_*_thumbnail functions log their own errors.