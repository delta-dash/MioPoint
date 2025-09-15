



from datetime import datetime
import json
import logging
import os
import shutil
from typing import List, Optional

import aiosqlite

from ConfigMedia import get_config
from EncryptionManager import encrypt
from src.db_utils import AsyncDBContext, execute_db_query
from src.log.HelperLog import log_event_async
from src.media.thumbnails import THUMBNAIL_FORMAT, create_image_thumbnail, create_pdf_thumbnail, create_video_thumbnail
from src.models import MediaData
from src.roles.rbac_manager import get_everyone_role_id
from src.tag_utils import _get_table_names


config = get_config()  # <-- Directly access the data dictionary
logger = logging.getLogger("MediaDB")
logger.setLevel(logging.DEBUG)


# ==============================================================================
# 2. Service Classes
# ==============================================================================




from src.media.media_repository import DatabaseService
