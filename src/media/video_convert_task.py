# app/tasks.py

import asyncio
import logging
from utilsVideo import QUEUED_OR_PROCESSING_VIDEO_IDS, convert_and_update_video

# Import the actual worker function from utilsVideo

logger = logging.getLogger("TaskScheduler")
logger.setLevel(logging.DEBUG)

# This is the function your API endpoint will call.
# It acts as a gatekeeper to the task queue.
async def schedule_video_conversion(file_id: int, progress_cache: dict):
    """
    Checks if a conversion is already queued/running for the given file_id.
    If not, it acquires the "lock" and schedules the conversion task.
    """
    # 1. Check the lock/deduplication set
    if file_id in QUEUED_OR_PROCESSING_VIDEO_IDS:
        logger.warning(
            f"Conversion for file ID {file_id} is already queued or in progress. Ignoring request."
        )
        # It's already running, so our job here is done.
        return

    try:
        # 2. Acquire the lock by adding the ID to the set
        QUEUED_OR_PROCESSING_VIDEO_IDS.add(file_id)
        logger.info(f"Lock acquired for file ID {file_id}. Scheduling conversion.")
        
        # 3. Schedule the task.
        # This call doesn't run the function immediately. It puts it in the "video"
        # manager's queue and returns a Future. We use asyncio.create_task to
        # run it in the background without blocking the API response.
        asyncio.create_task(convert_and_update_video(file_id, progress_cache))
        
    except Exception as e:
        # If scheduling itself fails for some reason, release the lock immediately.
        if file_id in QUEUED_OR_PROCESSING_VIDEO_IDS:
            QUEUED_OR_PROCESSING_VIDEO_IDS.remove(file_id)
        logger.error(f"Failed to schedule conversion for file ID {file_id}: {e}", exc_info=True)
        # Optionally re-raise the exception if the caller needs to know about the failure.
        raise