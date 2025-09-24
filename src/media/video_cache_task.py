import asyncio
import logging

from TaskManager import task_registry, Priority
from src.media.utilsVideo import convert_and_update_video_content as cache_worker_function

logger = logging.getLogger("VideoCacheTasks")
logger.setLevel(logging.DEBUG)

_PROCESSING_LOCK = asyncio.Lock()
_PROCESSING_CONTENT_IDS = set()


async def _run_transcode_and_release_lock(instance_id: int, content_id: int, source_path: str):
    """A wrapper to run the task and ensure the lock is released after the task completes."""
    try:
        # The worker function `convert_and_update_video_content` is already async
        await task_registry.get_manager("video-conversion").schedule_task(
            func=cache_worker_function,
            args=(instance_id, content_id, source_path),
            kwargs={},
            priority=Priority.LOW  # Lower priority for background caching
        )
    finally:
        async with _PROCESSING_LOCK:
            if content_id in _PROCESSING_CONTENT_IDS:
                _PROCESSING_CONTENT_IDS.remove(content_id)
                logger.info(f"Released lock for content ID {content_id} after web-ready transcode task completion.")


async def schedule_webready_transcode(instance_id: int, content_id: int, source_path: str):
    """
    Schedules a video for background transcoding to a web-ready format if it's not already being processed.
    This is idempotent at the content level.
    """
    async with _PROCESSING_LOCK:
        if content_id in _PROCESSING_CONTENT_IDS:
            logger.info(f"Web-ready transcode for content ID {content_id} is already queued or in progress. Ignoring request.")
            return
        _PROCESSING_CONTENT_IDS.add(content_id)
        logger.info(f"Claimed content ID {content_id} for web-ready transcoding. Scheduling task.")

    try:
        # Create a fire-and-forget task that handles running the worker and releasing the lock
        asyncio.create_task(
            _run_transcode_and_release_lock(instance_id, content_id, source_path)
        )
    except Exception as e:
        logger.error(f"Failed to schedule web-ready transcode task for content ID {content_id}: {e}", exc_info=True)
        # If scheduling fails, release the lock immediately
        async with _PROCESSING_LOCK:
            if content_id in _PROCESSING_CONTENT_IDS:
                _PROCESSING_CONTENT_IDS.remove(content_id)
        raise