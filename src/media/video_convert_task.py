import asyncio
import logging
from typing import Dict

from TaskManager import task_registry, Priority
from src.media.actions import run_media_conversion_and_ingest as conversion_worker_function

logger = logging.getLogger("VideoTasks")
logger.setLevel(logging.DEBUG)

# This set tracks INSTANCE_IDs to prevent duplicate conversions for the same file instance.
_PROCESSING_LOCK = asyncio.Lock()
_PROCESSING_INSTANCE_IDS = set()


async def schedule_media_conversion(
    instance_id: int,
    source_path: str,
    original_filename: str,
    file_type: str,
    conversion_options: Dict,
    scanner_app_state: Dict,
    user_id: int,
    task_type: str = 'conversion'
):
    """
    Schedules a media file for background conversion if it's not already being processed.
    """
    async with _PROCESSING_LOCK:
        if instance_id in _PROCESSING_INSTANCE_IDS:
            logger.info(f"Conversion for instance ID {instance_id} is already queued or in progress. Ignoring request.")
            return

        _PROCESSING_INSTANCE_IDS.add(instance_id)
        logger.info(f"Claimed instance ID {instance_id} for conversion. Scheduling task.")

    try:
        asyncio.create_task(
            _run_conversion_and_release_lock(
                instance_id, source_path, original_filename, file_type, 
                conversion_options, scanner_app_state, user_id, task_type
            )
        )
    except Exception as e:
        logger.error(f"Failed to schedule conversion task for instance ID {instance_id}: {e}", exc_info=True)
        async with _PROCESSING_LOCK:
            if instance_id in _PROCESSING_INSTANCE_IDS:
                _PROCESSING_INSTANCE_IDS.remove(instance_id)
        raise

async def _run_conversion_and_release_lock(
    instance_id: int,
    source_path: str,
    original_filename: str,
    file_type: str,
    conversion_options: Dict,
    scanner_app_state: Dict,
    user_id: int,
    task_type: str
):
    """A wrapper to run the task and ensure the lock is released."""
    try:
        await task_registry.get_manager("video-conversion").schedule_task(
            func=conversion_worker_function,
            args=(instance_id, source_path, original_filename, file_type, conversion_options, scanner_app_state, user_id, task_type),
            kwargs={},
            priority=Priority.NORMAL
        )
    finally:
        async with _PROCESSING_LOCK:
            if instance_id in _PROCESSING_INSTANCE_IDS:
                _PROCESSING_INSTANCE_IDS.remove(instance_id)
                logger.info(f"Released lock for instance ID {instance_id} after conversion task completion.")