# dispatcher.py
import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
import time

from TaskManager import task_registry, Priority
from src.media.FileProcessors import (
    get_file_type_and_extension,
    process_image_task,
    process_video_task,
    process_text_task,
    process_generic_task,
    update_media_task # <-- Import update_media_task at the top
)
from src.media.schemas import ProcessingJob
from ClipManager import get_clip_manager
from ImageTagger import get_tagger
from utils import get_original_filename

logger = logging.getLogger(__name__)

async def dispatcher_worker(name: str, app_state: dict, queue: asyncio.Queue, executor: ThreadPoolExecutor):
    """
    Pulls file paths from the intake queue, determines the correct processing
    task (e.g., new image, updated video), and dispatches it to the appropriate
    worker pool via the TaskManager.
    """
    dispatcher_logger = logging.getLogger(name)
    dispatcher_logger.setLevel(logging.DEBUG)
    loop = asyncio.get_running_loop()
    dispatcher_logger.debug("Ready and waiting for intake tasks.")

    while True:
        await app_state["processing_enabled_event"].wait()

        # By splitting the try/except blocks, we can ensure that task_done() is
        # only called if queue.get() was successful, preventing the "too many times"
        # error on graceful shutdown.
        try:
            ingest_data = await queue.get()
        except asyncio.CancelledError:
            dispatcher_logger.info("Dispatcher task cancelled.")
            break  # Exit the loop cleanly

        try:
            path, ingest_source, *extra = ingest_data
            
            if not path or not os.path.exists(path):
                dispatcher_logger.warning(f"File vanished before processing: {path}")
                continue

            # --- 1. Determine original_filename and uploader_id early ---
            # This is the key fix: we get the original filename here, so we can use it for type detection.
            uploader_id = None
            original_filename = None
            context = {}

            if ingest_source == 'conversion':
                uploader_id = extra[0] if len(extra) > 0 else None
                original_filename = extra[1] if len(extra) > 1 else get_original_filename(path)
                context['original_content_id'] = extra[2] if len(extra) > 2 else None
            elif ingest_source == 'upload':
                uploader_id = extra[0] if len(extra) > 0 else None
                original_filename = extra[1] if len(extra) > 1 else get_original_filename(path)
            else: # watcher_create, folder_scan, watcher_update
                uploader_id = None
                original_filename = get_original_filename(path)

            # --- 2. Identify Target Task ---
            target_task_func = None
            manager_name = None

            if ingest_source == "watcher_update":
                target_task_func = update_media_task
                manager_name = "image"
            else: # For all new files (upload, watcher_create, initial_scan)
                file_type_map = app_state["config"].get("FILE_TYPE_MAPPINGS", {})
                file_type, _ = get_file_type_and_extension(original_filename, file_type_map)
                enabled_types = app_state["config"].get("ENABLED_FILE_TYPES", [])
                if file_type not in enabled_types:
                    dispatcher_logger.info(f"Skipping '{original_filename}' because file type '{file_type}' is not enabled in config.")
                    continue

                # Map file type to its processing task and worker pool
                task_map = {
                    "image": ("image", process_image_task),
                    "video": ("video", process_video_task),
                    "text": ("text", process_text_task),
                    "pdf": ("generic", process_generic_task), # PDF uses the generic processor
                    "generic": ("generic", process_generic_task),
                }
                
                if file_type in task_map:
                    manager_name, target_task_func = task_map[file_type]
                else:
                    dispatcher_logger.warning(f"Unknown file type '{file_type}' for {path}. Using generic processor.")
                    manager_name, target_task_func = "generic", process_generic_task

            # --- 3. Check if a valid task was found and if its manager is enabled ---
            if not target_task_func or not manager_name:
                dispatcher_logger.warning(f"Could not determine a processing task for {path}. Skipping.")
                continue

            manager = task_registry.get_manager(manager_name)
            if manager.num_workers <= 0:
                dispatcher_logger.info(f"Skipping: Processing for type '{manager_name}' is disabled.")
                continue

            dispatcher_logger.info(f"Dispatching '{original_filename}' (from {ingest_source}) to '{manager_name}' pool using task '{target_task_func.__name__}'.")

            # --- 4. Gather Resources and Build the Job ---
            current_config = app_state["config"]
            default_role_ids = app_state["default_visibility_role_ids"]

            # `uploader_id`, `original_filename`, and `context` are already determined above.

            tagger = await get_tagger(current_config["MODEL_TAGGER"])
            clip_manager = get_clip_manager()
            clip_model, clip_preprocess = await clip_manager.load_or_get_model(current_config["MODEL_REVERSE_IMAGE"])

            if not clip_model:
                dispatcher_logger.error(f"Could not load CLIP model. Skipping task for {original_filename}.")
                continue
            priority = Priority.HIGH if ingest_source.startswith("watcher") or ingest_source == "upload" else Priority.NORMAL

            job = ProcessingJob(
                # The executor is intentionally set to None here.
                # The ProcessingManager's worker will inject its own dedicated
                # ThreadPoolExecutor before running the task. This is critical for parallelism.
                executor=None,
                path=path,
                ingest_source=ingest_source,
                tagger_model_name=current_config["MODEL_TAGGER"],
                clip_model_name=current_config["MODEL_REVERSE_IMAGE"],
                tagger=tagger,
                clip_model=clip_model,
                clip_preprocess=clip_preprocess,
                uploader_id=uploader_id,
                default_visibility_roles=default_role_ids,
                original_filename=original_filename,
                app_state=app_state,
                priority=priority,
                context=context,
            )

            # --- 4. Schedule the Task ---
            asyncio.create_task(manager.schedule_task(target_task_func, args=(job,), kwargs={}, priority=priority))

        except FileNotFoundError as e:
            dispatcher_logger.warning(f"File vanished before dispatch, skipping: {path} ({e})")
        except Exception:
            dispatcher_logger.exception(f"A critical top-level error occurred while dispatching {path}")
        finally:
            # This is now in the 'finally' for the inner 'try', so it will always
            # be called if get() succeeded, but not if get() was cancelled.
            queue.task_done()
