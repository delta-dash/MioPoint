# scanner_service.py
# This script is the core processor for the media tagging system.
# It performs the following actions:
# 1. Watches a directory for new image and video files.
# 2. Uses a single "intake" queue for discovered files.
# 3. Employs a pool of dispatcher workers to route files to the appropriate manager.
# 4. Uses dedicated Image and Video Processing Managers (from Manager*.py) to handle
#    task scheduling, priority, and concurrency for each media type.
# 5. A concurrency setting of 0 for a media type will disable processing for it.
# 6. For images: tags the image and stores the tags in the database. Animated GIFs are preserved.
# 7. For videos: detects scenes, tags a frame from each scene, and stores scene-specific data.
# 8. All metadata is stored in a single, unified SQLite database via MediaDB.py.
# 9. Manages ONNX model lifecycle, unloading them when idle to free VRAM.
# 10. Features colored, prefixed logging for clear, organized output from concurrent workers.
# 11. Configuration is loaded from config.ini and can be changed at runtime.



# Import Managers and Priority Enums
# This now uses the global singleton instance from TaskManager.py
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import logging
import os
import time
from watchdog.observers import Observer

from watchdog.events import FileSystemEventHandler
import signal
import magic
from ClipManager import get_clip_manager
from ConfigMedia import ConfigChangeHandler, ConfigManager, get_config
from ImageTagger import IDLE_UNLOAD_SECONDS, get_tagger, _cache_lock, _model_cache
from TaskManager import task_registry, Priority
from build_index import build_faiss_index
from src.media.FileProcessors import process_image_task, process_video_task
from src.roles.rbac_manager import get_role_by_name


magic_identifier = magic.Magic(mime=True)

# --- UPDATED: Standard logger setup ---
# Assuming you configure the root logger elsewhere (e.g., adding handlers, formatters)
# For standalone script, this is a good place for basic config.
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger("System")
logger.setLevel(logging.INFO)

processed_files = set()
processing_queue = asyncio.Queue() # This is now the "intake" queue
app_state = {
    "config": None,
    "media_observer": None,
    "processing_enabled_event": asyncio.Event(),
    "shutdown_event": asyncio.Event(),
    "executor": None,
    "intake_queue": processing_queue,
    "default_visibility_role_ids": [],
}
_app_state_lock = asyncio.Lock()

MIN_AGE_SECONDS = 5
IDLE_TIMEOUT = 1000 # This seems unused in your code, but I'll leave it


def safe_to_process(path: str) -> bool:
    try: return (time.time() - os.path.getmtime(path)) >= MIN_AGE_SECONDS
    except FileNotFoundError: return False

def is_media_file(path: str) -> bool:
    if not os.path.isfile(path) or path.endswith(('.crdownload', '.tmp', '.part')):
        return False
    try:
        mime_type = magic_identifier.from_file(path)
        return mime_type.startswith('image/') or mime_type.startswith('video/')
    except magic.MagicException as e:
        logger.warning(f"Could not identify file type for {os.path.basename(path)}: {e}")
        return False
    except Exception:
        return False

async def model_cleanup_task():
    while True:
        await asyncio.sleep(300)
        logger.info("Checking for idle models to unload...")
        current_time = time.time()
        async with _cache_lock:
            models_to_unload = [t for t in _model_cache.values() if t.is_loaded() and (current_time - t.last_used) > IDLE_UNLOAD_SECONDS]
            for tagger in models_to_unload: tagger.unload()


class MediaHandler(FileSystemEventHandler):
    def __init__(self, loop, queue):
        self.loop = loop
        self.queue = queue
    def _handle_event(self, path):
        if not is_media_file(path) or path in processed_files or not os.path.isfile(path): return
        logger.info(f"Discovered: {os.path.basename(path)}")
        ingest_info = (path, "watcher")
        self.loop.call_soon_threadsafe(self.queue.put_nowait, ingest_info)
        processed_files.add(path)
    def on_modified(self, event): self._handle_event(event.src_path)
    def on_moved(self, event): self._handle_event(event.dest_path)

async def initial_scan(queue: asyncio.Queue, watch_dirs: list):
    logger.info("Performing initial directory scan...")
    count = 0
    for directory in watch_dirs:
        if not os.path.isdir(directory):
            logger.warning(f"Skipping initial scan for non-existent directory: {directory}")
            continue
        logger.info(f"Scanning: {directory}")
        for filename in os.listdir(directory):
            full_path = os.path.join(directory, filename)
            if os.path.isfile(full_path) and is_media_file(full_path) and safe_to_process(full_path) and full_path not in processed_files:
                logger.info(f"Queuing from initial scan: {os.path.basename(full_path)}")
                ingest_info = (full_path, "folder scan")
                await queue.put(ingest_info)
                processed_files.add(full_path)
                count += 1
    logger.info(f"Initial scan complete. Queued {count} files for processing.")



async def dispatcher_worker(name: str, queue: asyncio.Queue, executor: ThreadPoolExecutor):
    """
    Pulls files from the intake queue, identifies their type, and dispatches them
    to the appropriate worker pool via the TaskManagerRegistry.
    """
    # --- UPDATED: Standard logger creation ---
    dispatcher_logger = logging.getLogger(name)
    dispatcher_logger.setLevel(logging.DEBUG)
    loop = asyncio.get_running_loop()
    dispatcher_logger.debug("Ready and waiting for intake tasks.")

    while True:
        await app_state["processing_enabled_event"].wait()

        ingest_data = await queue.get()
        path, ingest_source, *extra = ingest_data # Handle variable-length tuples
        uploader_id = extra[0] if extra else None

        if not path:
            queue.task_done()
            continue

        try:
            mime_type = await loop.run_in_executor(executor, magic_identifier.from_file, path)
            manager_name = None
            target_task_func = None

            if mime_type.startswith('image/'):
                manager_name = "image"
                target_task_func = process_image_task
            elif mime_type.startswith('video/'):
                manager_name = "video"
                target_task_func = process_video_task

            if not manager_name:
                # --- UPDATED: Log directly with context in the message ---
                dispatcher_logger.warning(f"Skipping: Unknown mime type '{mime_type}'.")
                continue

            manager = task_registry.get_manager(manager_name)
            if manager.max_workers <= 0:
                # --- UPDATED: Log directly with context in the message ---
                dispatcher_logger.info(f"Skipping: Processing for type '{manager_name}' is disabled.")
                if path in processed_files: processed_files.remove(path)
                continue

            # --- UPDATED: Log directly with context in the message ---
            dispatcher_logger.info(f"Dispatching '{os.path.basename(path)}' (from {ingest_source}) to '{manager_name}' pool.")

            async with _app_state_lock:
                current_config = app_state["config"]
                default_role_ids = app_state["default_visibility_role_ids"]

            tagger = await get_tagger(current_config["MODEL_TAGGER"])
            clip_manager = get_clip_manager()
            clip_model, clip_preprocess = await clip_manager.load_or_get_model(current_config["MODEL_REVERSE_IMAGE"])

            if not clip_model:
                 # --- UPDATED: Log directly ---
                dispatcher_logger.error(f"Could not load CLIP model. Skipping task for {os.path.basename(path)}.")
                continue

            # --- Create the final task function with all its arguments ---
            actual_task_func = functools.partial(
                target_task_func,
                executor=executor, path=path, ingest_source=ingest_source,
                tagger_model_name=current_config["MODEL_TAGGER"], clip_model_name=current_config["MODEL_REVERSE_IMAGE"],
                tagger=tagger, clip_model=clip_model, clip_preprocess=clip_preprocess,
                uploader_id=uploader_id,default_visibility_roles=default_role_ids
            )
            actual_task_func.__name__ = f"{target_task_func.__name__}_{os.path.basename(path)}"

            priority = Priority.HIGH if ingest_source == "watcher" else Priority.NORMAL
            
            # --- UPDATED: Schedule the task directly, no more context wrapper ---
            await manager.schedule_task(actual_task_func, args=(), kwargs={}, priority=priority)

        except (FileNotFoundError, magic.MagicException) as e:
            # --- UPDATED: Log directly ---
            dispatcher_logger.warning(f"File vanished or unreadable before dispatch, skipping: {path} ({e})")
        except asyncio.CancelledError:
            dispatcher_logger.info("Dispatcher task cancelled.")
            break
        except Exception:
            dispatcher_logger.exception(f"A critical top-level error occurred while dispatching {path}")
        finally:
            queue.task_done()


async def update_application_state(config_manager: ConfigManager, queue: asyncio.Queue, first_run: bool = False):
    """Atomically updates the application's running state by reconfiguring the TaskManagerRegistry."""
    async with _app_state_lock:
        if not first_run:
            logger.info("Reloading configuration and re-initializing worker pools...")

        old_config = app_state.get("config") or {}
        config_manager.reload()
        new_config = config_manager.data
        app_state["config"] = new_config

        role_names_from_config = new_config.get("DEFAULT_VISIBILITY_ROLES", ["Everyone"])
        resolved_role_ids = []
        for role_name in role_names_from_config:
            role_data = await get_role_by_name(role_name)
            if role_data:
                resolved_role_ids.append(role_data['id'])
            else:
                logger.warning(f"Default visibility role '{role_name}' from config not found in database. It will be ignored.")

        # If after checking all names, the list is empty, default to Everyone
        if not resolved_role_ids:
            everyone_role = await get_role_by_name("Everyone")
            if everyone_role:
                resolved_role_ids.append(everyone_role['id'])
                logger.info("No valid default roles found in config, defaulting to 'Everyone'.")

        app_state["default_visibility_role_ids"] = resolved_role_ids
        logger.info(f"Default visibility for new files set to role IDs: {resolved_role_ids}")

        old_img_concurrency = old_config.get("IMAGE_GPU_CONCURRENCY", -1)
        old_vid_concurrency = old_config.get("VIDEO_GPU_CONCURRENCY", -1)
        new_img_concurrency = new_config["IMAGE_GPU_CONCURRENCY"]
        new_vid_concurrency = new_config["VIDEO_GPU_CONCURRENCY"]

        if first_run or old_img_concurrency != new_img_concurrency or old_vid_concurrency != new_vid_concurrency:
            if not first_run:
                # 1. Stop all current worker tasks gracefully.
                logger.info("Concurrency settings changed. Stopping all worker pools...")
                await task_registry.stop_all()

                # 2. Clear the old manager instances from the registry.
                task_registry.clear()

            # 3. Configure new manager instances based on the new settings.
            logger.info(f"Configuring pools: Images({new_img_concurrency}), Videos({new_vid_concurrency})")
            task_pools_config = {
                "image": {"workers": new_img_concurrency},
                "video": {"workers": new_vid_concurrency}
            }
            task_registry.configure_from_dict(task_pools_config)

            # 4. Start the workers for the newly configured managers.
            task_registry.start_all()

        if new_img_concurrency > 0 or new_vid_concurrency > 0:
            if not app_state["processing_enabled_event"].is_set():
                logger.info("Processing is now ENABLED. Dispatchers are resuming.")
            app_state["processing_enabled_event"].set()
        else:
            if app_state["processing_enabled_event"].is_set():
                logger.warning("All processing is now DISABLED. Dispatchers will pause.")
            app_state["processing_enabled_event"].clear()

        image_processing_just_enabled = old_img_concurrency == 0 and new_img_concurrency > 0
        video_processing_just_enabled = old_vid_concurrency == 0 and new_vid_concurrency > 0
        if not first_run and (image_processing_just_enabled or video_processing_just_enabled):
            logger.info("A processing type was enabled. Triggering a re-scan...")
            asyncio.create_task(initial_scan(queue, new_config["WATCH_DIRS"]))

        old_clip_model_name = old_config.get("MODEL_REVERSE_IMAGE")
        new_clip_model_name = new_config["MODEL_REVERSE_IMAGE"]
        if first_run or old_clip_model_name != new_clip_model_name:
            logger.info(f"CLIP model configuration changed to: '{new_clip_model_name}'.")
            clip_manager = get_clip_manager()
            await clip_manager.load_or_get_model(new_clip_model_name)
            if first_run:
                await clip_manager.configure_idle_cleanup(IDLE_TIMEOUT)

        loop = asyncio.get_running_loop()
        old_watch_dirs = set(old_config.get("WATCH_DIRS", []))
        new_watch_dirs = set(new_config["WATCH_DIRS"])
        if first_run or old_watch_dirs != new_watch_dirs:
            logger.info(f"Updating watched directories to: {list(new_watch_dirs)}")
            if app_state.get("media_observer"):
                app_state["media_observer"].stop()
                app_state["media_observer"].join(timeout=2.0)
            observer = Observer()
            handler = MediaHandler(loop, queue)
            for path in new_watch_dirs:
                if os.path.isdir(path):
                    observer.schedule(handler, path=path, recursive=False)
                else:
                    logger.warning(f"Watch directory does not exist, skipping: {path}")
            app_state["media_observer"] = observer

        logger.info("Application state update complete.")

async def periodic_index_builder():
    """A background task that periodically rebuilds the FAISS index."""
    # --- UPDATED: Standard logger creation ---
    builder_logger = logging.getLogger("IndexBuilder")
    builder_logger.setLevel(logging.INFO)
    shutdown_event = app_state["shutdown_event"]

    try:
        rebuild_interval_minutes = int(app_state["config"].get("INDEX_REBUILD_INTERVAL_MINUTES", 15))
        if rebuild_interval_minutes <= 0:
            builder_logger.warning("Periodic index rebuilding is disabled (interval <= 0).")
            return

        rebuild_interval_seconds = rebuild_interval_minutes * 60
        builder_logger.info(f"FAISS index will be rebuilt every {rebuild_interval_minutes} minutes.")

        while not shutdown_event.is_set():
            try:
                # Wait for the interval, checking for shutdown every 5 seconds
                await asyncio.wait_for(shutdown_event.wait(), timeout=rebuild_interval_seconds)
            except asyncio.TimeoutError:
                builder_logger.info("Triggering periodic FAISS index rebuild...")
                try:
                    await build_faiss_index()
                    builder_logger.info("Periodic FAISS index rebuild completed successfully.")
                except Exception as e:
                    builder_logger.error(f"Periodic FAISS index build failed: {e}", exc_info=True)
    except asyncio.CancelledError:
        builder_logger.info("Periodic index builder task was cancelled.")
    finally:
        builder_logger.info("Periodic index builder has shut down.")


async def startup_scanner_logic():
    logger.info("--- Media Scanner Initializing ---")

    executor = ThreadPoolExecutor(max_workers=(os.cpu_count() or 4) * 2)
    app_state["executor"] = executor
    config_manager = get_config()

    # Phase 1: Initial Configuration and Worker Setup
    await update_application_state(config_manager, processing_queue, first_run=True)
    config = app_state["config"]
    os.makedirs(config["FILESTORE_DIR"], exist_ok=True)

    # Start the dispatcher and model cleanup tasks. They will wait for work.
    num_dispatchers = (os.cpu_count() or 4)
    dispatchers = [asyncio.create_task(dispatcher_worker(f"D-{i+1}", processing_queue, executor)) for i in range(num_dispatchers)]
    cleanup_task = asyncio.create_task(model_cleanup_task())

    # Phase 2: Initial Scan and Batch Processing
    if not config_manager.get('SKIP_SCAN_ON_START', '1'):
        await initial_scan(processing_queue, config["WATCH_DIRS"])

    logger.info("Waiting for all initial files to be dispatched...")
    await processing_queue.join()
    logger.info("All initial files have been dispatched to worker pools.")

    await task_registry.join_all()
    logger.info("All initial file processing is complete.")

    # Phase 3: Initial FAISS Index Build
    if not processing_queue.empty():
         logger.warning("Intake queue is not empty after join, this is unexpected.")

    logger.info("Triggering initial FAISS index build...")
    try:
        await build_faiss_index()
        logger.info("Initial FAISS index build completed successfully.")
    except Exception as e:
        logger.error(f"Initial FAISS index build failed: {e}", exc_info=True)

    # Phase 4: Start Long-Running Services
    logger.info("Starting long-running services (file watcher, periodic index builder)...")

    media_observer = app_state.get("media_observer")
    if media_observer:
        media_observer.start()

    index_builder_task = asyncio.create_task(periodic_index_builder())

    loop = asyncio.get_running_loop()
    def reload_sync_callback():
        logger.info("Config file change detected. Scheduling state update...")
        asyncio.run_coroutine_threadsafe(
            update_application_state(config_manager, processing_queue), loop
        )
    config_handler = ConfigChangeHandler(file_paths=config_manager.paths, reload_callback=reload_sync_callback)
    config_observer = Observer()
    config_watch_dirs = {os.path.dirname(p) for p in config_manager.paths}
    for d in config_watch_dirs:
        config_observer.schedule(config_handler, d, recursive=False)
    config_observer.start()
    logger.info(f"Watching for changes in: {config_manager.paths[0]}")

    return {
        "background_tasks": dispatchers + [cleanup_task],
        "index_builder_task": index_builder_task,
        "executor": executor,
        "config_observer": config_observer,
        "intake_queue": processing_queue,
    }


async def shutdown_scanner_logic(state: dict):
    logger.info("\n--- Starting graceful shutdown of the scanner service ---")

    if "shutdown_event" in app_state:
        app_state["shutdown_event"].set()

    config_observer = state.get("config_observer")
    media_observer = app_state.get("media_observer")
    if config_observer:
        config_observer.stop(); config_observer.join(timeout=2.0)
    if media_observer:
        media_observer.stop(); media_observer.join(timeout=2.0)

    all_tasks = []
    if state.get("background_tasks"): all_tasks.extend(state["background_tasks"])
    if state.get("index_builder_task"): all_tasks.append(state["index_builder_task"])

    if all_tasks:
        logger.info(f"Cancelling {len(all_tasks)} background tasks...")
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

    logger.info("Unloading CLIP model...")
    await get_clip_manager().unload()

    logger.info("Stopping all worker pools...")
    await task_registry.stop_all()

    executor = state.get("executor")
    if executor:
        logger.info("Shutting down scanner thread pool executor...")
        executor.shutdown(wait=True)

    logger.info("--- Scanner service shutdown complete ---")


if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    shutdown_event = asyncio.Event()
    scanner_state = {}

    def signal_handler(sig, frame):
        logger.info(f"Caught signal {sig}, initiating graceful shutdown...")
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        loop.call_soon_threadsafe(shutdown_event.set)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    async def run_application():
        global scanner_state
        try:
            scanner_state = await startup_scanner_logic()
            logger.info("--- System is running. Press Ctrl+C to exit. ---")
            await shutdown_event.wait()
        except asyncio.CancelledError:
            logger.info("Main application task was cancelled.")
        except Exception as e:
            logger.exception(f"An unhandled exception occurred during runtime: {e}")
        finally:
            if scanner_state:
                await shutdown_scanner_logic(scanner_state)

    try:
        loop.run_until_complete(run_application())
    finally:
        loop.close()
        logger.info("Application exited.")