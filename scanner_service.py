# scanner_service.py
import asyncio
import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor

from watchdog.observers import Observer

from ConfigMedia import ConfigChangeHandler, ConfigManager, get_config
from TaskManager import task_registry
from build_index import build_faiss_index
from src.media.dispatcher import dispatcher_worker
from src.media.scanner import Scanner
from src.roles.rbac_manager import get_role_by_name

logger = logging.getLogger("System")
logger.setLevel(logging.INFO)

app_state = {
    "config": None,
    "scanner": None,
    "processing_enabled_event": asyncio.Event(),
    "shutdown_event": asyncio.Event(),
    "executor": None,
    "intake_queue": asyncio.Queue(),
    "default_visibility_role_ids": [],
    "processing_hashes": set(),
    "processing_lock": asyncio.Lock(),
    "files_processed_session": 0,
    "currently_processing": set(),
    "session_stats_lock": asyncio.Lock(),
}
app_state["service_handles"] = {}
_app_state_lock = asyncio.Lock()


async def update_application_state(config_manager: ConfigManager, first_run: bool = False):
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

        if not resolved_role_ids:
            everyone_role = await get_role_by_name("Everyone")
            if everyone_role:
                resolved_role_ids.append(everyone_role['id'])
                logger.info("No valid default roles found in config, defaulting to 'Everyone'.")

        app_state["default_visibility_role_ids"] = resolved_role_ids
        logger.info(f"Default visibility for new files set to role IDs: {resolved_role_ids}")

        old_img_concurrency = old_config.get("IMAGE_GPU_CONCURRENCY", -1)
        old_vid_concurrency = old_config.get("VIDEO_GPU_CONCURRENCY", -1)
        old_vid_conversion_concurrency = old_config.get("VIDEO_CONVERSION_GPU_CONCURRENCY", -1)
        old_txt_concurrency = old_config.get("TEXT_CPU_CONCURRENCY", -1)
        old_gen_concurrency = old_config.get("GENERIC_CPU_CONCURRENCY", -1)
        
        new_img_concurrency = new_config.get("IMAGE_GPU_CONCURRENCY", 0)
        new_vid_concurrency = new_config.get("VIDEO_GPU_CONCURRENCY", 0)
        new_vid_conversion_concurrency = new_config.get("VIDEO_CONVERSION_GPU_CONCURRENCY", 1)
        new_txt_concurrency = new_config.get("TEXT_CPU_CONCURRENCY", 2)
        new_gen_concurrency = new_config.get("GENERIC_CPU_CONCURRENCY", 2)

        if (first_run or 
            old_img_concurrency != new_img_concurrency or 
            old_vid_concurrency != new_vid_concurrency or
            old_vid_conversion_concurrency != new_vid_conversion_concurrency or
            old_txt_concurrency != new_txt_concurrency or
            old_gen_concurrency != new_gen_concurrency):
            if not first_run:
                logger.info("Concurrency settings changed. Stopping all worker pools...")
                # Clear is now an async method that handles stopping everything gracefully.
                await task_registry.clear()

            logger.info(f"Configuring pools: Images({new_img_concurrency}), Videos({new_vid_concurrency}), VideoConversions({new_vid_conversion_concurrency}), Text({new_txt_concurrency}), Generic({new_gen_concurrency}))")
            task_pools_config = {
                "image": {"workers": new_img_concurrency},
                "video": {"workers": new_vid_concurrency},
                "video-conversion": {"workers": new_vid_conversion_concurrency},
                "text": {"workers": new_txt_concurrency},
                "generic": {"workers": new_gen_concurrency}
            }
            task_registry.configure_from_dict(task_pools_config)
            task_registry.start_all()

        if new_img_concurrency > 0 or new_vid_concurrency > 0 or new_txt_concurrency > 0 or new_gen_concurrency > 0:
            if not app_state["processing_enabled_event"].is_set():
                logger.info("Processing is now ENABLED. Dispatchers are resuming.")
            app_state["processing_enabled_event"].set()
        else:
            if app_state["processing_enabled_event"].is_set():
                logger.warning("All processing is now DISABLED. Dispatchers will pause.")
            app_state["processing_enabled_event"].clear()

        image_processing_just_enabled = old_img_concurrency <= 0 and new_img_concurrency > 0
        video_processing_just_enabled = old_vid_concurrency <= 0 and new_vid_concurrency > 0
        text_processing_just_enabled = old_txt_concurrency <= 0 and new_txt_concurrency > 0
        generic_processing_just_enabled = old_gen_concurrency <= 0 and new_gen_concurrency > 0
        if not first_run and (image_processing_just_enabled or video_processing_just_enabled or text_processing_just_enabled or generic_processing_just_enabled):
            logger.info("A processing type was enabled. Triggering a re-scan...")
            scanner = app_state.get("scanner")
            if scanner:
                asyncio.create_task(scanner.initial_scan())

        old_clip_model_name = old_config.get("MODEL_REVERSE_IMAGE")
        new_clip_model_name = new_config["MODEL_REVERSE_IMAGE"]
        if first_run or old_clip_model_name != new_clip_model_name:
            logger.info(f"CLIP model configuration changed to: '{new_clip_model_name}'.")
            

        old_watch_dirs = set(old_config.get("WATCH_DIRS", []))
        new_watch_dirs = set(new_config["WATCH_DIRS"])
        if first_run or old_watch_dirs != new_watch_dirs:
            logger.info(f"Updating watched directories to: {list(new_watch_dirs)}")
            if app_state.get("scanner"):
                app_state["scanner"].stop_watching()
            
            scanner = Scanner(app_state["intake_queue"], new_watch_dirs)
            app_state["scanner"] = scanner

        logger.info("Application state update complete.")

async def warmup_and_unload_models(config: dict):
    """
    Pre-compiles models on the main thread to prevent deadlocks, then unloads them
    to keep startup memory usage low.
    """
    logger.info("--- Warming up AI models (pre-compile and unload) ---")
    loop = asyncio.get_running_loop()

    # --- ResNet for Scene Detection ---
    if config.get('VIDEO_GPU_CONCURRENCY', 0) > 0:
        from src.media.HelperMedia import _get_resnet_model, unload_resnet_model
        logger.info("Warming up ResNet (compiling)...")
        await loop.run_in_executor(None, _get_resnet_model)
        logger.info("Unloading ResNet post-warmup.")
        await loop.run_in_executor(None, unload_resnet_model)

    # --- Tagger for Image/Video Tagging ---
    if config.get('AUTOMATICALLY_TAG_IMG') or config.get('AUTOMATICALLY_TAG_VIDEOS'):
        from ImageTagger import get_tagger
        model_name = config['MODEL_TAGGER']
        logger.info(f"Warming up Tagger '{model_name}'...")
        tagger = await get_tagger(model_name)
        logger.info("Unloading Tagger post-warmup.")
        tagger.unload()

    # --- CLIP for Reverse Image Search ---
    from ClipManager import get_clip_manager
    clip_manager = get_clip_manager()
    model_name = config['MODEL_REVERSE_IMAGE']
    logger.info(f"Warming up CLIP model '{model_name}'...")
    await clip_manager.load_or_get_model(model_name)
    logger.info("Unloading CLIP post-warmup.")
    await clip_manager.unload()
    logger.info("--- AI model warmup complete ---")


async def periodic_background_tasks():
    """A single background task for periodic maintenance like index building and model cleanup."""
    task_logger = logging.getLogger("PeriodicTasks")
    task_logger.setLevel(logging.INFO)
    shutdown_event = app_state["shutdown_event"]

    try:
        config = app_state["config"]
        rebuild_interval = int(config.get("INDEX_REBUILD_INTERVAL_MINUTES", 15)) * 60
        idle_timeout = int(config.get("MODEL_IDLE_UNLOAD_SECONDS", 1800))
        cleanup_interval = 60  # Check every minute

        # Configure ClipManager's self-managed idle cleanup
        from ClipManager import get_clip_manager
        await get_clip_manager().configure_idle_cleanup(idle_timeout)

        task_logger.info(f"Periodic tasks started. Index rebuild: {rebuild_interval}s, Model cleanup: {cleanup_interval}s, Idle timeout: {idle_timeout}s.")
        last_rebuild_time = asyncio.get_running_loop().time()

        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=cleanup_interval)
            except asyncio.TimeoutError:
                
                # --- Model Cleanup ---
                if idle_timeout > 0:
                    from src.media.HelperMedia import check_and_unload_idle_resnet
                    from ImageTagger import check_and_unload_idle_taggers
                    task_logger.debug("Running periodic model cleanup...")
                    await asyncio.to_thread(check_and_unload_idle_resnet, idle_timeout)
                    await check_and_unload_idle_taggers(idle_timeout)

                # --- Index Rebuild ---
                if rebuild_interval > 0 and (asyncio.get_running_loop().time() - last_rebuild_time) > rebuild_interval:
                    task_logger.info("Triggering periodic FAISS index rebuild...")
                    try:
                        await build_faiss_index()
                        task_logger.info("Periodic FAISS index rebuild completed successfully.")
                        last_rebuild_time = asyncio.get_running_loop().time()
                    except Exception as e:
                        task_logger.error(f"Periodic FAISS index build failed: {e}", exc_info=True)
    except asyncio.CancelledError:
        task_logger.info("Periodic background task was cancelled.")
    finally:
        task_logger.info("Periodic background tasks have shut down.")




async def startup_scanner_logic():
    logger.info("--- Media Scanner Initializing ---")

    config_manager = get_config()
    await warmup_and_unload_models(config_manager.data)

    executor = ThreadPoolExecutor(max_workers=(os.cpu_count() or 4) * 2)
    app_state["executor"] = executor
    await update_application_state(config_manager, first_run=True)
    config = app_state["config"]
    os.makedirs(config["FILESTORE_DIR"], exist_ok=True)

    num_dispatchers = (os.cpu_count() or 4)
    dispatchers = [asyncio.create_task(dispatcher_worker(f"D-{i+1}", app_state, app_state["intake_queue"], executor)) for i in range(num_dispatchers)]

    if not config_manager.get('SKIP_SCAN_ON_START', '1'):
        scanner = app_state.get("scanner")
        if scanner:
            await scanner.initial_scan()

    logger.info("Waiting for all initial files to be dispatched...")
    await app_state["intake_queue"].join()
    logger.info("All initial files have been dispatched to worker pools.")

    await task_registry.join_all()
    logger.info("All initial file processing is complete.")

    if not app_state["intake_queue"].empty():
         logger.warning("Intake queue is not empty after join, this is unexpected.")

    logger.info("Triggering initial FAISS index build...")
    try:
        await build_faiss_index()
        logger.info("Initial FAISS index build completed successfully.")
    except Exception as e:
        logger.error(f"Initial FAISS index build failed: {e}", exc_info=True)

    logger.info("Starting long-running services (file watcher, periodic index builder)...")

    scanner = app_state.get("scanner")
    if scanner:
        scanner.start_watching()

    periodic_tasks_handle = asyncio.create_task(periodic_background_tasks())

    loop = asyncio.get_running_loop()
    def reload_sync_callback():
        logger.info("Config file change detected. Scheduling state update...")
        asyncio.run_coroutine_threadsafe( 
            update_application_state(config_manager), loop
        )

    config_handler = ConfigChangeHandler(file_paths=config_manager.paths, reload_callback=reload_sync_callback)
    config_observer = Observer()
    config_watch_dirs = {os.path.dirname(p) for p in config_manager.paths}
    for d in config_watch_dirs:
        config_observer.schedule(config_handler, d, recursive=False)
    config_observer.start()
    logger.info(f"Watching for changes in: {config_manager.paths[0]}")

    app_state["service_handles"] = {
        "background_tasks": dispatchers + [],
        "periodic_tasks_handle": periodic_tasks_handle,
        "executor": executor,
        "config_observer": config_observer,
    }
    return app_state


async def shutdown_scanner_logic(state: dict):
    logger.info("\n--- Starting graceful shutdown of the scanner service ---")

    app_state["shutdown_event"].set()

    service_handles = state.get("service_handles", {})
    config_observer = service_handles.get("config_observer")
    if config_observer:
        config_observer.stop()
        # Run the blocking join in an executor to avoid blocking the event loop.
        await asyncio.get_running_loop().run_in_executor(None, config_observer.join, 2.0)
    
    scanner = app_state.get("scanner")
    if scanner:
        # This method is now async and uses run_in_executor for its own join.
        await scanner.stop_watching()

    # --- Stop auxiliary background tasks ---
    from ClipManager import get_clip_manager
    from Searcher import _searcher_instance

    logger.info("Stopping auxiliary background tasks (ClipManager, Searcher)...")
    await get_clip_manager().stop_cleanup_task()
    if _searcher_instance:
        await _searcher_instance.shutdown()
    # ---

    all_tasks = []
    if service_handles.get("background_tasks"): all_tasks.extend(service_handles["background_tasks"])
    if service_handles.get("periodic_tasks_handle"): all_tasks.append(service_handles["periodic_tasks_handle"])

    if all_tasks:
        logger.info(f"Cancelling {len(all_tasks)} background tasks...")
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

    logger.info("Unloading CLIP model...")
    await get_clip_manager().unload()

    logger.info("Stopping all worker pools...")
    await task_registry.stop_all()

    executor = service_handles.get("executor")
    if executor:
        logger.info("Shutting down scanner thread pool executor...")
        await asyncio.get_running_loop().run_in_executor(None, executor.shutdown, True)

    # --- Explicitly shut down torch inductor workers to prevent atexit errors ---
    try:
        import torch._inductor.async_compile
        logger.info("Shutting down PyTorch inductor compile workers...")
        # This is a synchronous function, so run it in an executor.
        await asyncio.get_running_loop().run_in_executor(None, torch._inductor.async_compile.shutdown_compile_workers)
        logger.info("PyTorch inductor workers shut down.")
    except ImportError:
        # If torch inductor is not used or not found, this will fail, which is fine.
        logger.debug("PyTorch inductor module not found, skipping its shutdown.")
        pass
    except Exception as e:
        logger.error(f"Error shutting down PyTorch inductor workers: {e}")

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