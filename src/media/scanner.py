# scanner.py
import asyncio
import logging
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from src.media.HelperMedia import get_file_details_by_path_async
from utils import logging_handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging_handler)
MIN_AGE_SECONDS = 60

def safe_to_process(path: str) -> bool:
    try:
        return (time.time() - os.path.getmtime(path)) >= MIN_AGE_SECONDS
    except FileNotFoundError:
        return False

class MediaHandler(FileSystemEventHandler):
    def __init__(self, loop, queue):
        self.loop = loop
        self.queue = queue

    async def _process_watched_file(self, path: str):
        """
        Process a file event from the watcher. It waits until the file is stable
        (no modifications for a few seconds) before queuing it for processing.
        """
        try:
            # Wait until the file is stable. This handles in-progress copies/downloads.
            await asyncio.sleep(1) # Small initial delay to avoid race conditions on fast writes.
            while not safe_to_process(path):
                logger.debug(f"File '{os.path.basename(path)}' is still being written. Waiting...")
                await asyncio.sleep(MIN_AGE_SECONDS)

            # Now that the file is stable, queue it.
            existing_file = await get_file_details_by_path_async(path)
            if existing_file:
                logger.info(f"Detected modification for: {os.path.basename(path)}")
                ingest_info = (path, "watcher_update")
                self.queue.put_nowait(ingest_info)
            else:
                logger.info(f"Discovered new file: {os.path.basename(path)}")
                ingest_info = (path, "watcher_create")
                self.queue.put_nowait(ingest_info)
        except FileNotFoundError:
            logger.warning(f"File '{path}' vanished before it could be processed.")
        except Exception as e:
            logger.error(f"Error processing watched file {path}: {e}", exc_info=True)

    def _handle_event(self, path):
        if not os.path.isfile(path) or path.endswith(('.crdownload', '.tmp', '.part')):
            return
        
        asyncio.run_coroutine_threadsafe(self._process_watched_file(path), self.loop)

    def on_created(self, event):
        self._handle_event(event.src_path)

    def on_modified(self, event):
        self._handle_event(event.src_path)

    def on_moved(self, event):
        self._handle_event(event.dest_path)

class Scanner:
    def __init__(self, queue, watch_dirs):
        self.queue = queue
        self.watch_dirs = watch_dirs
        self.observer = Observer()
        self.loop = asyncio.get_running_loop()

    async def _queue_new_file(self, file_path: str, scan_type: str) -> bool:
        """Checks if a file is safe to process and not already in the database, then queues it."""
        if safe_to_process(file_path):
            
            existing_file = await get_file_details_by_path_async(file_path)
            if not existing_file:
                logger.info(f"Queuing from {scan_type}: {os.path.basename(file_path)}")
                ingest_info = (file_path, scan_type)
                await self.queue.put(ingest_info)
                return True
        return False

    async def initial_scan(self):
        logger.info("Performing initial directory scan...")
        count = 0
        for directory in self.watch_dirs:
            if not os.path.isdir(directory):
                logger.warning(f"Skipping initial scan for non-existent directory: {directory}")
                continue
            logger.info(f"Scanning: {directory}")
            for dirpath, _, filenames in os.walk(directory):
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    
                    if full_path.endswith(('.crdownload', '.tmp', '.part')):
                        continue
                    if await self._queue_new_file(full_path, "folder scan"):
                        count += 1
        logger.info(f"Initial scan complete. Queued {count} files for processing.")

    def start_watching(self):
        handler = MediaHandler(self.loop, self.queue)
        for path in self.watch_dirs:
            if os.path.isdir(path):
                self.observer.schedule(handler, path=path, recursive=True)
            else:
                logger.warning(f"Watch directory does not exist, skipping: {path}")
        if self.observer.emitters:
            self.observer.start()
            logger.info(f"Started watching directories: {self.watch_dirs}")

    async def stop_watching(self):
        if self.observer.is_alive():
            self.observer.stop()
            # Run the blocking join in an executor to avoid blocking the event loop.
            await self.loop.run_in_executor(None, self.observer.join, 2.0)
            logger.info("Stopped watching directories.")