import asyncio
import logging
import time
import contextlib
import clip
import torch

logger = logging.getLogger("System")
logger.setLevel(logging.DEBUG)
# Use the same device setting as your main application
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# How often the cleanup task should wake up to check for inactivity
CLEANUP_CHECK_INTERVAL_SECONDS = 60

class ClipModelManager:
    """
    A singleton-style manager for the CLIP model with idle-unload capability.
    """
    _instance = None

    def __new__(cls):
        # Enforce singleton pattern
        if cls._instance is None:
            cls._instance = super(ClipModelManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._model = None
        self._preprocess = None
        self._model_identifier = None
        self._lock = asyncio.Lock()
        self._is_loaded = False
        
        # --- New attributes for idle unload ---
        self.last_used_time = time.time()
        self._idle_unload_seconds = 0  # Configured timeout, 0 means disabled
        self._cleanup_task = None
        # ---
        
        self._initialized = True
        logger.info(f"ClipModelManager initialized. Target device: {DEVICE}")

    @property
    def is_loaded(self):
        return self._is_loaded

    async def unload(self):
        """Asynchronously unloads the model to free VRAM."""
        async with self._lock:
            # Call the internal method that doesn't acquire the lock
            await self._internal_unload()

    async def load_or_get_model(self, model_identifier: str):
        """
        Loads the specified CLIP model if it's not already loaded or if the identifier
        is different. Returns the model and preprocessor.
        """
        async with self._lock:
            # --- Update last used time on every access ---
            self.last_used_time = time.time()
            
            if self._is_loaded and self._model_identifier == model_identifier:
                return self._model, self._preprocess

            if self._is_loaded and self._model_identifier != model_identifier:
                logger.info(f"Model change requested. From '{self._model_identifier}' to '{model_identifier}'.")
                await self._internal_unload()

            logger.info(f"Loading CLIP model '{model_identifier}' onto {DEVICE}...")
            loop = asyncio.get_running_loop()
            
            try:
                model, preprocess = await loop.run_in_executor(
                    None, lambda: clip.load(model_identifier, device=DEVICE)
                )
                self._model = model
                self._preprocess = preprocess
                self._model_identifier = model_identifier
                self._is_loaded = True
                
                # --- Update time again after successful load ---
                self.last_used_time = time.time()
                
                logger.info(f"Successfully loaded CLIP model: {model_identifier}")
                return self._model, self._preprocess
            except Exception as e:
                logger.exception(f"Failed to load CLIP model '{model_identifier}'. Error: {e}")
                await self._internal_unload()
                return None, None

    # --- New methods for managing the cleanup task ---

    async def configure_idle_cleanup(self, idle_unload_seconds: int):
        """Starts or updates the idle-unload background task."""
        self._idle_unload_seconds = idle_unload_seconds
        
        # Stop any existing task before starting a new one
        await self.stop_cleanup_task()

        if self._idle_unload_seconds > 0:
            logger.info(f"Starting CLIP idle-unload task. Timeout: {self._idle_unload_seconds}s")
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup_task())
        else:
            logger.info("CLIP idle-unload is disabled.")

    async def stop_cleanup_task(self):
        """Gracefully stops the background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
            logger.info("CLIP idle-unload task stopped.")
        self._cleanup_task = None

    async def _periodic_cleanup_task(self):
        """The background task that checks for inactivity."""
        while True:
            await asyncio.sleep(CLEANUP_CHECK_INTERVAL_SECONDS)
            
            # We must acquire the lock to safely check and modify state
            async with self._lock:
                if not self._is_loaded:
                    continue

                is_enabled = self._idle_unload_seconds > 0
                is_idle = (time.time() - self.last_used_time) > self._idle_unload_seconds

                if is_enabled and is_idle:
                    logger.info(f"CLIP model has been idle for over {self._idle_unload_seconds}s. Unloading.")
                    await self._internal_unload()

    async def _internal_unload(self):
        """Unloads the model without acquiring the lock (assumes lock is held)."""
        if not self._is_loaded:
            return
        
        logger.info(f"Internal Unload: {self._model_identifier}")
        self._model = None
        self._preprocess = None
        self._is_loaded = False
        self._model_identifier = None
        if DEVICE == "cuda":
            torch.cuda.empty_cache()

# Global accessor function (remains the same)
_clip_manager_instance = ClipModelManager()

def get_clip_manager() -> ClipModelManager:
    return _clip_manager_instance