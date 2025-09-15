# ImageTagger.py

import os
import csv
import sys
import time
import asyncio
import aiohttp
import numpy as np
from PIL import Image
from onnxruntime import InferenceSession, get_available_providers

# --- Configuration ---
IDLE_UNLOAD_SECONDS = 1800  # Unload models after 30 minutes (1800 seconds) of inactivity.

# Map model names to their Hugging Face repository IDs
MODELS_CONFIG = {
    "wd-swinv2-tagger-v3": "SmilingWolf/wd-swinv2-tagger-v3",
    # Add other compatible models here, e.g.:
    # "wd-vit-tagger-v3": "SmilingWolf/wd-vit-tagger-v3",
}

# --- Global Cache and Lock ---
# This dictionary will act as a cache to hold loaded Tagger instances.
# Key: model_name (str), Value: Tagger instance
_model_cache = {}
# A lock to prevent race conditions when modifying the cache from multiple asyncio tasks.
_cache_lock = asyncio.Lock()


async def _download_to_file(url: str, dest_path: str, progress_callback, session: aiohttp.ClientSession):
    """Asynchronously downloads a file and streams it to a destination path, with progress."""
    try:
        async with session.get(url) as response:
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            # Ensure the destination directory exists
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            with open(dest_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size > 0:
                        percentage = (downloaded_size / total_size) * 100
                        await progress_callback(percentage)
            
            # Ensure the final 100% is reported
            await progress_callback(100)

    except aiohttp.ClientResponseError as e:
        print(f"\nError downloading {url}: {e.status} {e.message}", file=sys.stderr)
        # Clean up partial file
        if os.path.exists(dest_path):
            os.remove(dest_path)
        raise
    except Exception as e:
        print(f"\nAn unexpected error occurred during download of {url}: {e}", file=sys.stderr)
        if os.path.exists(dest_path):
            os.remove(dest_path)
        raise


async def get_tagger(model_name: str = "wd-swinv2-tagger-v3"):
    """
    Asynchronous factory function to get a Tagger instance.
    
    This function manages a global cache of Tagger objects to ensure that models
    are loaded into memory only once. It will also download model files if they are not found.

    Args:
        model_name (str): The name of the model to load (without the .onnx extension).

    Returns:
        Tagger: An initialized Tagger instance ready for use.
    """
    async with _cache_lock:
        if model_name in _model_cache:
            tagger = _model_cache[model_name]
            # If the model was unloaded due to inactivity, re-initialize it before returning.
            if not tagger.is_loaded():
                print(f"[Tagger] Re-loading previously unloaded model: {model_name}")
                await tagger.load()
            return tagger
        else:
            print(f"[Tagger] Loading model '{model_name}' for the first time...")
            tagger = Tagger(model_name)
            await tagger.load()  # This will download if necessary
            _model_cache[model_name] = tagger
            print(f"[Tagger] Model '{model_name}' loaded and cached.")
            return tagger


class Tagger:
    """
    A class to handle image tagging using an ONNX model.
    
    It encapsulates model loading (including downloading), pre-processing, 
    inference, post-processing, and memory management (loading/unloading). 
    An instance of this class should be created via the `get_tagger` factory function.
    """
    def __init__(self, model_name: str, models_dir: str = "./models"):
        self.models_dir = models_dir
        self.model_name = model_name
        
        # --- Internal State ---
        self.model: InferenceSession | None = None
        self.tags = {}
        self.last_used = 0
        
        # --- Model-specific attributes ---
        self.input_name = ""
        self.input_height = 0
        self.output_name = ""

    async def _download_model(self):
        """Downloads the model.onnx and selected_tags.csv files for the current model."""
        if self.model_name not in MODELS_CONFIG:
            raise ValueError(f"Model '{self.model_name}' is not configured for download. Please add it to MODELS_CONFIG.")

        repo_id = MODELS_CONFIG[self.model_name]
        hf_endpoint = os.getenv("HF_ENDPOINT", 'https://huggingface.co')
        if not hf_endpoint.startswith("https://"):
            hf_endpoint = f"https://{hf_endpoint}"
        if hf_endpoint.endswith("/"):
            hf_endpoint = hf_endpoint.rstrip("/")
        
        # Construct base URL for the model repo files
        base_url = f"{hf_endpoint}/{repo_id}/resolve/main/"
        
        onnx_url = f"{base_url}model.onnx"
        csv_url = f"{base_url}selected_tags.csv"
        
        onnx_dest = os.path.join(self.models_dir, f"{self.model_name}.onnx")
        csv_dest = os.path.join(self.models_dir, f"{self.model_name}.csv")

        print(f"[{self.model_name}] Model files not found. Downloading...")

        async with aiohttp.ClientSession() as session:
            last_printed_perc = {}
            async def update_callback(file_name, perc):
                # Only print every 5% to avoid spamming the console
                current_perc = int(perc)
                if current_perc > last_printed_perc.get(file_name, -1) and (current_perc % 5 == 0 or current_perc == 100):
                    progress = current_perc
                    bar = '#' * (progress // 5) + '-' * (20 - (progress // 5))
                    print(f"\rDownloading {file_name}: [{bar}] {progress:>3}%", end="", flush=True)
                    last_printed_perc[file_name] = current_perc

            try:
                # Download ONNX model
                print(f"Downloading ONNX model to {onnx_dest}...")
                await _download_to_file(onnx_url, onnx_dest, lambda p: update_callback(f"{self.model_name}.onnx", p), session=session)
                print() # Newline after progress bar
                
                # Download tags CSV
                print(f"Downloading tags CSV to {csv_dest}...")
                await _download_to_file(csv_url, csv_dest, lambda p: update_callback(f"{self.model_name}.csv", p), session=session)
                print() # Newline after progress bar
                
            except aiohttp.ClientError as err:
                print(f"\nERROR: Unable to download model '{self.model_name}'. "
                      "Please download files manually or try using a HF mirror/proxy "
                      "by setting the environment variable HF_ENDPOINT=https://....", file=sys.stderr)
                # Re-raise to stop the loading process
                raise
        
        print(f"[{self.model_name}] Download complete.")

    def is_loaded(self) -> bool:
        """Checks if the ONNX model is currently loaded in memory."""
        return self.model is not None

    async def load(self):
        """
        Loads the ONNX model and associated tags into memory.
        If the model files are not found, it triggers a download.
        """
        if self.is_loaded():
            return  # Already loaded.

        # 1. --- CHECK FOR MODEL FILES AND DOWNLOAD IF MISSING ---
        onnx_path = os.path.join(self.models_dir, self.model_name + ".onnx")
        csv_path = os.path.join(self.models_dir, self.model_name + ".csv")
        
        if not os.path.exists(onnx_path) or not os.path.exists(csv_path):
            await self._download_model()
        
        if not os.path.exists(onnx_path):
             raise FileNotFoundError(f"Model file could not be found or downloaded: {onnx_path}")
        if not os.path.exists(csv_path):
             raise FileNotFoundError(f"Tag file could not be found or downloaded: {csv_path}")

        # 2. --- LOAD THE ONNX MODEL ---
        available_providers = get_available_providers()
        providers = ['CUDAExecutionProvider', 'CPUExecutionProvider'] if 'CUDAExecutionProvider' in available_providers else ['CPUExecutionProvider']
        
        self.model = InferenceSession(onnx_path, providers=providers)
        
        # 3. --- GET MODEL-SPECIFIC INFO ---
        self.input_name = self.model.get_inputs()[0].name
        self.input_height = self.model.get_inputs()[0].shape[1]
        self.output_name = self.model.get_outputs()[0].name

        # 4. --- LOAD TAGS FROM CSV ---
        self.tags = self._load_tags()

        # 5. --- UPDATE TIMESTAMP AND REPORT ---
        self.touch()
        print(f"[{self.model_name}] Initialized ONNX session with provider: {self.model.get_providers()[0]}")

    def unload(self):
        """Unloads the model to free up GPU VRAM."""
        if not self.is_loaded():
            return
        
        print(f"[{self.model_name}] Unloading model due to inactivity to free VRAM...")
        # De-referencing the model is the key. Python's garbage collector will handle the rest.
        self.model = None
        self.tags = {}

    def touch(self):
        """Updates the last-used timestamp to the current time."""
        self.last_used = time.time()

    def _load_tags(self) -> list[tuple[str, int]]:
        """
        Loads tag data from the model's corresponding .csv file,
        preserving the original order.
        Returns a list of (tag_name, category) tuples.
        """
        csv_path = os.path.join(self.models_dir, self.model_name + ".csv")
        # No need to check for existence here, as the `load` method already did.
        ordered_tags = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                # row[1] is name, row[2] is category
                ordered_tags.append((row[1], int(row[2]))) 
        return ordered_tags

    def tag(self, image: Image.Image, threshold=0.35, character_threshold=0.85, 
            exclude_tags="", replace_underscore=True) -> list[str]:
        """
        Processes and tags a single PIL Image.

        This is a synchronous, CPU/GPU-bound function. It should be run in a
        separate thread (e.g., using `run_in_executor`) to avoid blocking
        an asyncio event loop.

        Args:
            image: The PIL Image object to tag.
            threshold: Confidence threshold for general tags (category 0).
            character_threshold: Confidence threshold for character tags (category 4).
            exclude_tags: A comma-separated string of tags to exclude.
            replace_underscore: If True, replaces underscores in tags with spaces.

        Returns:
            A list of tag strings.
        """
        if not self.is_loaded() or self.model is None or not self.tags:
            raise RuntimeError(f"Model '{self.model_name}' is not loaded. Cannot perform tagging.")
        
        self.touch()

        # 1. --- PRE-PROCESS THE IMAGE (CPU-bound) ---
        ratio = float(self.input_height) / max(image.size)
        new_size = tuple([int(x * ratio) for x in image.size])
        image = image.resize(new_size, Image.LANCZOS)
        square = Image.new("RGB", (self.input_height, self.input_height), (255, 255, 255))
        square.paste(image, ((self.input_height - new_size[0]) // 2, (self.input_height - new_size[1]) // 2))

        image_np = np.array(square).astype(np.float32)
        image_np = image_np[:, :, ::-1]  # RGB -> BGR
        image_np = np.expand_dims(image_np, 0)

        # 2. --- RUN INFERENCE (GPU-bound) ---
        probs = self.model.run([self.output_name], {self.input_name: image_np})[0][0]
        
        # 3. --- POST-PROCESS RESULTS (CPU-bound) ---
        general_tags = []
        character_tags = []

        for (tag_name, category), probability in zip(self.tags, probs):
            if category == 0 and probability > threshold:
                general_tags.append(tag_name)
            elif category == 4 and probability > character_threshold:
                character_tags.append(tag_name)
        
        combined_tags = character_tags + general_tags
        
        remove_set = {s.strip().lower() for s in exclude_tags.lower().split(",") if s}
        
        final_tags = []
        for tag in combined_tags:
            formatted_tag = tag.replace("_", " ") if replace_underscore else tag
            if formatted_tag.lower() not in remove_set:
                final_tags.append(formatted_tag)
                
        return final_tags

# --- Example Usage (for testing the module directly) ---
if __name__ == '__main__':
    async def test_tagger():
        # This demonstrates how the tagger would be used in an async application.
        # To test the downloader, delete the ./models directory before running.
        
        # Create a dummy image for testing
        try:
            test_image = Image.new('RGB', (600, 800), color='red')
            print("Created a dummy test image.")
        except Exception as e:
            print(f"Could not create a test image: {e}")
            return
            
        print("\n--- First Call (will download if models are missing) ---")
        tagger1 = await get_tagger("wd-swinv2-tagger-v3")
        tags1 = tagger1.tag(test_image)
        print(f"Tags found: {tags1[:5]}...") # Print first 5 tags
        print(f"Tagger last used at: {time.ctime(tagger1.last_used)}")

        await asyncio.sleep(2)

        print("\n--- Second Call (should be cached) ---")
        tagger2 = await get_tagger("wd-swinv2-tagger-v3")
        tagger2.tag(test_image, threshold=0.5)
        print(f"Tagger last used at: {time.ctime(tagger2.last_used)}")
        print(f"Is tagger1 the same object as tagger2? {tagger1 is tagger2}")

        print("\n--- Simulating Unloading ---")
        tagger2.unload()
        print(f"Is model loaded? {tagger2.is_loaded()}")

        print("\n--- Third Call (should be re-loaded) ---")
        tagger3 = await get_tagger("wd-swinv2-tagger-v3")
        print(f"Is model loaded now? {tagger3.is_loaded()}")
        print(f"Is tagger2 the same object as tagger3? {tagger2 is tagger3}")
    
    try:
        asyncio.run(test_tagger())
    except Exception as e:
        print(f"\nAn unexpected error occurred during the test: {e}")
        print("If the error is related to downloads, check your internet connection or try setting the HF_ENDPOINT environment variable to a mirror.")