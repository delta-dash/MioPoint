# ConfigMedia.py
import json
import logging
import os
import sqlite3
import threading
from typing import Dict, Any, List, Union, Optional


# --- Global Cache and Lock for ConfigManager instances ---
_config_cache: Dict[tuple, "ConfigManager"] = {}
_cache_lock = threading.Lock()

logger = logging.getLogger("ConfigMedia")
logger.setLevel(logging.DEBUG)


ConfigDict = Dict[str, Any]


class ConfigManager:
    """
    Manages loading and parsing of a configuration file in JSON format.
    Each configuration item has a "name", "value", and "type" to guide parsing.
    
    This manager automatically creates directories for any configuration items
    of type 'dir_path', 'file_path' (parent dir), or 'list_path'.
    
    If the primary config file does not exist, it is created automatically
    using a set of internal defaults.
    """
    def __init__(self, paths: Union[str, List[str], None] = None):
        if paths is None or (isinstance(paths, list) and not paths):
            project_root = os.path.dirname(os.path.abspath(__file__))
            self.paths = [os.path.join(project_root, "config.json")]
        elif isinstance(paths, str):
            self.paths = [os.path.abspath(paths)]
        else:
            self.paths = [os.path.abspath(p) for p in paths]

        self._config: ConfigDict = {}
        self._lock = threading.Lock()
        
        self.reload()

    def _get_default_values(self) -> List[Dict[str, Any]]:
        """
        Returns the canonical default configuration values as a list of dicts.
        Each dict contains the name, value, type for parsing, and a description.
        """
        # --- NEW: Changed type for file paths from 'dir_path' to 'file_path' for clarity ---
        return [
            {
                "name": "ROOT_DIR", "value": ".", "default": ".", "type": "root_path",
                "description": "The root directory for all other relative paths. '.' means the directory where this config file is located.",
                "category": "System & Paths"
            },
            {
                "name": "IMAGE_GPU_CONCURRENCY", "value": 2, "default": 2, "type": "int",
                "description": "Number of concurrent image processing tasks on the GPU.",
                "category": "Processing"
            },
            {
                "name": "VIDEO_GPU_CONCURRENCY", "value": 1, "default": 1, "type": "int",
                "description": "Number of concurrent video processing tasks on the GPU.",
                "category": "Processing"
            },
            {
                "name": "VIDEO_CONVERSION_GPU_CONCURRENCY", "value": 1, "default": 1, "type": "int",
                "description": "Number of concurrent video conversion (transcoding) tasks on the GPU.",
                "category": "Processing"
            },
            {
                "name": "TEXT_CPU_CONCURRENCY", "value": 2, "default": 2, "type": "int",
                "description": "Number of concurrent text file processing tasks on the CPU.",
                "category": "Processing"
            },
            {
                "name": "GENERIC_CPU_CONCURRENCY", "value": 2, "default": 2, "type": "int",
                "description": "Number of concurrent generic file processing tasks on the CPU.",
                "category": "Processing"
            },
            {
                "name": "SKIP_SCAN_ON_START", "value": 0, "default": 0, "type": "bool",
                "description": "If 1 (true), skips the media scan when the application starts. Set to 0 (false) to scan on start.",
                "category": "Processing"
            },
            {
                "name": "DELETE_ON_PROCESS", "value": 0, "default": 0, "type": "bool",
                "description": "If 1 (true), deletes original files from WATCH_DIRS after they are successfully processed and moved to FILESTORE_DIR.",
                "category": "Processing"
            },
            {
                "name": "AUTOMATICALLY_TAG_IMG", "value": 1, "default": 1, "type": "bool",
                "description": "If 1 (true), automatically generate tags for new images.",
                "category": "Features"
            },
            {
                "name": "AUTOMATICALLY_TAG_VIDEOS", "value": 0, "default": 0, "type": "bool",
                "description": "If 1 (true), automatically generate tags for new videos.",
                "category": "Features"
            },
            {
                "name": "VIDEO_FRAME_BATCH_SIZE", "value": 32, "default": 32, "type": "int",
                "description": "How many video frames to process at once for tagging/embedding. Lower to reduce memory usage.",
                "category": "Processing"
            },
            {
                "name": "MAX_IMAGE_DIMENSION", "value": 4096, "default": 4096, "type": "int",
                "description": "Images with any dimension larger than this will be downscaled before processing to save memory. Set to 0 to disable.",
                "category": "Processing"
            },
            {
                "name": "DB_FILE", "value": "$FILESTORE_DIR/library.db", "default": "$FILESTORE_DIR/library.db", "type": "file_path", # <-- CHANGED
                "description": "Path to the SQLite database file, relative to ROOT_DIR.",
                "category": "System & Paths"
            },
            {
                "name": "FILESTORE_DIR", "value": "filesstore", "default": "filesstore", "type": "dir_path",
                "description": "Directory where processed media files are stored, relative to ROOT_DIR.",
                "category": "System & Paths"
            },
            {
                "name": "WATCH_DIRS", "value": "~/Downloads,~/Documents", "default": "~/Downloads,~/Documents", "type": "list_path",
                "description": "Comma-separated list of directories to watch for new media files. Supports '~' for home directory.",
                "category": "System & Paths"
            },
            {
                "name": "DEFAULT_CREATE_FOLDER", "value": "$FILESTORE_DIR/folders", "default": "$FILESTORE_DIR/folders", "type": "dir_path",
                "description": "The absolute path to the directory where new folders are created via the API if no parent is specified. If empty, defaults to the first directory in WATCH_DIRS. This path MUST be one of the directories listed in WATCH_DIRS.",
                "category": "System & Paths"
            },
            {
                "name": "MODEL_TAGGER", "value": "wd-swinv2-tagger-v3", "default": "wd-swinv2-tagger-v3", "type": "str",
                "description": "The name of the model used for automatic tagging.",
                "category": "AI Models"
            },
            {
                "name": "MODEL_REVERSE_IMAGE", "value": "ViT-B/32", "default": "ViT-B/32", "type": "str",
                "description": "The CLIP model for reverse image search. Available: ['RN50', 'RN101', 'RN50x4', 'RN50x16', 'RN50x64', 'ViT-B/32', 'ViT-B/16', 'ViT-L/14', 'ViT-L/14@336px']",
                "category": "AI Models"
            },
            {
                "name": "FAISS_INDEX_FILE", "value": "$FILESTORE_DIR/media_index.faiss", "default": "$FILESTORE_DIR/media_index.faiss", "type": "file_path", # <-- CHANGED
                "description": "Path to the FAISS index file for reverse image search, relative to ROOT_DIR.",
                "category": "System & Paths"
            },
            {
                "name": "FAISS_ID_MAP_FILE", "value": "$FILESTORE_DIR/faiss_to_db_id.json", "default": "$FILESTORE_DIR/faiss_to_db_id.json", "type": "file_path", # <-- CHANGED
                "description": "Path to the mapping file between FAISS indices and database IDs, relative to ROOT_DIR.",
                "category": "System & Paths"
            },
            {
                "name": "TRANSCODED_CACHE_DIR", "value": "$FILESTORE_DIR/transcoded_videos", "default": "$FILESTORE_DIR/transcoded_videos", "type": "dir_path",
                "description": "Directory for storing web-ready transcoded video files. This is a cache and can be cleared if needed.",
                "category": "System & Paths"
            },
            {
                "name": "THUMBNAILS_DIR", "value": "$FILESTORE_DIR/.thumbnails", "default": "$FILESTORE_DIR/.thumbnails", "type": "dir_path",
                "description": "Directory for storing generated thumbnails, relative to ROOT_DIR. Can reference other values like $FILESTORE_DIR.",
                "category": "System & Paths"
            },
            {
                "name": "THUMBNAIL_FORMAT", "value": "WEBP", "default": "WEBP", "type": "str",
                "description": "The format for generated thumbnails (e.g., WEBP, JPEG, PNG).",
                "category": "Media"
            },
            {
                "name": "COPY_FILE_TO_FOLDER",
                "value": 0, "default": 0,
                "type": "bool",
                "description": "If 1 (true), it will copy the file to the FILESTORE_DIR.",
                "category": "Processing"
            },
            {
                "name": "DEFAULT_VISIBILITY_ROLES", "value": "Admin", "default": "Admin", "type": "list_str",
                "description": "Comma-separated list of role NAMES to apply to new files by default. If empty, will default to 'Admin'.",
                "category": "Features"
            },
            
    
            {
                "name": "SCENE_DETECTION_THRESHOLD", "value": 0.3, "default": 0.3, "type": "float",
                "description": "Threshold for ffmpeg's scenedetect filter. Lower is more sensitive to scene changes. (0.1-0.5 is a reasonable range)",
                "category": "Features"
            },
            {
                "name": "SCENE_SECONDS_PER_KEYFRAME_SAMPLE", "value": 60, "default": 60, "type": "int",
                "description": "Sample one keyframe for every X seconds of a scene's duration. Set to 0 to only ever sample the middle frame.",
                "category": "Features"
            },
            {
                "name": "SCENE_KEYFRAME_MAX_SAMPLES", "value": 10, "default": 10, "type": "int",
                "description": "The absolute maximum number of frames to sample per scene, regardless of duration.",
                "category": "Features"
            },
            {
                "name": "SCENE_KEYFRAME_PHASH_THRESHOLD", "value": 5, "default": 5, "type": "int",
                "description": "pHash distance threshold to select a new keyframe within a scene. Lower is more sensitive to visual changes.",
                "category": "Features"
            },
            {
                "name": "MODEL_IDLE_UNLOAD_SECONDS", "value": 1800, "default": 1800, "type": "int",
                "description": "Seconds of inactivity before unloading AI models from VRAM to save memory. Set to 0 to disable.",
                "category": "AI Models"
            },
            {
                "name": "INDEX_REBUILD_INTERVAL_MINUTES", "value": 5, "default": 5, "type": "int",
                "description": "How often (in minutes) to automatically rebuild the reverse-search index. Set to 0 to disable.",
                "category": "Processing"
            },
            {
                "name": "FILE_TYPE_MAPPINGS",
                "value": {
                    "image": [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"],
                    "video": [".mp4", ".mkv", ".avi", ".mov", ".webm"],
                    "pdf": [".pdf"],
                    "text": [".txt", ".md", ".json", ".xml", ".html", ".css", ".js"]
                },
                "default": {
                    "image": [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"],
                    "video": [".mp4", ".mkv", ".avi", ".mov", ".webm"],
                    "pdf": [".pdf"],
                    "text": [".txt", ".md", ".json", ".xml", ".html", ".css", ".js"]
                },
                "type": "json_obj",
                "description": "A JSON object mapping canonical types (e.g., 'image') to lists of file extensions (e.g., ['.jpg', '.jpeg']).",
                "category": "Processing"
            },
            {
                "name": "ENABLED_FILE_TYPES", "value": "image,video", "default": "image,video,text,pdf,generic", "type": "list_str",
                "description": "Comma-separated list of file types to process. Available: image, video, text, pdf, generic.",
                "category": "Processing"
            }
        ]

    def _substitute_variables(self, value: Any, all_raw_values: Dict[str, Any], seen: Optional[set] = None) -> Any:
        """
        Recursively substitutes variables in a string value, like $VAR or ${VAR}.
        Detects circular references.
        """
        if seen is None:
            seen = set()

        if not isinstance(value, str):
            return value

        import re
        pattern = re.compile(r'\$(\w+)|\$\{(\w+)\}')

        def replacer(match):
            var_name = match.group(1) or match.group(2)
            if var_name in seen:
                raise ValueError(f"Circular reference detected in config for variable: {var_name}")
            
            if var_name in all_raw_values:
                # The value from the raw map might itself contain variables.
                val_to_sub = all_raw_values[var_name]
                
                new_seen = seen.copy()
                new_seen.add(var_name)
                
                # Recursively substitute. The result must be a string to be part of the larger string.
                return str(self._substitute_variables(val_to_sub, all_raw_values, new_seen))
            
            return match.group(0) # If the variable is not in our config map, leave it as is.

        return pattern.sub(replacer, value)
    # --- NEW: Helper method to create directories ---
    def _ensure_path_exists(self, path: str, is_directory: bool) -> None:
        """
        Ensures that a path exists. If it's a directory path, it creates the directory.
        If it's a file path, it creates the parent directory.
        """
        target_dir = path if is_directory else os.path.dirname(path)
        if target_dir and not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
                logger.info(f"Created missing configuration directory: {target_dir}")
            except OSError as e:
                logger.error(f"Failed to create directory {target_dir}: {e}")

    def _parse_value(self, item: Dict[str, Any], root_dir: str) -> Any:
        """Parses a value from a config item dict based on its 'type' and ensures paths exist."""
        name = item.get('name', 'UNKNOWN')
        value = item.get('value')
        item_type = item.get('type')

        if value is None:
            raise ValueError(f"Configuration item '{name}' has no value.")

        if item_type == 'int':
            return int(value)
        if item_type == 'float':
            return float(value)
        if item_type == 'bool':
            return bool(int(value))
        if item_type == 'str':
            return str(value)
        
        # --- REFACTORED: Split path handling and add directory creation ---
        if item_type == 'dir_path':
            full_path = os.path.join(root_dir, os.path.expanduser(str(value)))
            self._ensure_path_exists(full_path, is_directory=True)
            return full_path
        
        if item_type == 'file_path' or item_type == 'path': # Treat 'path' as a file path
            full_path = os.path.join(root_dir, os.path.expanduser(str(value)))
            self._ensure_path_exists(full_path, is_directory=False)
            return full_path

        if item_type == 'list_str':
            value_str = value if isinstance(value, str) else ','.join(value)
            return [item.strip() for item in value_str.split(',') if item.strip()]
        
        if item_type in ['json_str', 'json_obj']:
            if isinstance(value, dict):
                return value # New format, or from update_config
            try:
                # Attempt to parse it as a string (old format) for backward compatibility
                return json.loads(str(value))
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Config for '{name}' has type '{item_type}' but value is not a valid dictionary or JSON string. Using empty dict.")
                return {}

        if item_type == 'list_path':
            paths_str = value if isinstance(value, str) else ','.join(value)
            resolved_paths = [os.path.expanduser(d.strip()) for d in paths_str.split(',') if d.strip()]
            for p in resolved_paths:
                self._ensure_path_exists(p, is_directory=True)
            return resolved_paths

        if item_type == 'root_path':
            if value == '.':
               
                path = os.path.dirname(os.path.dirname(self.paths[0]))
            else:
                path = os.path.expanduser(str(value))
            self._ensure_path_exists(path, is_directory=True)
            return path
        
        logger.warning(f"Unknown config type '{item_type}' for '{name}'. Using value as is.")
        return value
    
    def _format_value_for_json(self, value: Any, item_type: str) -> Any:
        """Converts a Python-native config value back to a JSON-serializable format."""
        if item_type == 'bool':
            return 1 if value else 0
        if item_type in ['list_path', 'list_str']:
            if isinstance(value, list):
                return ','.join(map(str, value))
        if item_type == 'json_obj':
            if isinstance(value, dict):
                return value # Persist as a native JSON object
        if item_type == 'json_str': # For backward compatibility on save
            if isinstance(value, dict):
                return json.dumps(value) # Persist as a string
        # For paths, we assume the provided value is the new desired string representation
        # int, str, path, etc. can be returned as is, as they are JSON-serializable.
        return value

    def load(self) -> None:
        """
        Loads configuration from the JSON file. If the file does not exist,
        it's created with defaults. The load is atomic.
        """
        primary_path = self.paths[0]
        programmatic_defaults = self._get_default_values()

        if not os.path.exists(primary_path):
            logger.info(f"Configuration file not found. Creating a default one at: {primary_path}")
            try:
                # Ensure the parent directory for the config file itself exists
                config_dir = os.path.dirname(primary_path)
                if config_dir:
                    os.makedirs(config_dir, exist_ok=True)

                with open(primary_path, "w") as f:
                    json.dump(programmatic_defaults, f, indent=4)
                logger.info("Default configuration file created successfully.")
                config_list_to_parse = programmatic_defaults
            except IOError as e:
                logger.error(f"FATAL: Could not create default config file at {primary_path}. Error: {e}")
                return
        else:
            try:
                with open(primary_path, 'r') as f:
                    user_config_list = json.load(f)

                user_map = {item['name']: item for item in user_config_list}
                file_needs_update = False
                for default_item in programmatic_defaults:
                    item_name = default_item['name']
                    if item_name not in user_map:
                        logger.info(f"Configuration key '{item_name}' is missing. Adding it to the config file with its default value.")
                        user_config_list.append(default_item)
                        file_needs_update = True
                    else:
                        # Key exists, check if the default value is correct and update if necessary.
                        user_item = user_map[item_name]
                        if user_item.get('default') != default_item.get('default'):
                            logger.info(f"Default value for '{item_name}' is incorrect in config file. Updating it.")
                            user_item['default'] = default_item['default']
                            file_needs_update = True
                
                if file_needs_update:
                    try:
                        with open(primary_path, 'w') as f:
                            json.dump(user_config_list, f, indent=4)
                        logger.info(f"Updated '{primary_path}' with missing default values.")
                    except IOError as e:
                        logger.error(f"Could not write updated config to {primary_path}: {e}")

                config_list_to_parse = user_config_list
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Error parsing {primary_path}: {e}. Using internal defaults.")
                config_list_to_parse = programmatic_defaults

        # --- NEW: Variable Substitution Pass ---
        # Create a map of raw values for substitution lookup.
        raw_config_values = {item['name']: item['value'] for item in config_list_to_parse}
        
        # Perform substitution on each value before parsing.
        try:
            for item in config_list_to_parse:
                # We only substitute if the value is a string.
                if isinstance(item['value'], str):
                    item['value'] = self._substitute_variables(item['value'], raw_config_values)
        except ValueError as e:
            # On error, log and continue. Subsequent parsing will likely fail on the invalid path,
            # which provides a clear error message to the user about which path is wrong.
            logger.error(f"Error during config variable substitution: {e}. "
                         "Processing will continue with potentially invalid values. Please check your config for circular references.")

        temp_config = {}
        try:
            root_dir_item = next((item for item in config_list_to_parse if item['name'] == 'ROOT_DIR'), None)
            if root_dir_item:
                 resolved_root_dir = self._parse_value(root_dir_item, "")
            else:
                 resolved_root_dir = os.path.dirname(primary_path)
            temp_config["ROOT_DIR"] = resolved_root_dir

            for item in config_list_to_parse:
                name = item.get('name')
                if not name or name == "ROOT_DIR":
                    continue
                temp_config[name] = self._parse_value(item, resolved_root_dir)
            
            self._config = temp_config
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing configuration values: {e}. Keeping previous config.")

    def reload(self) -> None:
        """Thread-safe method to reload the configuration from disk."""
        with self._lock:
            logger.info("Loading/Reloading configuration...")
            self.load()
            # --- REMOVED: The special-case check is now handled by the generic parsing logic ---
            # if not os.path.exists(self._config['FILESTORE_DIR']):
            #     os.mkdir(self._config['FILESTORE_DIR'])
            logger.info("Configuration process finished.")
            # Ensure the database file can be opened (and its directory exists)
            try:
                conn = sqlite3.connect(self._config['DB_FILE'])
                conn.close()
            except sqlite3.OperationalError as e:
                logger.error(f"Could not connect to database at {self._config['DB_FILE']}. Error: {e}")


    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._config.get(key, default)

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            return self._config[key]

    @property
    def data(self) -> ConfigDict:
        with self._lock:
            return self._config.copy()

    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Updates configuration values in memory and persists them to the primary config file.
        This operation is thread-safe and atomic. It only updates existing keys.
        Directory creation for paths will be triggered automatically during this process.
        """
        primary_path = self.paths[0]
        with self._lock:
            logger.info(f"Attempting to update configuration with: {updates}")
            
            try:
                with open(primary_path, 'r') as f:
                    current_config_list = json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                logger.error(f"Cannot update config: failed to read current config file at {primary_path}. Error: {e}")
                return

            config_map = {item['name']: item for item in current_config_list}
            new_parsed_config = self._config.copy()

            # --- NEW: Prepare for substitution ---
            # Create a map of what the raw values WILL BE after the update.
            # Start with the current raw values from the file.
            future_raw_values = {item['name']: item['value'] for item in current_config_list}
            # Then, overlay the incoming updates to this map.
            for key, value in updates.items():
                if key in config_map:
                    item_type = config_map[key]['type']
                    # We need the storable format of the new value for our raw map.
                    future_raw_values[key] = self._format_value_for_json(value, item_type)

            for key, value in updates.items():
                if key in config_map:
                    item = config_map[key]
                    item_type = item['type']
                    root_dir = new_parsed_config['ROOT_DIR']
                    
                    # The storable value is what we'll save back to the JSON file.
                    storable_value = self._format_value_for_json(value, item_type)
                    item['value'] = storable_value
                    
                    # --- NEW: Substitute before parsing ---
                    # The value to be parsed might contain variables (e.g., "$FILESTORE_DIR/...").
                    value_to_parse = storable_value
                    if isinstance(value_to_parse, str):
                        try:
                            value_to_parse = self._substitute_variables(value_to_parse, future_raw_values)
                        except ValueError as e:
                            logger.error(f"Error during config update for '{key}': {e}. Using raw value, which may be invalid.")

                    item_for_reparsing = {
                        'name': key,
                        'value': value_to_parse, # Use the substituted value for parsing
                        'type': item_type
                    }

                    # Re-parsing will automatically trigger _ensure_path_exists for path types
                    parsed_value = self._parse_value(item_for_reparsing, root_dir)
                    new_parsed_config[key] = parsed_value
                else:
                    logger.warning(f"Config key '{key}' not found in config file. Skipping update for this key.")
            
            updated_config_list = list(config_map.values())
            
            try:
                json_string_to_write = json.dumps(updated_config_list, indent=4)
            except TypeError as e:
                logger.error(f"FATAL: Could not update config. New values are not JSON-serializable. Error: {e}")
                return
            
            try:
                with open(primary_path, "w") as f:
                    f.write(json_string_to_write)
                
                self._config = new_parsed_config
                logger.info(f"Configuration updated and saved to {primary_path}")

            except IOError as e:
                logger.error(f"FATAL: Could not write updated config to {primary_path}. Error: {e}")
        self.reload()

def get_config(paths: Union[str, List[str], None] = None) -> ConfigManager:
    if paths is None:
        # --- Using a sub-directory for config is often a good practice ---
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_dir = os.path.join(script_dir, 'config')
        path_list = [os.path.join(config_dir, "config.json")]
    elif isinstance(paths, str):
        path_list = [os.path.abspath(paths)]
    else:
        path_list = [os.path.abspath(p) for p in paths]
    
    cache_key = tuple(sorted(path_list))

    with _cache_lock:
        if cache_key not in _config_cache:
            logger.info(f"Creating new ConfigManager for: {list(cache_key)}")
            _config_cache[cache_key] = ConfigManager(path_list)
        return _config_cache[cache_key]

# --- Watchdog Handler (Unchanged) ---
from watchdog.events import FileSystemEventHandler

class ConfigChangeHandler(FileSystemEventHandler):
    """A watchdog event handler that triggers a callback on file modification."""
    def __init__(self, file_paths: List[str], reload_callback: callable):
        self.watched_files = {os.path.abspath(p) for p in file_paths}
        self.reload_callback = reload_callback

    def on_modified(self, event):
        if not event.is_directory and os.path.abspath(event.src_path) in self.watched_files:
            logger.info(f"Configuration file '{os.path.basename(event.src_path)}' has been modified.")
            self.reload_callback()

if __name__ == '__main__':
    # This will now create a ./config/config.json by default and also create
    # the 'filesstore', '.transcoded_cache', and '.thumbnails' directories automatically.
    config = get_config()
    print("\n--- Initial Configuration ---")
    print(json.dumps(config.data, indent=2))
    print("-----------------------------\n")

    # Example of updating the config
    # This will automatically create the '~/Pictures' and '~/Desktop/ToSort' dirs if they don't exist
    updates_to_make = {
        "IMAGE_GPU_CONCURRENCY": 3,
        "AUTOMATICALLY_TAG_VIDEOS": True,
        "WATCH_DIRS": ["~/Pictures", "~/Desktop/ToSort"],
        "NON_EXISTENT_KEY": "some_value" # This will be skipped
    }
    # To run this example, remove the `exit()` from the original code
    # config.update_config(updates_to_make)

    # print("\n--- Configuration After Update ---")
    # print(json.dumps(config.data, indent=2))
    # print("----------------------------------\n")