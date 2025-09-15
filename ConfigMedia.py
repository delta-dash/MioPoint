# ConfigMedia.py
import json
import logging
import os
import sqlite3
import threading
from typing import Dict, Any, List, Union


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
                "name": "ROOT_DIR", "value": ".", "type": "root_path",
                "description": "The root directory for all other relative paths. '.' means the directory where this config file is located."
            },
            {
                "name": "IMAGE_GPU_CONCURRENCY", "value": 2, "type": "int",
                "description": "Number of concurrent image processing tasks on the GPU."
            },
            {
                "name": "VIDEO_GPU_CONCURRENCY", "value": 1, "type": "int",
                "description": "Number of concurrent video processing tasks on the GPU."
            },
            {
                "name": "SKIP_SCAN_ON_START", "value": 1, "type": "bool",
                "description": "If 1 (true), skips the media scan when the application starts. Set to 0 (false) to scan on start."
            },
            {
                "name": "DELETE_ON_PROCESS", "value": 1, "type": "bool",
                "description": "If 1 (true), deletes original files from WATCH_DIRS after they are successfully processed and moved to FILESTORE_DIR."
            },
            {
                "name": "AUTOMATICALLY_TAG_IMG", "value": 1, "type": "bool",
                "description": "If 1 (true), automatically generate tags for new images."
            },
            {
                "name": "AUTOMATICALLY_TAG_VIDEOS", "value": 0, "type": "bool",
                "description": "If 1 (true), automatically generate tags for new videos."
            },
            {
                "name": "DB_FILE", "value": "filesstore/library.db", "type": "file_path", # <-- CHANGED
                "description": "Path to the SQLite database file, relative to ROOT_DIR."
            },
            {
                "name": "FILESTORE_DIR", "value": "filesstore", "type": "dir_path",
                "description": "Directory where processed media files are stored, relative to ROOT_DIR."
            },
            {
                "name": "WATCH_DIRS", "value": "~/Downloads,~/Documents", "type": "list_path",
                "description": "Comma-separated list of directories to watch for new media files. Supports '~' for home directory."
            },
            {
                "name": "MODEL_TAGGER", "value": "wd-swinv2-tagger-v3", "type": "str",
                "description": "The name of the model used for automatic tagging."
            },
            {
                "name": "MODEL_REVERSE_IMAGE", "value": "ViT-B/32", "type": "str",
                "description": "The CLIP model for reverse image search. Available: ['RN50', 'RN101', 'RN50x4', 'RN50x16', 'RN50x64', 'ViT-B/32', 'ViT-B/16', 'ViT-L/14', 'ViT-L/14@336px']"
            },
            {
                "name": "FAISS_INDEX_FILE", "value": "filesstore/media_index.faiss", "type": "file_path", # <-- CHANGED
                "description": "Path to the FAISS index file for reverse image search, relative to ROOT_DIR."
            },
            {
                "name": "FAISS_ID_MAP_FILE", "value": "filesstore/faiss_to_db_id.json", "type": "file_path", # <-- CHANGED
                "description": "Path to the mapping file between FAISS indices and database IDs, relative to ROOT_DIR."
            },
            {
                "name": "TRANSCODED_CACHE_DIR", "value": "filesstore/.transcoded_cache", "type": "dir_path",
                "description": "Cache directory for transcoded video files, relative to ROOT_DIR."
            },
            {
                "name": "THUMBNAILS_DIR", "value": "filesstore/.thumbnails", "type": "dir_path",
                "description": "Directory for storing generated thumbnails, relative to ROOT_DIR."
            },
            {
                "name": "DEFAULT_VISIBILITY_ROLES", "value": "Admin", "type": "list_str",
                "description": "Comma-separated list of role NAMES to apply to new files by default. If empty, will default to 'Admin'."
            },
            {
                "name": "ENABLE_PERCEPTUAL_DUPLICATE_CHECK", "value": 0, "type": "bool",
                "description": "If 1 (true), enables checking for visually similar (non-identical) videos and image using CLIP embeddings. This is Will prevent adding similar files."
            },
            {
                "name": "DELETE_LOWER_QUALITY_DUPLICATE", "value": 0, "type": "bool",
                "description": "If 1 (true), enables deleting for visually similar (non-identical) videos and image using CLIP embeddings."
            },
            {
                "name": "PERCEPTUAL_DUPLICATE_THRESHOLD", "value": 0.01, "type": "float",
                "description": "The FAISS distance threshold for flagging a perceptual duplicate. Lower is stricter. (e.g., 0.05 means >99.5% cosine similarity)."
            }
        ]

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
            full_path = os.path.join(root_dir, value)
            self._ensure_path_exists(full_path, is_directory=True)
            return full_path
        
        if item_type == 'file_path' or item_type == 'path': # Treat 'path' as a file path
            full_path = os.path.join(root_dir, value)
            self._ensure_path_exists(full_path, is_directory=False)
            return full_path

        if item_type == 'list_str':
            value_str = value if isinstance(value, str) else ','.join(value)
            return [item.strip() for item in value_str.split(',') if item.strip()]
        
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
        default_map = {item['name']: item for item in programmatic_defaults}

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
                merged_map = {**default_map, **user_map}
                config_list_to_parse = list(merged_map.values())
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Error parsing {primary_path}: {e}. Using internal defaults.")
                config_list_to_parse = programmatic_defaults

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

            for key, value in updates.items():
                if key in config_map:
                    item = config_map[key]
                    item_type = item['type']
                    root_dir = new_parsed_config['ROOT_DIR']
                    storable_value = self._format_value_for_json(value, item_type)
                    item['value'] = storable_value
                    
                    item_for_reparsing = {
                        'name': key,
                        'value': storable_value,
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