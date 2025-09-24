import os
import shutil
import subprocess
import uuid


# Assuming supLogger is a local module you have
import logging

logger = logging.getLogger("UtilsFile")
logger.setLevel(logging.DEBUG)

LEVEL_COLORS = {
    "DEBUG": "\033[36m",    # Cyan
    "INFO": "\033[32m",     # Green
    "WARNING": "\033[33m",  # Yellow
    "ERROR": "\033[31m",    # Red
    "CRITICAL": "\033[41m", # Red background
}
RESET = "\033[0m"

class ColorFormatter(logging.Formatter):
    def format(self, record):
        level_color = LEVEL_COLORS.get(record.levelname, "")
        record.levelname = f"{level_color}{record.levelname}{RESET}"
        record.name = f"{LEVEL_COLORS['WARNING']}{record.name}{RESET}"
        return f"{record.levelname}\t{record.name}\t{record.getMessage()}"
    
logging_handler = logging.StreamHandler()
logging_handler.setLevel(logging.DEBUG)
logging_handler.setFormatter(ColorFormatter())


def get_original_filename(path: str) -> str:
    """
    Extracts the original filename from a path that is expected to be in the
    format: {uuid}_{original_filename}.
    If the format is not matched, it returns the original basename.
    """
    base_name = os.path.basename(path)
    parts = base_name.split('_', 1)
    if len(parts) == 2:
        try:
            # Try to parse the first part as a UUID.
            uuid.UUID(parts[0], version=4)
            # If successful, return the second part, which is the original filename.
            return parts[1]
        except ValueError:
            # If the first part is not a valid UUID, it's not the format we expect.
            # In this case, we assume the whole basename is the filename.
            pass
    return base_name

def truncate_string(text: str, max_len: int = 40) -> str:
    """Truncates a string to a max length, adding '...' if shortened."""
    if len(text) > max_len:
        return text[:max_len - 3] + "..."
    return text

# --- MODIFIED: To verify the existence of the Flatpak app before using it ---
_mpv_command_cache = None

def _find_mpv_command():
    """
    Finds the correct command to run mpv, checking for standard and VERIFIED
    Flatpak versions. The result is cached to avoid repeated lookups.

    Returns:
        list[str] or None: The command as a list (e.g., ['mpv'] or
                           ['flatpak', 'run', 'io.mpv.Mpv']) or None if not found.
    """
    global _mpv_command_cache
    # If we already determined the command, return the cached result
    if _mpv_command_cache is not None:
        return _mpv_command_cache if _mpv_command_cache else None

    # 1. Check for standard 'mpv' in the system PATH
    if shutil.which('mpv'):
        logger.info("Found 'mpv' in system PATH.")
        _mpv_command_cache = ['mpv']
        return _mpv_command_cache

    # 2. If not found, check if the 'flatpak' command itself exists
    if shutil.which('flatpak'):
        mpv_flatpak_id = 'io.mpv.Mpv'
        logger.info(f"Found 'flatpak', checking for installed app '{mpv_flatpak_id}'...")

        try:
            # --- THIS IS THE KEY CHANGE ---
            # Run 'flatpak info' to verify the app is actually installed.
            # We redirect stdout and stderr to DEVNULL to keep our console clean.
            result = subprocess.run(
                ['flatpak', 'info', mpv_flatpak_id],
                check=False,  # Set to False to prevent raising an error on failure
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            # If the command returns 0, the app is installed and we can use it.
            if result.returncode == 0:
                logger.info(f"Verified Flatpak MPV ('{mpv_flatpak_id}') is installed.")
                command = ['flatpak', 'run', mpv_flatpak_id]
                _mpv_command_cache = command
                return command
            else:
                logger.info(f"Flatpak MPV ('{mpv_flatpak_id}') is not installed.")

        except Exception as e:
            # This is a safeguard in case the subprocess call itself fails.
            logger.error(f"An error occurred while checking for Flatpak MPV: {e}")

    # 3. If neither standard mpv nor a verified Flatpak version was found, cache the failure.
    logger.info("No usable 'mpv' command found (checked PATH and verified Flatpak).")
    _mpv_command_cache = []  # Cache failure (empty list) so we don't check again
    return None