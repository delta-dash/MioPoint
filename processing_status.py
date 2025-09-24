import threading
from typing import Dict, Any, Optional
import time

class ProcessingStatusManager:
    """A thread-safe, centralized manager for tracking the status of long-running tasks."""

    def __init__(self):
        self._statuses: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start_processing(self, job_id: Any, initial_message: str = "Starting...", ingest_source: str = 'unknown', priority: str = 'NORMAL', filename: Optional[str] = None):
        """Registers a new job and sets its initial status."""
        with self._lock:
            self._statuses[job_id] = {
                "status": "processing",
                "progress": 0.0,
                "message": initial_message,
                "start_time": time.time(),
                "ingest_source": ingest_source,
                "priority": priority,
                "filename": filename,
            }

    def update_progress(self, job_id: str, progress: float, message: str):
        """Updates the progress of an ongoing job."""
        with self._lock:
            if job_id in self._statuses:
                self._statuses[job_id]["progress"] = round(progress, 2)
                self._statuses[job_id]["message"] = message

    def set_completed(self, job_id: str, final_message: str = "Completed successfully"):
        """Marks a job as completed."""
        with self._lock:
            # On successful completion, remove the job from tracking.
            # The UI will see it disappear from the "currently processing" list.
            if job_id in self._statuses:
                del self._statuses[job_id]
    
    def set_error(self, job_id: str, error_message: str):
        """Marks a job as failed with an error message."""
        with self._lock:
            if job_id in self._statuses:
                self._statuses[job_id].update({
                    "status": "error",
                    # Progress is not set to 100 to indicate it failed mid-way.
                    "message": error_message,
                })

    def get_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Gets the status of a single job."""
        with self._lock:
            # Return a copy to prevent external modification
            return self._statuses.get(job_id, {}).copy()

    def get_all_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Gets the status of all tracked jobs."""
        with self._lock:
            # Return a deep copy to ensure thread safety
            return {k: v.copy() for k, v in self._statuses.items()}

# Create a singleton instance to be used across the application
status_manager = ProcessingStatusManager()