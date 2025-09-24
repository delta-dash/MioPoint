from dataclasses import dataclass, field
from typing import Any, List, Optional
from concurrent.futures import ThreadPoolExecutor
import asyncio

from ImageTagger import Tagger
from TaskManager import Priority

@dataclass
class ProcessingJob:
    """Holds all information needed for a media processing task."""
    executor: ThreadPoolExecutor
    path: str
    ingest_source: str
    tagger_model_name: str
    clip_model_name: str
    tagger: Tagger  # The actual tagger model object
    clip_model: Any  # The actual CLIP model object
    clip_preprocess: Any  # The CLIP preprocessing object
    uploader_id: Optional[int]
    default_visibility_roles: List[int]
    original_filename: str

    # This gives the job access to all shared state like queues, locks, and counters.
    app_state: dict
    priority: Priority
    # A dictionary for passing extra context, e.g., original_content_id for conversions.
    context: dict = field(default_factory=dict)

@dataclass
class MediaData:
    """
    A Data Transfer Object (DTO) that holds all extracted and generated media
    information, structured to support the content/instance database model.

    This object is populated during the processing pipeline and is the primary
    input for the `DatabaseService.add_media` method.
    """
    # --- Fields without default values (must be provided at initialization) ---
    file_hash: str
    file_type: str
    extension: str
    initial_physical_path: str # The path to the file before persistence.
    file_name: str
    sanitized_content_path: str # Path used by MediaDB to determine the logical folder.

    # --- Content-Specific Data (Tied to file_hash) ---
    # This data describes the unique content of the file. If another file with
    # the same hash is ingested, this information is reused.
    size_in_bytes: Optional[int] = None
    duration_seconds: Optional[float] = None
    phash: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    transcript: Optional[str] = None
    is_encrypted: bool = False
    is_webready: bool = False
    tags_list: list[str] = field(default_factory=list)
    clip_embedding: Optional[bytes] = None
    scenes_data: list[dict] = field(default_factory=list)

    # --- Instance-Specific Data (Tied to a specific stored file) ---
    # This data describes a particular copy or instance of the file.
    stored_path: Optional[str] = None  # The final, absolute path where the file is stored. Set by persistence service.
    original_created_at: Optional[str] = None
    original_modified_at: Optional[str] = None
    folder_id: Optional[int] = None # FK to the 'folders' table, set during ingestion.
    
    # Instance-specific visibility: Defines who can see this particular copy.
    # If empty, it defaults to the 'Everyone' role during ingestion.
    visibility_roles: List[int] = field(default_factory=list)
    
    # --- Processing & Contextual Data ---
    # Information about how and by whom this media was processed and ingested.
    ingest_source: str = "manual"
    uploader_id: Optional[int] = None
    
    # Name of the primary model used for generating tags.
    model_name: Optional[str] = None
    # Name of the model used for generating CLIP embeddings.
    clip_model_name: Optional[str] = None
