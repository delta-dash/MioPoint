# Searcher.py

import contextlib
import os
import json
import asyncio
import faiss
import imagehash
import torch
import numpy as np
from PIL import Image
import pprint
from typing import Optional, List, Dict, Any

# --- Custom Modules ---
from ConfigMedia import get_config
import logging
from ClipManager import get_clip_manager, ClipModelManager
from src.db_utils import AsyncDBContext, execute_db_query

# --- Initialization ---
CONFIG = get_config()
logger = logging.getLogger("Searcher")
logger.setLevel(logging.DEBUG)
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"



class ReverseSearcher:
    """An async class to perform reverse image searches using a multi-stage approach."""

    def __init__(self):
        """Initializes the ReverseSearcher."""
        logger.info("Initializing ReverseSearcher...")
        self.clip_manager: ClipModelManager = get_clip_manager()
        
        # --- Internal State ---
        self.faiss_index: Optional[faiss.Index] = None
        self.faiss_to_db_id: Optional[List[int]] = None
        self.index_last_loaded_time: float = 0.0
        
        # --- Centralized task management attributes ---
        self._lock: asyncio.Lock = asyncio.Lock()
        self._reload_task: Optional[asyncio.Task] = None
        self._reload_check_interval: int = 0  # Disabled by default
        
        # Perform an initial blocking load. This is acceptable in the constructor
        # if the object is created at startup.
        self._internal_reload_logic()
        
    async def configure_reloader(self, check_interval_seconds: int = 60) -> None:
        """
        Starts or updates the background task that periodically checks for and
        reloads the FAISS index.

        Args:
            check_interval_seconds: How often to check for an updated index file.
                                    Set to 0 to disable.
        """
        self._reload_check_interval = check_interval_seconds
        
        # Stop any existing task before starting a new one
        await self.stop_reloader_task()

        if self._reload_check_interval > 0:
            logger.info(f"Starting FAISS index auto-reloader task. Interval: {self._reload_check_interval}s")
            self._reload_task = asyncio.create_task(self._periodic_reloader_task())
        else:
            logger.info("FAISS index auto-reloader is disabled.")

    async def stop_reloader_task(self) -> None:
        """Gracefully stops the background reloader task."""
        if self._reload_task and not self._reload_task.done():
            self._reload_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reload_task
            logger.info("FAISS index auto-reloader task stopped.")
        self._reload_task = None

    async def shutdown(self) -> None:
        """Prepares the searcher for a graceful application shutdown."""
        logger.info("Shutting down ReverseSearcher...")
        await self.stop_reloader_task()
        logger.info("ReverseSearcher shutdown complete.")

    async def _periodic_reloader_task(self) -> None:
        """The background task that checks for index file updates."""
        while True:
            await asyncio.sleep(self._reload_check_interval)
            logger.debug("Auto-reloader checking for index updates...")
            await self.load_or_reload_indices()

    def _internal_reload_logic(self) -> None:
        """
        Synchronous method containing the core logic to load/reload the index
        from disk. This is safe to run in a thread pool.
        """
        INDEX_FILE = CONFIG.get('FAISS_INDEX_FILE', 'media_index.faiss')
        ID_MAP_FILE = CONFIG.get('FAISS_ID_MAP_FILE', 'faiss_to_db_id.json')
        
        if not (os.path.exists(INDEX_FILE) and os.path.exists(ID_MAP_FILE)):
            if self.faiss_index is not None:
                logger.warning("FAISS index files disappeared. Disabling live search.")
                self.faiss_index = None
            return

        try:
            index_mod_time: float = os.path.getmtime(INDEX_FILE)
            if index_mod_time <= self.index_last_loaded_time and self.faiss_index is not None:
                return  # Index is already up-to-date.

            logger.info(f"Change detected. Reloading FAISS index from '{INDEX_FILE}'...")
            new_faiss_index: faiss.Index = faiss.read_index(INDEX_FILE)
            
            with open(ID_MAP_FILE, 'r') as f:
                new_faiss_to_db_id: List[int] = json.load(f)

            # Atomically update instance variables
            self.faiss_index = new_faiss_index
            self.faiss_to_db_id = new_faiss_to_db_id
            self.index_last_loaded_time = index_mod_time
            logger.info(f"FAISS index reloaded successfully ({self.faiss_index.ntotal} vectors).")

        except Exception as e:
            logger.error(f"Could not reload FAISS index: {e}", exc_info=True)
            self.faiss_index = None
            self.faiss_to_db_id = None
            self.index_last_loaded_time = 0.0

    async def load_or_reload_indices(self) -> None:
        """
        Asynchronously triggers a check and potential reload of the FAISS index.
        This runs the blocking file I/O in a thread pool to avoid blocking
        the main event loop.
        """
        loop = asyncio.get_running_loop()
        async with self._lock:
            # Run the synchronous, blocking I/O code in the default executor
            await loop.run_in_executor(None, self._internal_reload_logic)

    async def _find_by_phash(self, p_hash_str: str) -> List[Dict[str, Any]]:
        """Finds items in the database with an exact pHash match."""
        query = """
            SELECT 'image' as type, fc.phash, ce.id as embedding_id
            FROM file_content fc JOIN clip_embeddings ce ON fc.id = ce.content_id
            WHERE fc.phash = ?
            UNION ALL
            SELECT 'video_scene' as type, s.phash, ce.id as embedding_id
            FROM scenes s JOIN clip_embeddings ce ON s.id = ce.scene_id
            WHERE s.phash = ?
        """
        params = (p_hash_str, p_hash_str)
        async with AsyncDBContext() as conn:
            return await execute_db_query(conn, query, params, fetch_all=True)

    async def _find_by_precalculated_similarity(self, embedding_id: int, top_k: int) -> List[Dict[str, Any]]:
        """Finds similar items using the pre-calculated similarity table."""
        query = """
            SELECT embedding_id_b as other_id, score FROM embedding_similarity WHERE embedding_id_a = ?
            UNION
            SELECT embedding_id_a as other_id, score FROM embedding_similarity WHERE embedding_id_b = ?
            ORDER BY score DESC LIMIT ?
        """
        params = (embedding_id, embedding_id, top_k)
        async with AsyncDBContext() as conn:
            related_items: List[Dict[str, Any]] = await execute_db_query(conn, query, params, fetch_all=True)

        if not related_items:
            return []
            
        # aiosqlite.Row objects can be accessed by key
        related_embedding_ids: List[int] = [item['other_id'] for item in related_items]
        details_map: Dict[int, Dict[str, Any]] = await self._get_embedding_details(related_embedding_ids)
        
        results: List[Dict[str, Any]] = []
        for item in related_items:
            other_id: int = item['other_id']
            score: float = item['score']
            if other_id in details_map:
                details = details_map[other_id].copy() # Use copy to avoid modifying cache
                details['similarity'] = f"{score:.4f}"
                results.append(details)
        return results

    async def _find_by_live_similarity(self, query_image: Image.Image, top_k: int) -> List[Dict[str, Any]]:
        """
        Generates an embedding for the query image and performs a live FAISS search.
        """
        if not self.faiss_index or not self.faiss_to_db_id:
            logger.warning("Live similarity search called but index/ID map is not available.")
            return []
        
        clip_model, clip_preprocess = await self.clip_manager.load_or_get_model(CONFIG.get('MODEL_REVERSE_IMAGE'))
        if not clip_model:
            logger.error("Cannot perform live search: CLIP model is not available.")
            return []
            
        image_input: torch.Tensor = clip_preprocess(query_image).unsqueeze(0).to(DEVICE)
        with torch.no_grad():
            query_features: torch.Tensor = clip_model.encode_image(image_input)
            query_features /= query_features.norm(dim=-1, keepdim=True)
        
        query_embedding: np.ndarray = query_features.cpu().numpy().astype(np.float32)
            
        distances, faiss_indices = self.faiss_index.search(query_embedding, top_k)
        if not faiss_indices.size:
            return []

        db_ids: List[int] = [self.faiss_to_db_id[i] for i in faiss_indices[0] if i < len(self.faiss_to_db_id)]
        details_map: Dict[int, Dict[str, Any]] = await self._get_embedding_details(db_ids)
        
        results: List[Dict[str, Any]] = []
        for i, db_id in enumerate(db_ids):
            if db_id in details_map:
                details = details_map[db_id].copy() # Use copy to avoid modifying cache
                similarity_score: float = 1.0 - (distances[0][i]**2 / 2)
                details['similarity'] = f"{similarity_score:.4f}"
                results.append(details)
        return results

    async def _get_embedding_details(self, embedding_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        Retrieves comprehensive "card" details for a list of embedding IDs.
        """
        if not embedding_ids: 
            return {}

        placeholders = ','.join('?' for _ in embedding_ids)
        # This query is optimized to use LEFT JOINs for fetching tags, which is generally
        # more performant than using a correlated subquery in the SELECT clause.
        query = f"""
            SELECT
                ce.id as embedding_id,
                fi.id as instance_id,
                fi.file_name,
                fi.original_created_at,
                COALESCE(fc.file_type, s_fc.file_type) as file_type,
                COALESCE(fc.is_encrypted, s_fc.is_encrypted) as is_encrypted,
                COALESCE(fc.file_hash, s_fc.file_hash) as file_hash,
                COALESCE(fc.duration_seconds, s_fc.duration_seconds) as duration_seconds,
                s.id as scene_id,
                s.start_timecode,
                GROUP_CONCAT(DISTINCT t.name) as tags,
                GROUP_CONCAT(DISTINCT mt.name) as meta_tags,
                CASE 
                    WHEN s.id IS NOT NULL THEN 'video_scene' 
                    ELSE 'image' 
                END as result_type
            FROM clip_embeddings ce
            /* Join to get content details, either directly or through a scene */
            LEFT JOIN file_content fc ON ce.content_id = fc.id
            LEFT JOIN scenes s ON ce.scene_id = s.id
            LEFT JOIN file_content s_fc ON s.content_id = s_fc.id
            /* Join to get an associated instance (there might be many, we just need one for a card) */
            LEFT JOIN file_instances fi ON fi.content_id = COALESCE(fc.id, s_fc.id)
            /* Join to get all associated tags and meta_tags */
            LEFT JOIN file_content_tags fct ON fct.content_id = COALESCE(fc.id, s_fc.id) 
            LEFT JOIN tags t ON fct.tag_id = t.id
            LEFT JOIN file_content_meta_tags fcmt ON fcmt.content_id = COALESCE(fc.id, s_fc.id)
            LEFT JOIN meta_tags mt ON fcmt.tag_id = mt.id
            WHERE ce.id IN ({placeholders})
            GROUP BY ce.id
        """
        async with AsyncDBContext() as conn:
            rows: List[Dict[str, Any]] = await execute_db_query(conn, query, tuple(embedding_ids), fetch_all=True)
        
        results_map: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            card_data: Dict[str, Any] = dict(row)
            # Ensure file_name is not None before calling os.path.basename
            if card_data.get('file_name'):
                card_data['display_name'] = os.path.basename(card_data['file_name'])
            else:
                card_data['display_name'] = "Unknown"
            card_data['tags'] = sorted(card_data['tags'].split(',')) if card_data.get('tags') else []
            card_data['meta_tags'] = sorted(card_data['meta_tags'].split(',')) if card_data.get('meta_tags') else []
            card_data['id'] = card_data.get('instance_id')
            results_map[card_data['embedding_id']] = card_data
        
        return results_map

    async def search(self, image_path: str, top_k: int = 5) -> Dict[str, Any]:
        """Performs the multi-stage reverse image search asynchronously."""
        if not os.path.exists(image_path):
            return {"status": "error", "message": f"Image file not found: {image_path}"}

        try:
            query_image: Image.Image = Image.open(image_path).convert("RGB")
        except Exception as e:
            return {"status": "error", "message": f"Could not open or process image: {e}"}

        # Acquire lock to ensure index is not reloaded mid-search
        async with self._lock:
            # Stage 1: Exact Match via pHash
            query_phash: str = str(imagehash.phash(query_image))
            exact_matches: List[Dict[str, Any]] = await self._find_by_phash(query_phash)
            
            # Stage 2 (Fast Path)
            if exact_matches:
                # Use key access for better readability with aiosqlite.Row
                embedding_id: int = exact_matches[0]['embedding_id']
                logger.info(f"Found exact match (embedding_id: {embedding_id}). Using pre-calculated similarities.")
                results: List[Dict[str, Any]] = await self._find_by_precalculated_similarity(embedding_id, top_k)
                if results:
                    return {"status": "success", "match_type": "precalculated_similarity", "query_found_in_db": True, "results": results}
                logger.warning("Exact match found, but no pre-calculated data. Falling back to live search.")

            # Stage 3: Fallback to Live FAISS Search
            logger.info("Performing live FAISS search...")
            if self.faiss_index is None:
                return {"status": "success", "match_type": "none", "query_found_in_db": bool(exact_matches), "message": "Live similarity index is not loaded or available."}

            results: List[Dict[str, Any]] = await self._find_by_live_similarity(query_image, top_k)
            return {"status": "success", "match_type": "live_clip_search" if results else "none", "query_found_in_db": bool(exact_matches), "results": results}


# ==============================================================================
# 1. SINGLETON MANAGEMENT 
# ==============================================================================
_searcher_instance: Optional[ReverseSearcher] = None
_searcher_lock = asyncio.Lock()

async def get_searcher() -> ReverseSearcher:
    """
    Returns the singleton instance of the ReverseSearcher.
    Initializes it on the first call. This is thread-safe.
    """
    global _searcher_instance
    if _searcher_instance is None:
        async with _searcher_lock:
            # Double-check locking to prevent race conditions
            if _searcher_instance is None:
                logger.info("Creating singleton ReverseSearcher instance...")
                _searcher_instance = ReverseSearcher()
                # You can configure the reloader here if desired
                await _searcher_instance.configure_reloader(check_interval_seconds=60)
    return _searcher_instance


# ==============================================================================
# Example Usage
# ==============================================================================
async def main():
    """Asynchronous main function to demonstrate the ReverseSearcher."""
    # --- PRE-REQUISITES ---
    # 1. Run scanner_service.py to ingest media.
    # 2. Run build_index.py to create the FAISS index.
    # 3. (Optional) Run build_relationships.py for the fastest search path.

    # --- !! IMPORTANT: UPDATE THIS PATH !! ---
    #
    # ===> TEST CASE 1: Image already in the database (will use Stages 1 and 2)
    # test_image_path = "C:/path/to/my_media_folder/cats/fluffy.jpg"
    #
    # ===> TEST CASE 2: New image not in the database (will use Stage 3)
    test_image_path = "C:/path/to/some/new_image/on_my_desktop.png"

    if "path/to/" in test_image_path:
        print("=" * 70)
        print("!! PLEASE UPDATE 'test_image_path' IN THE SCRIPT TO A REAL FILE !!")
        print(f"   Current path is a placeholder: '{test_image_path}'")
        print("=" * 70)
        await get_clip_manager().stop_cleanup_task()
        return

    try:
        await get_clip_manager().configure_idle_cleanup(idle_unload_seconds=300)
        searcher: ReverseSearcher = await get_searcher() # Use singleton getter

        print(f"\nSearching for images similar to: {test_image_path}\n")
        search_results: Dict[str, Any] = await searcher.search(test_image_path, top_k=5)

        print("--- SEARCH RESULTS ---")
        pprint.pprint(search_results)
        print("--- END OF RESULTS ---")

    except FileNotFoundError:
        print(f"\n[FILE ERROR] The test image path was not found: '{test_image_path}'")
    except Exception as e:
        logger.error("An unexpected error occurred during the example run.", exc_info=True)
        print(f"\n[UNEXPECTED ERROR] An unexpected error occurred: {e}")
    finally:
        logger.info("Example run finished. Shutting down managers.")
        if _searcher_instance:
            await _searcher_instance.shutdown()
        await get_clip_manager().unload()
        await get_clip_manager().stop_cleanup_task()


if __name__ == '__main__':
    asyncio.run(main())