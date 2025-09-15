# build_index.py
import logging
import numpy as np
import faiss
import json
import asyncio

from ConfigMedia import get_config
from ClipManager import get_clip_manager
from src.db_utils import AsyncDBContext, execute_db_query
logger = logging.getLogger("IndexBuilder")
logger.setLevel(logging.DEBUG)
CONFIG = get_config()

async def get_embedding_dimension(model_name: str) -> int:
    """
    Gets the embedding dimension for a given CLIP model name via the manager.
    The dimension is independent of the data type (e.g., float16 vs float32).
    """
    clip_manager = get_clip_manager()
    try:
        # We need the model loaded to inspect it. The manager handles this efficiently.
        # The .visual.output_dim attribute gives the number of features, e.g., 512.
        model, _ = await clip_manager.load_or_get_model(model_name)
        if model:
            return model.visual.output_dim
        logger.error(f"Failed to load model '{model_name}' via ClipManager to get dimension.")
        return 0
    except Exception as e:
        logger.error(f"Could not get dimension for model '{model_name}': {e}", exc_info=True)
        return 0

async def build_faiss_index():
    """
    Reads embeddings from the database for the currently configured model,
    builds a FAISS index, and saves it along with an ID map to disk.

    This version is robust and handles embeddings stored as either float16 (half-precision)
    or float32 (full-precision).
    """
    # Configuration is now read from the central config object
    INDEX_FILE = CONFIG.get('FAISS_INDEX_FILE', 'media_index.faiss')
    ID_MAP_FILE = CONFIG.get('FAISS_ID_MAP_FILE', 'faiss_to_db_id.json')
    TARGET_MODEL_NAME = CONFIG.get('MODEL_REVERSE_IMAGE')

    if not TARGET_MODEL_NAME:
        logger.error("MODEL_REVERSE_IMAGE is not defined in the config. Cannot build index.")
        return

    logger.info(f"Attempting to build index for model: '{TARGET_MODEL_NAME}'")

    embedding_dim = await get_embedding_dimension(TARGET_MODEL_NAME)
    if embedding_dim == 0:
        # Error message is already logged in the helper function
        return

    logger.info(f"Expected embedding dimension for this model is: {embedding_dim}")

    # --- Refactored Database Access ---
    rows = []
    try:
        logger.info(f"Fetching embeddings that match model '{TARGET_MODEL_NAME}' from database...")
        # Use the async context manager for a single, managed transaction
        async with AsyncDBContext() as conn:
            # The `clip_embeddings` table is the correct source for building the index,
            # as it contains the raw embedding vectors for each file or scene.
            rows = await execute_db_query(
                conn,
                "SELECT ce.id, ce.embedding FROM clip_embeddings ce WHERE ce.model_name = ?",
                (TARGET_MODEL_NAME,),
                fetch_all=True
            )
    except Exception as e:
        # Specific database errors are logged within execute_db_query
        logger.error(f"Failed to fetch embeddings from the database. Halting index build.", exc_info=False)
        return

    if not rows:
        logger.warning(f"No embeddings found in the database for model '{TARGET_MODEL_NAME}'. Index will not be created.")
        return

    logger.info(f"Found {len(rows)} raw embeddings to process.")

    faiss_ids = []
    embeddings_list = []

    # Define expected sizes based on the model's dimension
    expected_size_fp16 = embedding_dim * 2  # 512 * 2 = 1024 bytes
    expected_size_fp32 = embedding_dim * 4  # 512 * 4 = 2048 bytes

    # --- Crucial Logic: Validate and parse each embedding blob (no changes here) ---
    for db_row in rows: # The new DB utils return aiosqlite.Row objects
        db_id = db_row['id']
        blob = db_row['embedding']
        blob_len = len(blob)

        if blob_len == expected_size_fp16:
            # It's a float16 (half-precision) embedding
            numpy_vector = np.frombuffer(blob, dtype=np.float16)
            embeddings_list.append(numpy_vector)
            faiss_ids.append(db_id)
        elif blob_len == expected_size_fp32:
            # It's a float32 (full-precision) embedding
            numpy_vector = np.frombuffer(blob, dtype=np.float32)
            embeddings_list.append(numpy_vector)
            faiss_ids.append(db_id)
        else:
            # The blob size is incorrect for this model's dimension
            logger.warning(f"Skipping embedding for id {db_id} due to incorrect size: {blob_len} bytes. Expected {expected_size_fp16} (fp16) or {expected_size_fp32} (fp32).")
            continue

    if not embeddings_list:
        logger.warning("No embeddings with a valid size were found after filtering. Index will not be created.")
        return

    # Convert all parsed embeddings to a single numpy array of float32 for FAISS.
    embeddings_np = np.array(embeddings_list, dtype=np.float32)
    embeddings_np = np.ascontiguousarray(embeddings_np)

    logger.info(f"Building FAISS index (IndexFlatL2) with dimension {embedding_dim}...")
    index = faiss.IndexFlatL2(embedding_dim)
    index.add(embeddings_np)
    logger.info(f"FAISS index built successfully. Total vectors indexed: {index.ntotal}")

    try:
        logger.info(f"Saving FAISS index to '{INDEX_FILE}'...")
        faiss.write_index(index, INDEX_FILE)

        logger.info(f"Saving ID map to '{ID_MAP_FILE}'...")
        with open(ID_MAP_FILE, 'w') as f:
            json.dump(faiss_ids, f)

        logger.info("Index and ID map have been successfully created/updated for the current model.")
    except Exception as e:
        logger.error(f"Failed to save index or ID map files: {e}", exc_info=True)

        