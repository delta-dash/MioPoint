# visualizer_app.py (FastAPI Version)

import io
import sqlite3
from fastapi import FastAPI, Response, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from PIL import Image
import cv2  # Make sure you have opencv-python installed
import itertools
from collections import Counter, defaultdict

import src.media.MediaDB as MediaDB
import logging  # Your existing module

# --- App Setup ---
app = FastAPI()

# Mount the 'static' directory to serve files like script.js
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup for rendering the HTML template
templates = Jinja2Templates(directory="templates")

THUMBNAIL_SIZE = (256, 256)

db_logger = logging.getLogger("Visualizer_app")
logger.setLevel(logging.DEBUG)

def get_visualization_data():
    """
    Fetches all tags and a sample of their associated files for visualization.
    The file data now includes the file's ID and a complete list of all its tags.
    """
    db_logger.info("Fetching data for visualization...")
    with sqlite3.connect(MediaDB.DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. Fetch all tags with their hierarchy
        cursor.execute("SELECT id, name, parent_id FROM tags")
        all_tags = [dict(row) for row in cursor.fetchall()]

        # 2. Get all file-to-tag relationships from both files and scenes
        # This creates a map like: {file_id: ['tag1', 'tag2', 'tag3'], ...}
        file_to_all_tags_map = defaultdict(list)
        query = """
        SELECT file_id, tag_name FROM (
            SELECT ft.file_id, t.name as tag_name FROM file_tags ft JOIN tags t ON ft.tag_id = t.id
            UNION
            SELECT s.file_id, t.name as tag_name FROM scene_tags st JOIN scenes s ON st.scene_id = s.id JOIN tags t ON st.tag_id = t.id
        ) ORDER BY file_id, tag_name;
        """
        cursor.execute(query)
        for row in cursor.fetchall():
            file_to_all_tags_map[row['file_id']].append(row['tag_name'])
        
        # 3. Get a sample of file IDs for each tag (your existing logic)
        tag_to_sample_files_map = defaultdict(list)
        query = """
        SELECT tag_id, file_id FROM (
            SELECT
                tag_id,
                file_id,
                ROW_NUMBER() OVER(PARTITION BY tag_id ORDER BY file_id DESC) as rn
            FROM (
                SELECT tag_id, file_id FROM file_tags
                UNION
                SELECT st.tag_id, s.file_id FROM scene_tags st JOIN scenes s ON st.scene_id = s.id
            )
        ) WHERE rn <= 5;
        """
        cursor.execute(query)
        for row in cursor.fetchall():
            tag_to_sample_files_map[row['tag_id']].append(row['file_id'])

        # 4. Combine the data into the final structure
        results = []
        for tag_data in all_tags:
            tag_id = tag_data['id']
            sample_file_ids = tag_to_sample_files_map.get(tag_id, [])
            
            files_with_details = []
            for file_id in sample_file_ids:
                files_with_details.append({
                    "id": file_id,
                    "tags": file_to_all_tags_map.get(file_id, [])
                })
            
            results.append({
                "id": tag_id,
                "name": tag_data['name'],
                "parent_id": tag_data['parent_id'],
                "files": files_with_details
            })

    db_logger.info(f"Assembled data for {len(results)} tags with full tag details for sample files.")
    return results

def get_image_grid_data(limit=200):
    """
    Fetches a flat list of files, each with its complete list of tags.
    This is optimized for a force-directed graph or grid layout.
    
    Args:
        limit (int): The maximum number of files to return to keep the visualization performant.
    
    Returns:
        list: A list of dictionaries, e.g., [{'id': 1, 'tags': ['tagA', 'tagB']}, ...]
    """
    db_logger.info(f"Fetching grid data for visualization (limit: {limit})...")
    with sqlite3.connect(MediaDB.DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. Get all file-to-tag relationships into a Python dictionary
        # This is more efficient than complex SQL joins for aggregation here.
        file_to_tags_map = defaultdict(list)
        query = """
        SELECT file_id, t.name as tag_name
        FROM (
            SELECT ft.file_id, ft.tag_id FROM file_tags ft
            UNION ALL
            SELECT s.file_id, st.tag_id FROM scene_tags st JOIN scenes s ON st.scene_id = s.id
        ) AS all_refs
        JOIN tags t ON all_refs.tag_id = t.id
        ORDER BY all_refs.file_id;
        """
        cursor.execute(query)
        for row in cursor.fetchall():
            file_to_tags_map[row['file_id']].append(row['tag_name'])
        
        # 2. Get a list of file IDs to use, applying the limit
        # We'll grab the most recently added files.
        cursor.execute("SELECT id FROM files ORDER BY id DESC LIMIT ?", (limit,))
        file_ids = [row['id'] for row in cursor.fetchall()]

        # 3. Assemble the final flat list
        results = []
        for file_id in file_ids:
            # Only include files that actually have tags
            if file_id in file_to_tags_map:
                results.append({
                    "id": file_id,
                    "tags": file_to_tags_map[file_id]
                })

    db_logger.info(f"Assembled grid data for {len(results)} files.")
    return results



# --- Helper Function (Unchanged) ---
def build_tree(elements):
    """
    Transforms a flat list of tags (with parent_id) into a hierarchical
    tree structure that D3.js can understand.
    """
    elements_map = {el['id']: el for el in elements}
    for el in elements:
        el['children'] = []
    
    root = {'id': None, 'name': 'All Tags', 'children': []}

    for el in elements:
        parent_id = el.get('parent_id')
        if parent_id in elements_map:
            elements_map[parent_id]['children'].append(el)
        else:
            root['children'].append(el)
            
    return root


# --- API Endpoints ---

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serves the main HTML page using Jinja2 templates."""
    return templates.TemplateResponse("index.html", {"request": request})



@app.get("/api/image-grid-data")
async def get_grid_data():
    """
    API endpoint that provides a flat list of images and their tags,
    suitable for a force-directed or grid layout.
    """
    # Using the new function from MediaDB
    # You can adjust the limit for performance vs. completeness
    image_data = MediaDB.get_image_grid_data(limit=150)
    if not image_data:
        raise HTTPException(status_code=404, detail="No tagged images found to visualize.")
    return image_data




@app.get("/api/tags")
async def get_tags_tree():
    """
    API endpoint that provides the tag hierarchy in JSON format.
    FastAPI automatically converts the dictionary to a JSON response.
    """
    flat_data = MediaDB.get_visualization_data()
    tree_data = build_tree(flat_data)
    return tree_data



@app.get("/thumbnail/{file_id}")
async def get_thumbnail(file_id: int):
    """
    API endpoint that generates and serves a thumbnail for a given file ID.
    Handles both images and videos.
    """
    try:
        with sqlite3.connect(MediaDB.DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT stored_path, file_type FROM files WHERE id = ?", (file_id,))
            result = cursor.fetchone()
            if not result:
                # Use HTTPException for standard HTTP errors in FastAPI
                raise HTTPException(status_code=404, detail="File not found")

            stored_path, file_type = result
            img_byte_arr = io.BytesIO()

            if file_type == 'image':
                with Image.open(stored_path) as img:
                    img.thumbnail(THUMBNAIL_SIZE)
                    img = img.convert("RGB")
                    img.save(img_byte_arr, format='JPEG')

            elif file_type == 'video':
                cap = cv2.VideoCapture(stored_path)
                ret, frame = cap.read()
                cap.release()
                if not ret:
                    raise HTTPException(status_code=500, detail="Could not read video frame")
                
                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                img = Image.fromarray(frame_rgb)
                img.thumbnail(THUMBNAIL_SIZE)
                img.save(img_byte_arr, format='JPEG')
            else:
                 raise HTTPException(status_code=404, detail="Thumbnail not available for this file type")

            img_byte_arr.seek(0)
            # Use FastAPI's Response class for custom content types
            return Response(content=img_byte_arr.read(), media_type='image/jpeg')

    except Exception as e:
        print(f"Error generating thumbnail for file_id {file_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")