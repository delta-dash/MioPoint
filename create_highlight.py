# highlight_creator.py (or your desired filename)

import asyncio
import argparse
import os
import sys

import ffmpeg
from ConfigMedia import get_config
import logging
from src.db_utils import AsyncDBContext, execute_db_query

# Import the new async database utilities

# --- Setup Configuration and Logger ---
config = get_config()
logger = logging.getLogger("HIGHLIGHT_CREATOR")
logger.setLevel(logging.DEBUG)


async def find_scenes_with_tags(video_path: str, tags: list[str]) -> list[dict]:
    """
    Asynchronously queries the database to find scenes from a specific video
    containing ANY of the given tags.
    """
    db_file = config.get("DB_FILE")
    if not db_file or not os.path.exists(db_file):
        logger.error(f"Database file '{db_file}' not found. Please run the main processor first.")
        return []

    scenes = []
    # Generate the correct number of placeholders for the IN clause
    placeholders = ', '.join('?' for _ in tags)

    # This query is robust for searching JSON arrays. It joins the scenes table
    # with an un-nested version of its own tags column to search for individual tags directly.
    query = f"""
        SELECT DISTINCT s.start_timecode, s.end_timecode
        FROM file_instances fi
        JOIN scenes s ON fi.content_id = s.content_id
        JOIN scene_tags st ON s.id = st.scene_id
        JOIN tags t ON st.tag_id = t.id
        WHERE fi.stored_path = ? AND t.name IN ({placeholders})
        ORDER BY s.start_timecode
    """

    params = (video_path, *tags)

    # Use the new async context manager and execution function
    try:
        async with AsyncDBContext() as conn:
            results = await execute_db_query(
                conn,
                query,
                params=params,
                fetch_all=True
            )
        
        if results:
            # aiosqlite.Row objects can be accessed like dictionaries
            scenes = [{'start': row['start_timecode'], 'end': row['end_timecode']} for row in results]

    except Exception as e:
        logger.error(f"Failed to query scenes from database: {e}", exc_info=True)
        # Return empty list on error

    return scenes


async def create_highlight_video(source_video: str, tags_str: str, output_filename: str):
    """
    Finds all scenes with specific tags and concatenates them into a new video file.
    """
    # Split the comma-separated string into a list of clean tags
    tags_list = [tag.strip().lower() for tag in tags_str.split(',') if tag.strip()]

    if not tags_list:
        logger.error("No valid tags provided.")
        return

    logger.info(f"Searching for scenes with tags {tags_list} in '{os.path.basename(source_video)}'...")
    scenes = await find_scenes_with_tags(source_video, tags_list)

    if not scenes:
        logger.warning("No scenes found with any of the specified tags. Exiting.")
        return

    logger.info(f"Found {len(scenes)} scenes. Preparing to generate highlight video...")

    input_stream = ffmpeg.input(source_video)
    clips = []
    for i, scene in enumerate(scenes):
        logger.info(f"  - Adding clip {i+1}: {scene['start']} -> {scene['end']}")
        clip = input_stream.video.trim(start=scene['start'], end=scene['end']).setpts('PTS-STARTPTS')
        audio = input_stream.audio.filter('atrim', start=scene['start'], end=scene['end']).filter('asetpts', 'PTS-STARTPTS')
        clips.append(clip)
        clips.append(audio)

    concatenated = ffmpeg.concat(*clips, v=1, a=1, n=len(scenes)).output(output_filename)

    try:
        logger.info(f"\nGenerating '{output_filename}'... (This may take a while)")
        # Note: ffmpeg.run() is a blocking operation. This is fine for a CLI script.
        concatenated.run(overwrite_output=True, quiet=True)
        logger.info("✅ Highlight video created successfully!")
    except ffmpeg.Error as e:
        logger.error("❌ ffmpeg error:", exc_info=False) # exc_info=False to avoid messy traceback here
        # Decode stderr for a readable error message
        error_message = e.stderr.decode('utf-8') if e.stderr else "No stderr output."
        sys.stderr.write(error_message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a highlight video from tagged scenes.")
    parser.add_argument("video_file", help="The full path to the source video file.")
    parser.add_argument("tags", help="A comma-separated list of tags to search for (e.g., '1girl,sky,tree').")
    parser.add_argument("-o", "--output", default="highlight.mp4", help="The name of the output highlight video file (default: highlight.mp4).")

    args = parser.parse_args()

    if not os.path.exists(args.video_file):
        logger.error(f"Source video file not found at '{args.video_file}'")
    else:
        # Run the main async function using asyncio
        asyncio.run(create_highlight_video(args.video_file, args.tags, args.output))