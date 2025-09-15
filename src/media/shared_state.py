# app/shared_state.py

# This dictionary will store the progress of ongoing conversions.
# It is shared in-memory across the application.
conversion_progress_cache: dict = {}