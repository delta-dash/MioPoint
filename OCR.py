# OCR.py

import math
import threading
import easyocr
import cv2
import random
import numpy as np
from enum import Enum, auto
from typing import List, Union

# --- Definitions remain outside the class as they are general utilities ---

class StartDirection(Enum):
    TOP_LEFT = auto()
    TOP_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_RIGHT = auto()


class GroupSort(Enum):
    HORIZONTAL_FIRST = auto()
    VERTICAL_FIRST = auto()


def distance(a, b):
    return math.hypot(a["center"][0] - b["center"][0], a["center"][1] - b["center"][1])


def sort_reading_order(group, start_direction, group_sort_strategy=None):
    # (Implementation is unchanged)
    if not group: return []
    if group_sort_strategy is None:
        if start_direction in (StartDirection.TOP_RIGHT, StartDirection.BOTTOM_RIGHT):
            group_sort_strategy = GroupSort.VERTICAL_FIRST
        else:
            group_sort_strategy = GroupSort.HORIZONTAL_FIRST
    if group_sort_strategy == GroupSort.VERTICAL_FIRST:
        if len(group) == 0: return []
        avg_width = sum(e["width"] for e in group) / len(group)
        horizontal_tol = avg_width * 1.5
        group.sort(key=lambda e: e["center"][0], reverse=True)
        columns = []
        if not group: return []
        current_column = [group[0]]
        for e in group[1:]:
            avg_column_x = sum(w["center"][0] for w in current_column) / len(current_column)
            if abs(e["center"][0] - avg_column_x) <= horizontal_tol:
                current_column.append(e)
            else:
                columns.append(current_column)
                current_column = [e]
        if current_column: columns.append(current_column)
        columns.sort(key=lambda col: sum(e["center"][0] for e in col) / len(col), reverse=True)
        sorted_group = []
        is_bottom_up = start_direction in (StartDirection.BOTTOM_LEFT, StartDirection.BOTTOM_RIGHT)
        for col in columns:
            col.sort(key=lambda w: w["center"][1], reverse=is_bottom_up)
            sorted_group.extend(col)
        return sorted_group
    else:
        if len(group) == 0: return []
        avg_height = sum(e["height"] for e in group) / len(group)
        vertical_tol = avg_height * 0.7
        is_bottom_up = start_direction in (StartDirection.BOTTOM_LEFT, StartDirection.BOTTOM_RIGHT)
        group.sort(key=lambda e: e["center"][1], reverse=is_bottom_up)
        lines = []
        if not group: return []
        current_line = [group[0]]
        for e in group[1:]:
            avg_line_y = sum(w["center"][1] for w in current_line) / len(current_line)
            if abs(e["center"][1] - avg_line_y) <= vertical_tol:
                current_line.append(e)
            else:
                lines.append(current_line)
                current_line = [e]
        if current_line: lines.append(current_line)
        lines.sort(key=lambda line: sum(e["center"][1] for e in line) / len(line), reverse=is_bottom_up)
        sorted_group = []
        is_right_to_left = start_direction in (StartDirection.TOP_RIGHT, StartDirection.BOTTOM_RIGHT)
        for line in lines:
            line.sort(key=lambda w: w["center"][0], reverse=is_right_to_left)
            sorted_group.extend(line)
        return sorted_group


class TextGrouper:
    """
    A class to efficiently process multiple images for text grouping and sorting.
    It encapsulates an EasyOCR reader instance.
    This class is intended to be managed by the `get_textgrouper` factory function.
    """
    def __init__(self, lang_list: List[str] = ['en'], gpu: bool = True):
        print(f"Initializing EasyOCR Reader for languages: {lang_list} (GPU: {gpu}). This may take a moment...")
        self.reader = easyocr.Reader(lang_list, gpu=gpu)
        print("Reader initialized.")

    def group_text(self, **kwargs):
        # Implementation is the same, just calling it from the class
        return self._group_text_logic(**kwargs)

    def _group_text_logic(
        self,
        image_path=None,
        image_data=None,
        output_debug_path=None,
        start_direction=StartDirection.TOP_LEFT,
        group_sort_strategy=None,
        proximity_scale=2.5
    ):
        if image_path is None and image_data is None:
            raise ValueError("Either 'image_path' or 'image_data' must be provided.")

        if image_data is not None:
            image_to_process = image_data
        else:
            image_to_process = cv2.imread(image_path)
            if image_to_process is None:
                raise FileNotFoundError(f"Could not read the image at: {image_path}")

        results = self.reader.readtext(image_to_process)
        
        elements = []
        for bbox, text, conf in results:
            (x_min, y_min), _, (x_max, y_max), _ = bbox
            elements.append({
                "bbox": bbox, "text": text, "confidence": conf,
                "height": abs(y_max - y_min), "width": abs(x_max - x_min),
                "center": ((x_min + x_max) / 2, (y_min + y_max) / 2)
            })

        if not elements:
            return []

        # (Grouping and sorting logic is unchanged)
        if start_direction == StartDirection.TOP_RIGHT:
            elements.sort(key=lambda e: (e["center"][1], -e["center"][0]))
        elif start_direction == StartDirection.TOP_LEFT:
            elements.sort(key=lambda e: (e["center"][1], e["center"][0]))
        elif start_direction == StartDirection.BOTTOM_LEFT:
            elements.sort(key=lambda e: (-e["center"][1], e["center"][0]))
        else: # BOTTOM_RIGHT
            elements.sort(key=lambda e: (-e["center"][1], -e["center"][0]))
        
        unassigned = set(range(len(elements)))
        groups = []
        while unassigned:
            idx = sorted(list(unassigned))[0]
            group = [elements[idx]]
            unassigned.remove(idx)
            queue = [idx]
            while queue:
                current_idx = queue.pop(0)
                for j in list(unassigned):
                    threshold = ((elements[j]["height"] + elements[current_idx]["height"]) / 2) * proximity_scale
                    if distance(elements[j], elements[current_idx]) <= threshold:
                        group.append(elements[j])
                        unassigned.remove(j)
                        queue.append(j)
            group = sort_reading_order(group, start_direction, group_sort_strategy)
            groups.append(group)

        if output_debug_path:
            debug_image = image_to_process.copy()
            colors = [tuple(random.randint(50, 255) for _ in range(3)) for _ in groups]
            for idx, group in enumerate(groups):
                color = colors[idx]
                for item_idx, item in enumerate(group):
                    pts = np.array(item["bbox"], dtype=np.int32)
                    cv2.polylines(debug_image, [pts], isClosed=True, color=color, thickness=2)
                    cv2.putText(debug_image, str(item_idx + 1), (int(item["bbox"][0][0]), int(item["bbox"][0][1]) - 5),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
            cv2.imwrite(output_debug_path, debug_image)
            print(f"Debug image saved to {output_debug_path}")

        return groups

# --- CORRECTED FACTORY FUNCTION ---

_CACHED_GROUPERS = {}
_grouper_lock = threading.Lock() # Global lock for creating groupers

def get_textgrouper(lang_list: List[str] = ['en'], gpu: bool = True) -> TextGrouper:
    """
    Factory function to get a thread-safe, cached TextGrouper instance.

    This function avoids the expensive re-initialization of EasyOCR models by
    using a lock to prevent race conditions in a multi-threaded environment.
    """
    cache_key = (tuple(sorted(lang_list)), gpu)

    # First check is optimistic and lock-free for performance.
    if cache_key not in _CACHED_GROUPERS:
        # If not found, acquire the lock before the second check.
        with _grouper_lock:
            # Check again inside the lock. Another thread might have created
            # the instance while we were waiting for the lock.
            if cache_key not in _CACHED_GROUPERS:
                # If it's still not there, we are the first and can safely create it.
                _CACHED_GROUPERS[cache_key] = TextGrouper(lang_list, gpu)
    
    return _CACHED_GROUPERS[cache_key]


# --- UPDATED EXAMPLE USAGE ---

if __name__ == "__main__":
    # NOTE: Please replace with valid paths to image files on your system.
    english_doc_path = "path/to/your/standard_text.png"
    manga_page_path = "path/to/your/manga_page.jpg"

    try:
        # --- Task 1: Process an English document ---
        print("--- Requesting English-only grouper ---")
        # Get the grouper. This will trigger initialization the first time.
        en_grouper = get_textgrouper(lang_list=['en'])
        
        print(f"\nProcessing file: {english_doc_path}")
        en_groups = en_grouper.group_text(
            image_path=english_doc_path,
            output_debug_path="debug_english_doc.jpg",
            start_direction=StartDirection.TOP_LEFT
        )
        for i, group in enumerate(en_groups, 1):
            print(f"  Group {i}: {[item['text'] for item in group]}")

        # --- Task 2: Process another English document ---
        print("\n--- Requesting English-only grouper AGAIN ---")
        # This call will be instantaneous as the instance is cached.
        # No "Initializing..." message will appear.
        en_grouper_again = get_textgrouper(lang_list=['en'])
        
        # Verify it's the same object in memory
        print(f"Is it the same instance? {en_grouper is en_grouper_again}")

        # --- Task 3: Process a Japanese manga page ---
        print("\n--- Requesting English/Japanese grouper ---")
        # This will trigger a *new* initialization because the language list is different.
        jp_grouper = get_textgrouper(lang_list=['en', 'ja'])

        print(f"\nProcessing file: {manga_page_path}")
        jp_groups = jp_grouper.group_text(
            image_path=manga_page_path,
            output_debug_path="debug_manga_page.jpg",
            start_direction=StartDirection.TOP_RIGHT
        )
        for i, group in enumerate(jp_groups, 1):
            print(f"  Group {i}: {[item['text'] for item in group]}")

    except FileNotFoundError as e:
        print(f"\nERROR: {e}")
        print("Please update the file paths in the __main__ block to valid image paths.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")