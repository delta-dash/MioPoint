def _get_table_names(tag_type: str) -> dict:
    """A helper to generate the dynamic table names."""
    if tag_type not in ["tags", "meta_tags"]:
        raise ValueError("tag_type must be either 'tags' or 'meta_tags'")
        
    base_name = tag_type.replace('s', '')
    return {
        "main": tag_type,
        "relationships": f"{base_name}_relationships",
        "file_junction": f"file_content_{tag_type}",
        "scene_junction": f"scene_{tag_type}",
    }
