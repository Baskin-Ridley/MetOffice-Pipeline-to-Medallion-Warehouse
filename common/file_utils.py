from pathlib import Path
from typing import Tuple

def get_latest_version_paths(base_dir: str, target_dir: str) -> Tuple[str, str, str]:
    """
    Identifies the most recent subfolder and prepares paths for Spark.
    """
    base_path = Path(base_dir)
    target_path = Path(target_dir)

    # Get sorted directories
    folders = sorted([f for f in base_path.iterdir() if f.is_dir()])
    
    if not folders:
        raise FileNotFoundError(f"No subfolders found in {base_dir}")

    latest_folder = folders[-1]
    
    # Return as strings for Spark compatibility
    landed_pattern = str(latest_folder / "*.json")
    version_id = latest_folder.name
    output_path = str(target_path / version_id)

    return landed_pattern, version_id, output_path
