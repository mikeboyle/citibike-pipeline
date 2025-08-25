from pathlib import Path
from typing import Any, Dict
from dotenv import dotenv_values

def load_config(env_name: str = "dev") -> Dict[str, Any]:
    # Get the directory where this script lives (ingestion/utils)
    config_dir = Path(__file__).parent # citibike/config
    # Navigate to project root, then to config
    config_path = config_dir.parent.parent / "config" / f"{env_name}.env"
    
    # Load the .env file
    return dotenv_values(config_path)