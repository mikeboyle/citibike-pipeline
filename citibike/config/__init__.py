import os
from pathlib import Path
from typing import Any, Dict
from dotenv import dotenv_values, load_dotenv

def load_env_config(env_name: str = "dev", verbose: bool = False) -> None:
    """Load environment configuration from project root config directory into os.environ"""
    # Find project root from this module's location
    config_dir = Path(__file__).parent  # citibike/config
    project_root = config_dir.parent.parent  # project root
    config_file = project_root / "config" / f"{env_name}.env"
    
    # Load the .env file with optional verbose output
    success = load_dotenv(config_file, verbose=verbose)
    
    if verbose or os.environ.get('CITIBIKE_CONFIG_DEBUG', '').lower() == 'true':
        if success:
            print(f"✅ Loaded config from: {config_file}")
        else:
            print(f"⚠️  Config file not found or empty: {config_file}")

def get_config_value_dict(env_name: str = "dev") -> Dict[str, Any]:
    """Get configuration values as a dictionary from .env file (for setup scripts that need multiple environments)"""
    # Get the directory where this script lives (citibike/config)
    config_dir = Path(__file__).parent 
    # Navigate to project root, then to config
    config_path = config_dir.parent.parent / "config" / f"{env_name}.env"
    
    # Load the .env file and return as dict
    return dotenv_values(config_path)