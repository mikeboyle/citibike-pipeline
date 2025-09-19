import os
from pathlib import Path
from typing import Any, Dict
from dotenv import dotenv_values, load_dotenv

def load_env_config(env_name: str = "dev", verbose: bool = True) -> None:
    """Load environment configuration from project root config directory into os.environ"""
    # Find project root from this module's location
    config_dir = Path(__file__).parent  # citibike/config
    project_root = config_dir.parent.parent  # project root
    config_file = project_root / "config" / f"{env_name}.env"
    
    # Load the .env file with optional verbose output
    success = load_dotenv(config_file, verbose=verbose)
    
    # Resolve relative credentials path to absolute path from project root
    if success and 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
        credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        if credentials_path and not os.path.isabs(credentials_path):
            absolute_path = str(project_root / credentials_path)
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = absolute_path
            if verbose or os.environ.get('CITIBIKE_CONFIG_DEBUG', '').lower() == 'true':
                print(f"🔧 Resolved credentials path: {credentials_path} -> {absolute_path}")
