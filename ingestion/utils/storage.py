from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

class StorageLocation(ABC):
    @abstractmethod
    def get_temp_path(self, filename: str) -> str:
        """Return a temporary file path for the given filename"""
        pass
    
    @abstractmethod
    def cleanup(self, paths: List[str]) -> None:
        """Clean up the file at the given path"""
        pass
    
    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if file exists at the given path"""
        pass

class LocalStorage(StorageLocation):
    def __init__(self, base_dir: str = "/tmp/citibike") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def get_temp_path(self, filename: str) -> str:
        return str(self.base_dir / filename)
    
    def cleanup(self, paths: List[str]) -> None:
        for path in paths:
            path_obj = Path(path)
            if path_obj.exists():
                path_obj.unlink()
    
    def exists(self, path: str) -> bool:
        return Path(path).exists()