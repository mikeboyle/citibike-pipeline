import os
import requests
import zipfile
from typing import List

from .storage import StorageLocation

class TripDataDownloader:
    def __init__(self, storage: StorageLocation, base_url: str):
        self.storage = storage
        self.base_url = base_url

    def download_month(self, year: int, month: int) -> List[str]:
        if year < 2024:
            raise ValueError(f"Year {year} not supported yet (pre-2024 format)")
        
        # Construct URL: 202401-citibike-tripdata.zip
        filename = f"{year:04d}{month:02d}-citibike-tripdata.zip"
        url = self.base_url + filename
        
        # Download, extract, return CSV paths
        zip_path = self.storage.get_temp_path(filename)
        self._download_file(url, zip_path)

        # Extract CSV files only
        csv_paths = self._extract_csv_files(zip_path)

        return csv_paths
    
    def _download_file(self, url: str, dest_path: str) -> None:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
    
    def _extract_csv_files(self, zip_path: str) -> List[str]:
        csv_paths = []
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                filename = file_info.filename
                if filename.endswith('.csv') and not file_info.is_dir():
                    # Extract with just the basename to flatten structure
                    basename = os.path.basename(filename)
                    
                    # Extract to storage location
                    extracted_path = self.storage.get_temp_path(basename)

                    with zip_ref.open(file_info) as source, open(extracted_path, 'wb') as target:
                        target.write(source.read())
                    
                    csv_paths.append(extracted_path)
        
        return csv_paths