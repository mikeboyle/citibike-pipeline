import os
import requests
import zipfile
from typing import List
from pathlib import Path
import re

from citibike.utils.storage import StorageLocation

class TripDataDownloader:
    def __init__(self, storage: StorageLocation, base_url: str):
        self.storage = storage
        self.base_url = base_url
        self.csv_files_created = []  # Track CSV files for data ingestion and eventual cleanup
        self.zip_files_created = []  # Track zip files for cleanup

    def download_month(self, year: int, month: int) -> None:
        # Clear previous state
        self.csv_files_created.clear()
        self.zip_files_created.clear()

        # Construct YYYYMM prefix of file we want
        year_month_prefix = f"{year:04d}{month:02d}"

        # Construct filename and URL
        filename = f"{year_month_prefix}-citibike-tripdata.zip" if year >= 2024 else f"{year:04d}-citibike-tripdata.zip"
        url = f"{self.base_url}/{filename}"

        # Download and extract files
        zip_path = self.storage.get_temp_path(filename)
        self._download_file(url, zip_path)
        self.zip_files_created.append(zip_path)  # Track for cleanup

        # Extract desired CSV files only
        self._extract_csv_files(zip_path, year_month_prefix)
    
    def _download_file(self, url: str, dest_path: str) -> None:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
    
    def _extract_csv_files(self, zip_path: str, year_month_prefix: str) -> None:
        print(f"extracting from {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                filename = file_info.filename
                basename = os.path.basename(filename)
                extension = Path(basename).suffix

                # Case 1: We have to unzip YYYYMM-citibike-tripdata.zip first
                if basename.startswith(year_month_prefix) and extension == ".zip":
                    # TODO: unzip this archive and recursively extract csv files from it
                    extracted_path = self.storage.get_temp_path(basename)

                    with zip_ref.open(file_info) as source, open(extracted_path, 'wb') as target:
                        target.write(source.read())
                    
                    self.zip_files_created.append(extracted_path)  # Track nested zip for cleanup
                    next_zip_path = self.storage.get_temp_path(basename)
                    print(f"recursively extract from {next_zip_path}")
                    self._extract_csv_files(next_zip_path, year_month_prefix)
                    return
                
                # Case 2: We have found a CSV file to extract
                elif basename.startswith(year_month_prefix) and extension == ".csv":                    
                    # Only extract files that start with the desired YYYYMM prefix
                    # and end with a _N batch number (this ignores duplicate files with formatting issues)
                    file_stem = Path(basename).stem               
                    is_batch_file = bool(re.search(r"_\d", file_stem))

                    if is_batch_file:
                        # Extract to storage location
                        extracted_path = self.storage.get_temp_path(basename)

                        with zip_ref.open(file_info) as source, open(extracted_path, 'wb') as target:
                            target.write(source.read())
                        
                        self.csv_files_created.append(extracted_path)

    def get_all_files_for_cleanup(self) -> List[str]:
        """Return all files (CSV and ZIP) created during download/extraction for cleanup."""
        return self.csv_files_created + self.zip_files_created