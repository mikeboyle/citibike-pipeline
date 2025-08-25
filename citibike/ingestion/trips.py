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

    def download_month(self, year: int, month: int) -> List[str]:
        # Construct YYYYMM prefix of file we want
        year_month_prefix = f"{year:04d}{month:02d}"

        # Construct filename and URL
        filename = f"{year_month_prefix}-citibike-tripdata.zip" if year >= 2024 else f"{year:04d}-citibike-tripdata.zip"
        url = f"{self.base_url}/{filename}"

        # Download, extract, return CSV paths
        zip_path = self.storage.get_temp_path(filename)
        self._download_file(url, zip_path)

        # Extract desired CSV files only
        csv_paths = self._extract_csv_files(zip_path, year_month_prefix)

        return csv_paths
    
    def _download_file(self, url: str, dest_path: str) -> None:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
    
    def _extract_csv_files(self, zip_path: str, year_month_prefix: str) -> List[str]:
        print(f"extracting from {zip_path}")
        csv_paths = []
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
                    
                    next_zip_path = self.storage.get_temp_path(basename)
                    print(f"recursively extract from {next_zip_path}")
                    return self._extract_csv_files(next_zip_path, year_month_prefix)
                
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
                        
                        csv_paths.append(extracted_path)
        
        return csv_paths