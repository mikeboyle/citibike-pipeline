from utils.storage import LocalStorage, StorageLocation
from utils.trips import TripDataDownloader

def test_download():
    storage = LocalStorage()
    downloader = TripDataDownloader(storage)

    # Test with a recent month
    year = 2024
    month = 1

    try:
        csv_files = downloader.download_month(year, month)
        print(f"Downloaded {len(csv_files)} files:")
        for file in csv_files:
            print(f"\t{file}")
    
    except Exception as e:
        print(f"Download failed: {e}")


if __name__ == "__main__":
    test_download()