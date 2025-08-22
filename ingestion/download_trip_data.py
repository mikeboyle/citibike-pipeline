import pandas as pd
from utils.validation import validate_and_cast_trip_schema
from utils.schemas import CURRENT_TRIP_CSV_SCHEMA
from utils.storage import LocalStorage
from utils.trips import TripDataDownloader

def test_download():
    # storage = LocalStorage()
    # downloader = TripDataDownloader(storage)

    # # Test with a recent month
    # year = 2024
    # month = 1

    # try:
    #     csv_files = downloader.download_month(year, month)
    #     print(f"Downloaded {len(csv_files)} files:")
    #     for file in csv_files:
    #         print(f"\t{file}")
    
    # except Exception as e:
    #     print(f"Download failed: {e}")
    storage = LocalStorage()

    csv_path = storage.get_temp_path("202401-citibike-tripdata_2.csv")

    if not storage.exists(csv_path):
        print(f"CSV file not found at {csv_path}")
        print("Run download_trip_data.py first to get test files")
        return
    
    print(f"Loading CSV: {csv_path}")

    # Load the CSV
    df_raw = pd.read_csv(csv_path)
    
    print(f"Raw DataFrame shape: {df_raw.shape}")
    print(f"Raw columns: {list(df_raw.columns)}")
    print(f"Raw dtypes:\n{df_raw.dtypes}")
    print("\nFirst few rows:")
    print(df_raw.head(2))
    
    # Test validation
    try:
        df_validated = validate_and_cast_trip_schema(df_raw, CURRENT_TRIP_CSV_SCHEMA)
        print(f"\n✅ Validation successful!")
        print(f"Validated dtypes:\n{df_validated.dtypes}")
        
    except ValueError as e:
        print(f"\n❌ Validation failed: {e}")


if __name__ == "__main__":
    test_download()