import pandas as pd
from pandas.api.extensions import ExtensionDtype
from typing import Dict

from citibike.utils.date_helpers import DATETIME_STR_FORMAT, now_nyc_datetime



def validate_and_cast_trip_schema(df: pd.DataFrame, schema: Dict[str, ExtensionDtype]) -> pd.DataFrame:
    """
    Validate CSV DataFrame against expected schema and cast to correct types.

    Raises:
        ValueError: If columns are missing, unexpected, or can't be cast
    
    Returns:
        DataFrame with properly typed columns
    """
    expected_columns = set(schema.keys())
    actual_columns = set(df.columns)

    # 1. Check for missing columns
    missing_columns = expected_columns - actual_columns
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    # 2. Check for unexpected new columns
    extra_columns = actual_columns - expected_columns
    if extra_columns:
        raise ValueError(f"Unexpected columns found: {sorted(extra_columns)}")
    
    # 3. Attempt to cast each column to expected type
    df_typed = df.copy()

    for column, expected_type in schema.items():
        try:
            if expected_type == "datetime64[ns]":
                df_typed[column] = pd.to_datetime(df_typed[column])
            elif expected_type in ['int64', 'Int64', 'float64']:
                # Force numeric conversion first, then to integer
                df_typed[column] = pd.to_numeric(df_typed[column], errors="coerce").astype(expected_type)
            else:
                df_typed[column] = df_typed[column].astype(expected_type)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to cast column '{column}' to {expected_type}: {e}")
    
    return df_typed

def add_metadata_columns(df: pd.DataFrame, batch_key_value: str, batch_key_col: str = "_batch_key") -> pd.DataFrame:
    """
    Add metadata columns to a validated DataFrame

    Args:
        df: Validated DataFrame with correct schema
        batch_key_value: Batch identifier (e.g., "2024-01-01")
        batch_key_col: Name of the batch key column (default "_batch_key") 
    """
    df_with_metadata = df.copy()

    # Add ingestion timestamp (when our pipeline ingested this data)
    # Set this as NYC local time (with timezone info stripped, leaving wall clock time only)
    df_with_metadata["_ingested_at"] = pd.Timestamp.now(tz='America/New_York').tz_localize(None)

    # Add batch key (the time in history of the actual data)
    df_with_metadata[batch_key_col] = batch_key_value

    return df_with_metadata