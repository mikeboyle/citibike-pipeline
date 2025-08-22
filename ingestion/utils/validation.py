import pandas as pd
from pandas.api.extensions import ExtensionDtype
from typing import Dict


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
            else:
                df_typed[column] = df_typed[column].astype(expected_type)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to cast column '{column}' to {expected_type}: {e}")
    
    return df_typed