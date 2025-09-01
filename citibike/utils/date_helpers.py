from datetime import datetime
import zoneinfo

DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"

def now_nyc_datetime_str() -> str:
    """
    Return the current datetime in America/New_York timezone
    formatted as a BigQuery DATETIME string (YYYY-MM-DD HH:MM:SS).
    """
    ny_tz = zoneinfo.ZoneInfo("America/New_York")
    return datetime.now(tz=ny_tz).strftime(DATETIME_STR_FORMAT)
