from datetime import datetime
import zoneinfo

DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"

def now_nyc_datetime() -> datetime:
    """
    Return the current datetime, converted to America/New_York time
    but with the timezone info removed (so it is a timezone naive datetime
    which BigQuery may need to infer it is a datetime and not a utc timestamp).
    """
    ny_tz = zoneinfo.ZoneInfo("America/New_York")
    return datetime.now(tz=ny_tz).replace(tzinfo=None)
