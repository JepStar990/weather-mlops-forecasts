from datetime import datetime, timezone
from dateutil import parser

def now_utc() -> datetime:
    """Return current UTC datetime (aware)."""
    return datetime.now(timezone.utc)

def to_utc(dt) -> datetime | None:
    """Coerce dt (str|datetime) to timezone-aware UTC datetime. Returns None if dt is None."""
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = parser.isoparse(dt)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def floor_hour(dt: datetime) -> datetime:
    dt = to_utc(dt)
    return dt.replace(minute=0, second=0, microsecond=0)

def horizon_hours(issue_time: datetime, valid_time: datetime) -> int:
    delta = to_utc(valid_time) - to_utc(issue_time)
    return int(round(delta.total_seconds() / 3600))
