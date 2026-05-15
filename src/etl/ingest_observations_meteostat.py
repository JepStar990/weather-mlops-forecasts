"""
Meteostat hourly observations via Point(lat, lon) for each configured location.
Docs: https://pypi.org/project/meteostat/
"""

import warnings
import pandas as pd
from datetime import datetime, timedelta, timezone
from meteostat import Point, Hourly

# Suppress FutureWarning from meteostat's internal use of deprecated
# pandas parse_dates nested-sequence syntax.
warnings.filterwarnings(
    "ignore",
    message="Support for nested sequences for 'parse_dates'",
    category=FutureWarning,
)

from src.config import CFG
from src.utils.db_utils import insert_dataframe_dedup
from src.utils.logging_utils import get_logger
from src.utils.unit_utils import normalize_value

logger = get_logger(__name__)


def fetch_obs(lat: float, lon: float) -> pd.DataFrame:
    """
    Fetch the last 24 hours of hourly observations near the given lat/lon.
    Meteostat expects naive datetimes for start/end. We'll convert to UTC after fetch.
    """
    # Use naive datetimes (no tzinfo) as required by meteostat
    end = datetime.utcnow().replace(minute=0, second=0, microsecond=0)   # naive
    start = end - timedelta(days=1)                                      # naive

    p = Point(lat, lon)

    try:
        # Do NOT pass timezone="UTC" here; meteostat will return times (naive/with tz)
        df = Hourly(p, start, end).fetch()
    except Exception as e:
        logger.warning("Hourly fetch failed near %.3f,%.3f: %s", lat, lon, e)
        return pd.DataFrame()

    if df is None or df.empty:
        logger.info("No hourly observations returned near %.3f,%.3f in the last 7 days", lat, lon)
        return pd.DataFrame()

    rows = []
    # Standardize timestamps to UTC-aware datetimes for DB consistency
    # If the index is naive, we set tzinfo=UTC; if tz-aware, convert to UTC.
    for ts, row in df.iterrows():
        # ts may be a pandas Timestamp; handle both naive and tz-aware
        ts_py = ts.to_pydatetime()
        if ts_py.tzinfo is None:
            obs_time = ts_py.replace(tzinfo=timezone.utc)
        else:
            obs_time = ts_py.astimezone(timezone.utc)

        # Temperature (°C) → temp_2m
        if "temp_2m" in CFG.VARIABLES and "temp" in row and pd.notna(row["temp"]):
            v, u = normalize_value("temp_2m", float(row["temp"]), "C")
            rows.append({
                "station_id": None,  # unknown/nearest selected by Meteostat Point
                "lat": lat, "lon": lon,
                "variable": "temp_2m",
                "obs_time": obs_time,
                "value": v, "unit": u,
                "source": "meteostat",
            })

        # Wind speed (typically km/h from Meteostat) → wind_speed_10m (normalized to m/s)
        if "wind_speed_10m" in CFG.VARIABLES and "wspd" in row and pd.notna(row["wspd"]):
            v, u = normalize_value("wind_speed_10m", float(row["wspd"]), "km/h")
            rows.append({
                "station_id": None,
                "lat": lat, "lon": lon,
                "variable": "wind_speed_10m",
                "obs_time": obs_time,
                "value": v, "unit": u,
                "source": "meteostat",
            })

        # Precipitation (mm) → precipitation
        if "precipitation" in CFG.VARIABLES and "prcp" in row and pd.notna(row["prcp"]):
            v, u = normalize_value("precipitation", float(row["prcp"] or 0.0), "mm")
            rows.append({
                "station_id": None,
                "lat": lat, "lon": lon,
                "variable": "precipitation",
                "obs_time": obs_time,
                "value": v, "unit": u,
                "source": "meteostat",
            })

    return pd.DataFrame(rows)


def main():
    frames = []
    for loc in CFG.TARGET_LOCATIONS:
        logger.info("Meteostat observations: %s", loc["name"])
        frames.append(fetch_obs(loc["lat"], loc["lon"]))

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    inserted = insert_dataframe_dedup(df, "observations", ["lat", "lon", "variable", "obs_time", "source"])
    logger.info("Inserted %d observation rows", inserted)


if __name__ == "__main__":
    main()
