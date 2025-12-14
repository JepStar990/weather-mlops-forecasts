"""
Visual Crossing Timeline API (free daily records cap).
Doc: https://www.visualcrossing.com/resources/blog/how-do-i-get-free-weather-api-access/
"""
import pandas as pd
from datetime import datetime, timedelta
from src.config import CFG, VISUAL_CROSSING_URL
from src.utils.http_utils import get_json
from src.utils.time_utils import now_utc, to_utc, horizon_hours
from src.utils.unit_utils import normalize_value
from src.utils.db_utils import insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def fetch_visual_crossing(lat: float, lon: float, variables: list[str]) -> pd.DataFrame:
    if not CFG.VISUAL_CROSSING_API_KEY:
        logger.warning("VISUAL_CROSSING_API_KEY missing; skipping")
        return pd.DataFrame()
    loc_str = f"{lat},{lon}"
    # Next 5 days hourly to respect free limits
    url = f"{VISUAL_CROSSING_URL}/{loc_str}"
    params = {
        "unitGroup": "metric",  # temp C, wind km/h, precip mm
        "include": "hours",
        "contentType": "json",
        "key": CFG.VISUAL_CROSSING_API_KEY,
        "elements": "datetime,temp,wspd,precip",
    }
    data = get_json(url, params=params)
    issue = now_utc()
    rows = []
    for day in data.get("days", []):
        for hr in day.get("hours", []):
            # VC returns local time; specify timezone offset available in 'tzoffset'? Use ISO form if present.
            vt = to_utc(hr.get("datetimeEpoch"))
            for var in variables:
                if var == "temp_2m" and "temp" in hr:
                    v, u = normalize_value("temp_2m", float(hr["temp"]), "C")
                elif var == "wind_speed_10m" and "wspd" in hr:
                    v, u = normalize_value("wind_speed_10m", float(hr["wspd"]), "km/h")
                elif var == "precipitation" and "precip" in hr:
                    v, u = normalize_value("precipitation", float(hr["precip"] or 0.0), "mm")
                else:
                    continue
                rows.append({
                    "source": "visual_crossing",
                                       "lat": lat, "lon": lon, "variable": var,
                    "issue_time": issue, "valid_time": vt,
                    "horizon_hours": horizon_hours(issue, vt),
                    "value": v, "unit": u,
                })
    return pd.DataFrame(rows)

def main():
    frames = []
    for loc in CFG.TARGET_LOCATIONS:
        frames.append(fetch_visual_crossing(loc["lat"], loc["lon"], CFG.VARIABLES))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    insert_dataframe(df, "forecasts")

if __name__ == "__main__":
    main()
