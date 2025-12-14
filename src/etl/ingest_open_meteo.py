"""
Open-Meteo hourly forecasts (no key). Variables: temperature_2m (C), wind_speed_10m (m/s), precipitation (mm).
Docs: https://open-meteo.com/
"""
import pandas as pd
from datetime import datetime
from src.config import CFG, OPEN_METEO_URL
from src.utils.http_utils import get_json
from src.utils.time_utils import now_utc, to_utc, horizon_hours
from src.utils.unit_utils import normalize_value
from src.utils.db_utils import insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def fetch_open_meteo(lat: float, lon: float, variables: list[str]) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "temperature_2m" if "temp_2m" in variables else "",
            "wind_speed_10m" if "wind_speed_10m" in variables else "",
            "precipitation" if "precipitation" in variables else "",
        ]).strip(","),
        "windspeed_unit": "ms",
        "precipitation_unit": "mm",
        "timezone": "UTC",
    }
    data = get_json(OPEN_METEO_URL, params=params)
    issue = now_utc()
    hourly = data.get("hourly", {})
    times = [to_utc(t) for t in hourly.get("time", [])]
    rows = []

    for i, vt in enumerate(times):
        for var in variables:
            if var == "temp_2m" and "temperature_2m" in hourly:
                value = hourly["temperature_2m"][i]
                v, u = normalize_value("temp_2m", float(value), "C")
            elif var == "wind_speed_10m" and "wind_speed_10m" in hourly:
                value = hourly["wind_speed_10m"][i]
                v, u = normalize_value("wind_speed_10m", float(value), "m/s")
            elif var == "precipitation" and "precipitation" in hourly:
                value = hourly["precipitation"][i]
                v, u = normalize_value("precipitation", float(value), "mm")
            else:
                continue
            rows.append({
                "source": "open_meteo",
                "lat": lat, "lon": lon,
                "variable": var,
                "issue_time": issue,
                "valid_time": vt,
                "horizon_hours": horizon_hours(issue, vt),
                "value": v, "unit": u,
            })
    return pd.DataFrame(rows)

def main():
    frames = []
    for loc in CFG.TARGET_LOCATIONS:
        logger.info("Fetching Open-Meteo for %s", loc["name"])
        frames.append(fetch_open_meteo(loc["lat"], loc["lon"], CFG.VARIABLES))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    insert_dataframe(df, "forecasts")

if __name__ == "__main__":
    main()
