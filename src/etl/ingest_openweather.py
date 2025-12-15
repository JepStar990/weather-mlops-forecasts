"""
OpenWeather One Call 3.0 (first 1,000 calls/day free).
Docs: https://openweathermap.org/price
"""
import pandas as pd
from src.config import CFG, OPENWEATHER_URL
from src.utils.http_utils import get_json
from src.utils.time_utils import now_utc, to_utc, horizon_hours
from src.utils.unit_utils import normalize_value
from src.utils.db_utils import insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def fetch_openweather(lat: float, lon: float, variables: list[str]) -> pd.DataFrame:
    if not CFG.OPENWEATHER_API_KEY:
        logger.warning("OPENWEATHER_API_KEY missing; skipping")
        return pd.DataFrame()
    params = {
        "lat": lat, "lon": lon,
        "appid": CFG.OPENWEATHER_API_KEY,
        "units": "metric",
        "exclude": "minutely,daily,alerts,current",
    }
    data = get_json(OPENWEATHER_URL, params=params)
    issue = now_utc()
    rows = []
    for h in data.get("hourly", []):
        vt = to_utc(h.get("dt"))
        for var in variables:
            if var == "temp_2m" and "temp" in h:
                v, u = normalize_value("temp_2m", float(h["temp"]), "C")
            elif var == "wind_speed_10m" and "wind_speed" in h:
                v, u = normalize_value("wind_speed_10m", float(h["wind_speed"]), "m/s")
            elif var == "precipitation":
                precip = 0.0
                if isinstance(h.get("rain"), dict) and "1h" in h["rain"]:
                    precip += float(h["rain"]["1h"])
                if isinstance(h.get("snow"), dict) and "1h" in h["snow"]:
                    precip += float(h["snow"]["1h"])
                v, u = normalize_value("precipitation", precip, "mm")
            else:
                continue
            rows.append({
                "source": "openweather",
                "lat": lat, "lon": lon, "variable": var,
                "issue_time": issue, "valid_time": vt,
                "horizon_hours": horizon_hours(issue, vt),
                "value": v, "unit": u,
            })
    return pd.DataFrame(rows)

def main():
    frames = []
    for loc in CFG.TARGET_LOCATIONS:
        frames.append(fetch_openweather(loc["lat"], loc["lon"], CFG.VARIABLES))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    insert_dataframe(df, "forecasts")

if __name__ == "__main__":
    main()
