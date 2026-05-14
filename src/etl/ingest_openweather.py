"""
OpenWeather 2.5 Forecast (free tier).
Docs: https://openweathermap.org/forecast5
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
    }
    data = get_json(OPENWEATHER_URL, params=params)
    issue = now_utc()
    rows = []
    for entry in data.get("list", []):
        vt = to_utc(entry.get("dt"))
        if vt is None:
            continue
        for var in variables:
            if var == "temp_2m" and "main" in entry and "temp" in entry["main"]:
                v, u = normalize_value("temp_2m", float(entry["main"]["temp"]), "C")
            elif var == "wind_speed_10m" and "wind" in entry and "speed" in entry["wind"]:
                v, u = normalize_value("wind_speed_10m", float(entry["wind"]["speed"]), "m/s")
            elif var == "precipitation":
                precip = 0.0
                if isinstance(entry.get("rain"), dict) and "3h" in entry["rain"]:
                    precip += float(entry["rain"]["3h"])
                if isinstance(entry.get("snow"), dict) and "3h" in entry["snow"]:
                    precip += float(entry["snow"]["3h"])
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
    if not df.empty:
        df = df[df["horizon_hours"].isin(CFG.HORIZONS_HOURS)]
    insert_dataframe(df, "forecasts")


if __name__ == "__main__":
    main()
