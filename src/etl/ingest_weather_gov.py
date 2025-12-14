"""
US NWS weather.gov gridpoints (only for US locations).
Docs: https://www.weather.gov/documentation/services-web-api
Gridpoints: https://github.com/weather-gov/api/blob/master/gridpoints.md
"""
import pandas as pd
from src.config import CFG, WEATHER_GOV_POINTS_URL, WEATHER_GOV_GRID_URL
from src.utils.http_utils import get_json
from src.utils.time_utils import now_utc, to_utc, horizon_hours
from src.utils.unit_utils import normalize_value
from src.utils.db_utils import insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def is_us(lat: float, lon: float) -> bool:
    # Rough bounding box for continental US
    return 18 <= lat <= 72 and -170 <= lon <= -50

def fetch_weather_gov(lat: float, lon: float, variables: list[str]) -> pd.DataFrame:
    if not is_us(lat, lon):
        return pd.DataFrame()
    headers = {"User-Agent": CFG.NWS_USER_AGENT, "Accept": "application/geo+json"}
    meta = get_json(WEATHER_GOV_POINTS_URL.format(lat=lat, lon=lon), headers=headers)
    props = meta.get("properties", {})
    office = props.get("gridId")
    gridX = props.get("gridX")
    gridY = props.get("gridY")
    if not office:
        return pd.DataFrame()
    grid = get_json(WEATHER_GOV_GRID_URL.format(office=office, gridX=gridX, gridY=gridY), headers=headers, params={"units":"si"})
    issue = now_utc()
    rows = []

    def extract_series(field: str):
        f = grid.get("properties", {}).get(field, {})
        vals = f.get("values", []) or []
        unit = f.get("uom", "")
        return vals, unit

    temp_vals, _ = extract_series("temperature")
    wind_vals, _ = extract_series("windSpeed")
    qpf_vals, _ = extract_series("quantitativePrecipitation")  # mm over period if SI

    # Convert NWS "validTime" like "2025-12-13T18:00:00+00:00/PT1H" to (start, duration)
    def parse_valid(vt: str):
        t, dur = vt.split("/")
        return to_utc(t), dur

    # Build dict by start time to approximate hourly grid
    tmap = {}
    for v in temp_vals:
        t, _ = parse_valid(v["validTime"])
        tmap.setdefault(t, {})["temp_2m"] = float(v["value"]) if v["value"] is not None else None
    for v in wind_vals:
        t, _ = parse_valid(v["validTime"])
        tmap.setdefault(t, {})["wind_speed_10m"] = float(v["value"]) if v["value"] is not None else None
    for v in qpf_vals:
        t, dur = parse_valid(v["validTime"])
        val = float(v["value"]) if v["value"] is not None else 0.0
        # If duration is PT1H, take directly; if longer (e.g., PT6H), apportion evenly per hour
        hours = 1
        if dur.startswith("PT") and dur.endswith("H"):
            hours = int(dur[2:-1])
        per_hour = val / hours if hours > 0 else val
        for h in range(hours):
            tt = to_utc(t)  # assuming continuous blocks
            tmap.setdefault(tt, {})["precipitation"] = per_hour

    for vt, vals in tmap.items():
        for var in variables:
            if var not in vals or vals[var] is None:
                continue
            if var == "temp_2m":
                v, u = normalize_value("temp_2m", vals[var], "C")
            elif var == "wind_speed_10m":
                # NWS SI often returns km/h; normalize from km/h -> m/s if needed. Here assume m/s (conservative)
                try:
                    v, u = normalize_value("wind_speed_10m", vals[var], "m/s")
                except Exception:
                    v, u = normalize_value("wind_speed_10m", vals[var], "km/h")
            else:
                v, u = normalize_value("precipitation", vals[var], "mm")
            rows.append({
                "source": "weather_gov",
                "lat": lat, "lon": lon, "variable": var,
                "issue_time": issue, "valid_time": vt,
                "horizon_hours": horizon_hours(issue, vt),
                "value": v, "unit": u,
            })
    return pd.DataFrame(rows)

def main():
    frames = []
    for loc in CFG.TARGET_LOCATIONS:
        frames.append(fetch_weather_gov(loc["lat"], loc["lon"], CFG.VARIABLES))
    df = pd.concat(frames, ignore_index=True) if frames else pd.Data    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    insert_dataframe(df, "forecasts")

if __name__ == "__main__":
    main()
