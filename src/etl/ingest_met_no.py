"""
MET Norway Locationforecast 2.0 (no key; requires User-Agent).
Docs: https://api.met.no/weatherapi/locationforecast/2.0/documentation
"""
import pandas as pd
from src.config import CFG, MET_NO_URL
from src.utils.http_utils import get_json
from src.utils.time_utils import now_utc, to_utc, horizon_hours
from src.utils.unit_utils import normalize_value
from src.utils.db_utils import insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def fetch_met_no(lat: float, lon: float, variables: list[str]) -> pd.DataFrame:
    headers = {"User-Agent": CFG.MET_NO_USER_AGENT}
    data = get_json(MET_NO_URL, params={"lat": lat, "lon": lon}, headers=headers)
    issue = now_utc()
    timeseries = data.get("properties", {}).get("timeseries", [])
    rows = []
    for ts in timeseries:
        vt = to_utc(ts["time"])
        d = ts.get("data", {})
        inst = d.get("instant", {}).get("details", {})
        next1 = d.get("next_1_hours", {})
        precip = None
        if next1:
            p = next1.get("details", {}).get("precipitation_amount")
            if p is not None:
                precip = float(p)

        for var in variables:
            if var == "temp_2m" and "air_temperature" in inst:
                v, u = normalize_value("temp_2m", float(inst["air_temperature"]), "C")
            elif var == "wind_speed_10m" and "wind_speed" in inst:
                v, u = normalize_value("wind_speed_10m", float(inst["wind_speed"]), "m/s")
            elif var == "precipitation" and precip is not None:
                v, u = normalize_value("precipitation", precip, "mm")
            else:
                continue
            rows.append({
                "source": "met_no",
                "lat": lat, "lon": lon,
                "variable": var, "issue_time": issue, "valid_time": vt,
                "horizon_hours": horizon                "horizon_hours": horizon_hours(issue, vt), "value": v, "unit": u
            })
    return pd.DataFrame(rows)

def main():
    frames = []
    for loc in CFG.TARGET_LOCATIONS:
        frames.append(fetch_met_no(loc["lat"], loc["lon"], CFG.VARIABLES))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    insert_dataframe(df, "forecasts")

if __name__ == "__main__":
    main()
