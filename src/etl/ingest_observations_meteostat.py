"""
Meteostat hourly observations for nearest station per location.
Docs: https://pypi.org/project/meteostat/
"""
import pandas as pd
from meteostat import Point, Hourly, Stations
from datetime import datetime, timedelta, timezone
from src.config import CFG
from src.utils.db_utils import insert_dataframe
from src.utils.logging_utils import get_logger
from src.utils.unit_utils import normalize_value

logger = get_logger(__name__)

def nearest_station(lat: float, lon: float):
    stations = Stations().nearby(lat, lon).inventory(hours=True)
    station = stations.fetch(1)
    if station.empty:
        return None, None
    row = station.iloc[0]
    return row["id"], (float(row["latitude"]), float(row["longitude"]))

def fetch_obs(lat: float, lon: float) -> pd.DataFrame:
    station_id, coord = nearest_station(lat, lon)
    if not station_id:
        logger.warning("No station near %.3f,%.3f", lat, lon)
        return pd.DataFrame()

    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=7)  # 7 days rolling
    p = Point(lat, lon)
    df = Hourly(p, start, end, timezone="UTC").fetch()  # columns: temp, dwpt, prcp, wdir, wspd, etc.

    rows = []
    for ts, row in df.iterrows():
        ts = ts.to_pydatetime().replace(tzinfo=timezone.utc)
        if "temp_2m" in CFG.VARIABLES and "temp" in row:
            v, u = normalize_value("temp_2m", float(row["temp"]), "C")
            rows.append({"station_id": station_id, "lat": lat, "lon": lon,
                         "variable": "temp_2m", "obs_time": ts, "value": v, "unit": u, "source": "meteostat"})
        if "wind_speed_10m" in CFG.VARIABLES and "wspd" in row:
            v, u = normalize_value("wind_speed_10m", float(row["wspd"]), "km/h")  # meteostat wspd = km/h
            rows.append({"station_id": station_id, "lat": lat, "lon": lon,
                         "variable": "wind_speed_10m", "obs_time": ts, "value": v, "unit": u, "source": "meteostat"})
        if "precipitation" in CFG.VARIABLES and "prcp" in row:
            v, u = normalize_value("precipitation", float(row["prcp"] or 0.0), "mm")
            rows.append({"station_id": station_id, "lat": lat, "lon": lon,
                         "variable": "precipitation", "obs_time": ts, "value": v, "unit": u, "source": "meteostat"})
    return pd.DataFrame(rows)

def main():
       frames = []
    for loc in CFG.TARGET_LOCATIONS:
        frames.append(fetch_obs(loc["lat"], loc["lon"]))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    insert_dataframe(df, "observations")

if __name__ == "__main__":
    main()
