"""
Join forecasts with observations by (lat, lon, variable, valid_time == obs_time).
Compute MAE, RMSE, MAPE per source/horizon/variable per valid hour.
"""
import pandas as pd
from sqlalchemy import text
from src.utils.db_utils import fetch_df, insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

SQL_JOIN = """
WITH f AS (
  SELECT source, lat, lon, variable, valid_time, horizon_hours, value AS f_value
  FROM forecasts
),
o AS (
  SELECT lat, lon, variable, obs_time AS valid_time, value AS o_value
  FROM observations
)
SELECT f.source, f.variable, f.valid_time, f.horizon_hours, f.f_value, o.o_value
FROM f JOIN o
ON f.lat=o.lat AND f.lon=o.lon AND f.variable=o.variable AND f.valid_time=o.valid_time
"""

def compute():
    df = fetch_df(SQL_JOIN)
    if df.empty:
        logger.info("No forecast-observation pairs yet")
        return pd.DataFrame()
    grp = df.groupby(["source","variable","valid_time","horizon_hours"])
    out = grp.apply(lambda g: pd.Series({
        "mae": (g["f_value"] - g["o_value"]).abs().mean(),
        "rmse": ((g["f_value"] - g["o_value"])**2).mean() ** 0.5,
        "mape": ( (g["f_value"] - g["o_value"]).abs() / (g["o_value"].abs() + 1e-6) ).mean(),
        "n": len(g),
    })).reset_index()
    return out

def main():
    df = compute()
    if not df.empty:
               insert_dataframe(df, "errors")

if __name__ == "__main__":
    main()
