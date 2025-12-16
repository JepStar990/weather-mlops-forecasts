"""
Use champion model to generate forecasts as source='our_model'.
For simplicity, use the latest run as champion fallback.
"""
import os
import mlflow
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timezone
from src.config import CFG
from src.model.features import build_features
from src.utils.db_utils import db_conn, insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def get_champion_model_name():
    with db_conn() as conn:
        row = conn.execute(
            text("SELECT name FROM models WHERE is_champion=TRUE ORDER BY id DESC LIMIT 1")
        ).fetchone()
        if row and row[0]:
            return row[0]

        # Fallback: latest model
        row = conn.execute(
            text("SELECT name FROM models ORDER BY id DESC LIMIT 1")
        ).fetchone()
        return row[0] if row else None

def mlflow_setup():
    if CFG.DAGSHUB_USERNAME and CFG.DAGSHUB_TOKEN and CFG.PUBLIC_REPO_NAME:
        os.environ["MLFLOW_TRACKING_USERNAME"] = CFG.DAGSHUB_USERNAME
        os.environ["MLFLOW_TRACKING_PASSWORD"] = CFG.DAGSHUB_TOKEN
        tracking_uri = f"https://dagshub.com/{CFG.DAGSHUB_USERNAME}/{CFG.PUBLIC_REPO_NAME}.mlflow"
        mlflow.set_tracking_uri(tracking_uri)

import re
def _sort_lag_cols(cols):
    def lag_key(c):
        m = re.search(r"^obs_lag_(\d+)h?$", c)
        return int(m.group(1)) if m else 10**9
    return sorted(cols, key=lag_key)

def main():
    run_id = get_champion_model_name()
    if not run_id:
        logger.warning("No champion found; skipping prediction")
        return
        
    mlflow_setup()
    champion_name = get_champion_model_name()
    if not champion_name:
        logger.warning("No champion model found; skipping prediction")
        return

    model = mlflow.pyfunc.load_model(f"models:/{champion_name}/Production")
    logger.info(f"Loaded champion model: {champion_name}")
    
    rows = []
    for var in CFG.VARIABLES:
        for h in CFG.HORIZONS_HOURS:
            Xy = build_features(var, h)
            if Xy is None or Xy.empty:
                continue

            vendor_cols = [c for c in ("open_meteo","met_no","openweather","visual_crossing","weather_gov") if c in Xy.columns]
            lag_cols = _sort_lag_cols([c for c in Xy.columns if c.startswith("obs_lag_")])
            feat_cols = vendor_cols + lag_cols + ["hour", "dow"]

            # Rebuild calendar features if missing
            if "hour" not in Xy.columns or Xy["hour"].isna().any():
                Xy["hour"] = pd.to_datetime(Xy["valid_time"]).dt.hour
            if "dow" not in Xy.columns or Xy["dow"].isna().any():
                Xy["dow"] = pd.to_datetime(Xy["valid_time"]).dt.dayofweek

            # Keep rows that have at least one vendor signal (imputer will handle lag NaNs)
            if not vendor_cols:
                continue
            mask_has_vendor = pd.notna(Xy[vendor_cols]).any(axis=1)
            X = Xy.loc[mask_has_vendor, ["lat","lon","valid_time"] + feat_cols]
            if X.empty: 
                continue
            
            yhat = model.predict(X[feat_cols])
            for (lat, lon, vt), v in zip(X[["lat","lon","valid_time"]].itertuples(index=False, name=None), yhat):
                rows.append({
                    "source": "our_model",
                    "lat": lat, "lon": lon, "variable": var,
                    "issue_time": datetime.now(timezone.utc),
                    "valid_time": vt,
                    "horizon_hours": h,
                    "value": float(v), "unit": {"temp_2m":"C","wind_speed_10m":"m/s","precipitation":"mm"}[var],
                })
    df = pd.DataFrame(rows)
    insert_dataframe(df, "forecasts")

if __name__ == "__main__":
    main()
