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

def get_champion_run():
    with db_conn() as conn:
        row = conn.execute(
            text("SELECT mlflow_run_id FROM models WHERE is_champion=TRUE ORDER BY id DESC LIMIT 1")
        ).fetchone()
        if row and row[0]:
            return row[0]

        row = conn.execute(
            text("SELECT mlflow_run_id FROM models ORDER BY id DESC LIMIT 1")
        ).fetchone()
        return row[0] if row else None

def mlflow_setup():
    if CFG.DAGSHUB_USERNAME and CFG.DAGSHUB_TOKEN and CFG.PUBLIC_REPO_NAME:
        os.environ["MLFLOW_TRACKING_USERNAME"] = CFG.DAGSHUB_USERNAME
        os.environ["MLFLOW_TRACKING_PASSWORD"] = CFG.DAGSHUB_TOKEN
        tracking_uri = f"https://dagshub.com/{CFG.DAGSHUB_USERNAME}/{CFG.PUBLIC_REPO_NAME}.mlflow"
        mlflow.set_tracking_uri(tracking_uri)

def main():
    run_id = get_champion_run()
    if not run_id:
        logger.warning("No champion found; skipping prediction")
        return
    mlflow_setup()
    # Try common artifact names; fall back to first artifact if needed
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    items = client.list_artifacts(run_id)
    if not items:
        raise RuntimeError(f"No artifacts found under run {run_id}")

    # Pick first directory artifact dynamically
    model_item = next((i for i in items if i.is_dir), None)
    if not model_item:
        raise RuntimeError(f"No model directory found under run {run_id}")

    loaded = mlflow.pyfunc.load_model(f"runs:/{run_id}/{model_item.path}")
    
    rows = []
    for var in CFG.VARIABLES:
        for h in CFG.HORIZONS_HOURS:
            Xy = build_features(var, h)
            if Xy is None or Xy.empty:
                continue
            feat_cols = [c for c in Xy.columns if c in ("open_meteo","met_no","openweather","visual_crossing","weather_gov") or c.startswith("obs_lag_") or c in ("hour","dow")]
            X = Xy[["lat","lon","valid_time"] + feat_cols].dropna()
            if X.empty: continue
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
