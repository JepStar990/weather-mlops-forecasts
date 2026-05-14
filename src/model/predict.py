"""
Use champion model to generate forecasts as source='our_model'.
For simplicity, use the latest run as champion fallback.
"""

import os
import gc
import re
import warnings
import mlflow
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timezone

from sklearn import set_config
set_config(transform_output="pandas")  # keep sklearn transformer outputs as DataFrames

from src.config import CFG
from src.model.features import build_features
from src.utils.db_utils import db_conn, insert_dataframe
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# MLflow warns on environment mismatches and extra inputs — benign since
# CI requirements are compatible and vendor columns may be missing at train time.
warnings.filterwarnings("ignore", message="Detected one or more mismatches")
warnings.filterwarnings("ignore", message="Found extra inputs")


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


def _sort_lag_cols(cols):
    def lag_key(c):
        m = re.search(r"^obs_lag_(\d+)h?$", c)
        return int(m.group(1)) if m else 10**9
    return sorted(cols, key=lag_key)


# Stream predictions in batches to avoid large in-memory accumulation
BATCH_SIZE = 50_000


def _predict_and_insert_stream(model, model_feat_cols, Xy: pd.DataFrame, var: str, h: int):
    all_vendors = ("open_meteo", "met_no", "openweather", "visual_crossing", "weather_gov")

    for vendor in all_vendors:
        if vendor not in Xy.columns:
            Xy[vendor] = float('nan')

    vendor_cols = list(all_vendors)
    lag_cols = _sort_lag_cols([c for c in Xy.columns if c.startswith("obs_lag_")])
    feat_cols = vendor_cols + lag_cols + ["hour", "dow"]

    # Ensure calendar features exist
    if "hour" not in Xy.columns or Xy["hour"].isna().any():
        Xy["hour"] = pd.to_datetime(Xy["valid_time"]).dt.hour
    if "dow" not in Xy.columns or Xy["dow"].isna().any():
        Xy["dow"] = pd.to_datetime(Xy["valid_time"]).dt.dayofweek

    # Keep rows with at least one vendor signal
    mask_has_vendor = pd.notna(Xy[vendor_cols]).any(axis=1)
    X = Xy.loc[mask_has_vendor, ["lat", "lon", "valid_time"] + feat_cols]
    if X.empty:
        return

    # Only pass columns the model was trained on (from its signature),
    # falling back to all feat_cols if the model has no signature.
    if model_feat_cols is not None:
        pred_cols = [c for c in model_feat_cols if c in X.columns]
    else:
        pred_cols = feat_cols

    n = len(X)
    for start in range(0, n, BATCH_SIZE):
        end = min(start + BATCH_SIZE, n)
        Xb = X.iloc[start:end]
        yhat = model.predict(Xb[pred_cols])

        out = pd.DataFrame({
            "source": "our_model",
            "lat": Xb["lat"].values,
            "lon": Xb["lon"].values,
            "variable": var,
            "issue_time": datetime.now(timezone.utc),
            "valid_time": Xb["valid_time"].values,
            "horizon_hours": h,
            "value": yhat.astype(float),
            "unit": {"temp_2m": "C", "wind_speed_10m": "m/s", "precipitation": "mm"}[var],
        })

        insert_dataframe(out, "forecasts")
        del Xb, yhat, out
        gc.collect()


def main():
    champion_name = get_champion_model_name()
    if not champion_name:
        logger.warning("No champion model found; skipping prediction")
        return

    mlflow_setup()
    model = mlflow.pyfunc.load_model(f"models:/{champion_name}/Production")
    logger.info("Loaded champion model: %s", champion_name)

    # Read the model's expected input columns from its signature
    model_feat_cols = []
    if model.metadata.signature and model.metadata.signature.inputs:
        model_feat_cols = [c.name for c in model.metadata.signature.inputs.columns]
    if not model_feat_cols:
        # Fallback for models without a signature: use all vendor + lag + calendar cols
        model_feat_cols = None

    for var in CFG.VARIABLES:
        for h in CFG.HORIZONS_HOURS:
            Xy = build_features(var, h)
            if Xy is None or Xy.empty:
                continue
            _predict_and_insert_stream(model, model_feat_cols, Xy, var, h)


if __name__ == "__main__":
    main()
