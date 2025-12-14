"""
Train per-variable, per-horizon models.
- Baseline: linear regression on vendor features (+lags)
- Ensemble: LightGBM if available; fallback to LinearRegression
- Log to DagsHub (MLflow)
"""
import os
import mlflow
from mlflow import sklearn as ml_sklearn
from sklearn.linear_model import LinearRegression
from lightgbm import LGBMRegressor
import pandas as pd
from src.config import CFG
from src.model.features import build_features
from src.model.evaluate import weekly_folds, evaluate_model
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def _setup_mlflow():
    if not (CFG.DAGSHUB_USERNAME and CFG.DAGSHUB_TOKEN and CFG.PUBLIC_REPO_NAME):
        logger.warning("DagsHub credentials missing; MLflow will use local filesystem.")
        return
    os.environ["MLFLOW_TRACKING_USERNAME"] = CFG.DAGSHUB_USERNAME
    os.environ["MLFLOW_TRACKING_PASSWORD"] = CFG.DAGSHUB_TOKEN
    tracking_uri = f"https://dagshub.com/{CFG.DAGSHUB_USERNAME}/{CFG.PUBLIC_REPO_NAME}.mlflow"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("weather-ensemble")

def train_one(variable: str, horizon: int):
    Xy = build_features(variable, horizon)
    if Xy is None or Xy.empty:
        logger.warning("No data for %s H+%d", variable, horizon)
        return None
    # features
    vendor_cols = [c for c in Xy.columns if c in ("open_meteo","met_no","openweather","visual_crossing","weather_gov")]
    lag_cols = [c for c in Xy.columns if c.startswith("obs_lag_")]
    feat = vendor_cols + lag_cols + ["hour","dow"]
    Xy = Xy.dropna(subset=feat + ["y"])
    if Xy.empty:
        logger.warning("After dropna, no rows for %s H+%d", variable, horizon)
        return None
    folds = weekly_folds(Xy)
    if not folds:
        logger.warning("No folds for %s H+%d", variable, horizon)
        return None

    _setup_mlflow()
    with mlflow.start_run(run_name=f"{variable}_H{horizon}"):
        # Baseline
        base = LinearRegression()
        tr = pd.concat([f[0] for f in folds], ignore_index=True)
        base.fit(tr[feat], tr["y"])

        try:
            ens = LGBMRegressor(n_estimators=300, learning_rate=0.05, max_depth=-1, subsample=0.8)
            ens.fit(tr[feat], tr["y"])
            model = ens
            algo = "lightgbm"
        except Exception:
            model = base
            algo = "linear"

        dfm, rmse, mae = evaluate_model(model, folds, feat)
        mlflow.log_params({"variable": variable, "horizon": horizon, "algo": algo})
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_artifact(local_path=dfm.to_csv(index=False), artifact_path="fold_metrics")  # quick artifact
        ml_sklearn.log_model(model, artifact_path="model")
        run_id = mlflow.active_run().info.run_id
        logger.info("Trained %s H+%d: RMSE=%.3f MAE=%.3f (run_id=%s)", variable, horizon, rmse, mae, run_id)
        return {"variable": variable, "horizon": horizon, "rmse": rmse, "mae": mae, "run_id": run_id, "features": feat}

defdef main():
    results = []
    for var in CFG.VARIABLES:
        for h in CFG.HORIZONS_HOURS:
            r = train_one(var, h)
            if r: results.append(r)
    if not results:
        logger.warning("No models trained")

if __name__ == "__main__":
    main()
