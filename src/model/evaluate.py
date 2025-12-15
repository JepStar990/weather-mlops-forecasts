"""
Rolling-origin evaluation: split by weeks, simulate train/validate.
"""
import pandas as pd
from datetime import timedelta
from sklearn.metrics import mean_absolute_error, mean_squared_error

def weekly_folds(df: pd.DataFrame, time_col="valid_time", weeks_back=6):
    df = df.sort_values(time_col)
    if df.empty:
        return []
    min_t, max_t = df[time_col].min(), df[time_col].max()
    step = pd.Timedelta(weeks=1)
    folds = []
    start = min_t
    while start + step < max_t and len(folds) < weeks_back:
        train_end = start + step
        valid_end = train_end + step
        tr = df[(df[time_col] < train_end)]
        va = df[(df[time_col] >= train_end) & (df[time_col] < valid_end)]
        if not tr.empty and not va.empty:
            folds.append((tr, va))
        start += step
    return folds

def evaluate_model(model, folds, features, target="y"):
    metrics = []
    for i, (tr, va) in enumerate(folds, 1):
        Xv = va[features]
        yv = va[target]
        pred = model.predict(Xv)
        mae = mean_absolute_error(yv, pred)
        rmse = mean_squared_error(yv, pred) ** 0.5
        metrics.append({"fold": i, "mae": mae, "rmse": rmse, "n": len(va)})
    dfm = pd.DataFrame(metrics)
    return dfm, rmse, mae
