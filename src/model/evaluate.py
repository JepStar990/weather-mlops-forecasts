"""
Rolling-origin evaluation: split by weeks, simulate train/validate.
"""
import pandas as pd
import numpy as np
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


import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error

def evaluate_model(model, folds, features, target="y"):
    metrics = []
    y_all, p_all = [], []

    for i, (tr, va) in enumerate(folds, 1):
        Xv = va[features]               # keep DataFrame to preserve feature names
        yv = va[target].to_numpy()
        pred = model.predict(Xv)

        mae = mean_absolute_error(yv, pred)
        rmse = mean_squared_error(yv, pred) ** 0.5

        metrics.append({"fold": i, "mae": mae, "rmse": rmse, "n": len(va)})
        y_all.append(yv)
        p_all.append(pred)

    dfm = pd.DataFrame(metrics)

    # Overall metrics across all validation rows
    y_all = np.concatenate(y_all) if y_all else np.array([])
    p_all = np.concatenate(p_all) if p_all else np.array([])

    rmse_overall = mean_squared_error(y_all, p_all) ** 0.5 if y_all.size else float("nan")
    mae_overall  = mean_absolute_error(y_all, p_all)       if y_all.size else float("nan")
    return dfm, rmse_overall, mae_overall
