# src/verify/leaderboard.py

import pandas as pd
from sqlalchemy import text
from src.utils.db_utils import fetch_df
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def leaderboard(days: int = 7) -> pd.DataFrame:
    """
    Return a leaderboard of the best-performing sources per variable & horizon
    over the last `days` days, based on RMSE (lower is better).
    """
    sql = """
    SELECT
        source,
        variable,
        horizon_hours,
        valid_time,
        rmse,
        mae,
        mape
    FROM errors
    WHERE valid_time >= now() - (interval '1 day' * :days)
    """
    df = fetch_df(sql, {"days": int(days)})
    if df.empty:
        logger.info("No error rows found in the last %s days", days)
        return pd.DataFrame(columns=["variable", "horizon_hours", "best_source", "rmse", "mae", "mape", "n"])

    # Aggregate by source, variable, horizon: mean metrics + count
    agg = (
        df.groupby(["source", "variable", "horizon_hours"], as_index=False)
          .agg(
              rmse=("rmse", "mean"),
              mae=("mae", "mean"),
              mape=("mape", "mean"),
              n=("rmse", "size"),
          )
    )

    # For each (variable, horizon), pick the source with the lowest RMSE
    idx = agg.groupby(["variable", "horizon_hours"])["rmse"].idxmin()
    best = agg.loc[idx].reset_index(drop=True)

    # Rename columns for clarity and sort
    best = best.rename(columns={"source": "best_source"})
    best = best[["variable", "horizon_hours", "best_source", "rmse", "mae", "mape", "n"]]
    best = best.sort_values(["variable", "horizon_hours"]).reset_index(drop=True)
    return best
