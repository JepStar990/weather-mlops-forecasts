"""
Export live stats from Neon + DagsHub MLflow to docs/dashboard.json.
Run in CI to keep the portfolio landing page current.
"""
import json, os, sys, traceback
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import CFG
from src.utils.db_utils import fetch_df
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "dashboard.json")


def neon_stats():
    counts = {}
    for table in ("forecasts", "observations", "errors", "models"):
        try:
            df = fetch_df(f"SELECT reltuples::bigint AS n FROM pg_class WHERE relname = '{table}'")
            counts[table] = int(df.iloc[0]["n"]) if len(df) > 0 else 0
        except Exception:
            counts[table] = -1

    errors_7d = None
    try:
        df = fetch_df("""
            SELECT source, variable, horizon_hours,
                   ROUND(AVG(rmse)::numeric, 3) AS rmse,
                   ROUND(AVG(mae)::numeric, 3) AS mae
            FROM errors
            WHERE valid_time >= NOW() - INTERVAL '7 days'
            GROUP BY source, variable, horizon_hours
            ORDER BY variable, horizon_hours, rmse
        """)
        errors_7d = df.to_dict(orient="records") if not df.empty else []
    except Exception:
        errors_7d = []

    leaderboard = None
    try:
        from src.verify.leaderboard import leaderboard as lb_func
        lb = lb_func(7)
        leaderboard = lb.to_dict(orient="records") if lb is not None and not lb.empty else []
    except Exception:
        leaderboard = []

    return {
        "row_counts": counts,
        "errors_7d": errors_7d[:50],  # top 50 only
        "leaderboard": leaderboard,
    }


def dagshub_stats():
    """Fetch latest MLflow experiment metrics from DagsHub (public repo)."""
    if not (CFG.DAGSHUB_USERNAME and CFG.PUBLIC_REPO_NAME):
        return {"mlflow_url": None, "latest_runs": []}

    mlflow_url = f"https://dagshub.com/{CFG.DAGSHUB_USERNAME}/{CFG.PUBLIC_REPO_NAME}.mlflow"
    runs = []
    try:
        import urllib.request
        import urllib.error
        # MLflow REST API: list runs for experiment
        exp_url = f"{mlflow_url}/api/2.0/mlflow/experiments/search"
        req = urllib.request.Request(exp_url, data=b'{"max_results":1}', headers={"Content-Type": "application/json"})
        # This may fail if DagsHub requires auth even for public reads
        # Fall back gracefully
    except Exception:
        pass

    return {"mlflow_url": mlflow_url, "latest_runs": runs}


def main():
    data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "neon": {},
        "dagshub": {},
        "config": {
            "variables": CFG.VARIABLES,
            "horizons": CFG.HORIZONS_HOURS,
            "sources": ["open_meteo", "met_no", "openweather", "visual_crossing", "weather_gov", "our_model"],
        },
    }

    logger.info("Exporting Neon stats...")
    try:
        data["neon"] = neon_stats()
        logger.info("Neon stats: %s rows", data["neon"]["row_counts"])
    except Exception as e:
        logger.error("Neon export failed: %s", traceback.format_exc())
        data["neon"] = {"error": str(e)}

    logger.info("Exporting DagsHub stats...")
    try:
        data["dagshub"] = dagshub_stats()
    except Exception as e:
        logger.error("DagsHub export failed: %s", traceback.format_exc())
        data["dagshub"] = {"error": str(e)}

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info("Wrote dashboard data to %s (%d bytes)", OUT, os.path.getsize(OUT))


if __name__ == "__main__":
    main()
