"""
Champion-Challenger promotion: compare the latest two model entries.
If the challenger outperforms the champion by >2% on both aggregated
RMSE and MAE, promote it. Otherwise keep the current champion.
"""
import json
from sqlalchemy import text
from src.utils.db_utils import db_conn
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

PROMOTION_THRESHOLD = 0.02  # 2%


def better_by(champion_val: float, challenger_val: float) -> float:
    """Return relative improvement of challenger over champion (>0 = better)."""
    if champion_val <= 0:
        return 0.0
    return (champion_val - challenger_val) / champion_val


def main():
    with db_conn() as conn:
        name = "model"
        rows = conn.execute(
            text(
                "SELECT id, mlflow_run_id, metrics_json, created_at, is_champion "
                "FROM models WHERE name = :n ORDER BY id DESC LIMIT 2"
            ),
            {"n": name},
        ).fetchall()

        if len(rows) < 1:
            logger.info("No models registered; nothing to promote")
            return

        champion = conn.execute(
            text("SELECT id, mlflow_run_id, metrics_json FROM models WHERE name = :n AND is_champion = TRUE ORDER BY id DESC LIMIT 1"),
            {"n": name},
        ).fetchone()

        if not champion:
            conn.execute(text("UPDATE models SET is_champion = TRUE WHERE id = :id"), {"id": rows[0].id})
            logger.info("No champion existed; promoted model id=%s as first champion", rows[0].id)
            return

        challenger = rows[0]
        if challenger.id == champion.id:
            logger.info("Champion (id=%s) is already the latest model; no challenger to evaluate", champion.id)
            return

        if not champion.metrics_json or not challenger.metrics_json:
            logger.warning("Missing metrics_json on champion or challenger; skipping promotion")
            return

        champ_m = json.loads(champion.metrics_json) if isinstance(champion.metrics_json, str) else champion.metrics_json
        chall_m = json.loads(challenger.metrics_json) if isinstance(challenger.metrics_json, str) else challenger.metrics_json

        rmse_imp = better_by(champ_m["agg_rmse"], chall_m["agg_rmse"])
        mae_imp = better_by(champ_m["agg_mae"], chall_m["agg_mae"])

        logger.info(
            "Champion (id=%s) agg_rmse=%.4f agg_mae=%.4f | Challenger (id=%s) agg_rmse=%.4f agg_mae=%.4f",
            champion.id, champ_m["agg_rmse"], champ_m["agg_mae"],
            challenger.id, chall_m["agg_rmse"], chall_m["agg_mae"],
        )
        logger.info("Improvement: RMSE %.2f%% MAE %.2f%% (threshold %.1f%%)",
                     rmse_imp * 100, mae_imp * 100, PROMOTION_THRESHOLD * 100)

        if rmse_imp > PROMOTION_THRESHOLD and mae_imp > PROMOTION_THRESHOLD:
            conn.execute(text("UPDATE models SET is_champion = FALSE WHERE name = :n"), {"n": name})
            conn.execute(
                text("UPDATE models SET is_champion = TRUE WHERE id = :id"),
                {"id": challenger.id},
            )
            logger.info("PROMOTED challenger (id=%s) to champion!", challenger.id)

            import os
            import mlflow
            if os.getenv("DAGSHUB_USERNAME") and os.getenv("DAGSHUB_TOKEN"):
                from mlflow.tracking import MlflowClient
                client = MlflowClient()
                try:
                    latest_none = client.get_latest_versions(name, stages=["None"])
                    if latest_none:
                        mv = latest_none[0]
                        client.transition_model_version_stage(
                            name=name, version=mv.version,
                            stage="Production", archive_existing_versions=True,
                        )
                        logger.info("Promoted MLflow model %s version %s to Production", name, mv.version)
                except Exception as e:
                    logger.warning("MLflow promotion failed (non-fatal): %s", e)
        else:
            logger.info("Challenger did not beat threshold; keeping champion (id=%s)", champion.id)


if __name__ == "__main__":
    main()
