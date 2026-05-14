"""
Champion-Challenger promotion: compare the latest two model entries.
If the challenger outperforms the champion by >2% on both aggregated
RMSE and MAE, promote it. Otherwise keep the current champion.
"""
import json
import os
from sqlalchemy import text
from src.utils.db_utils import db_conn
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

PROMOTION_THRESHOLD = 0.02  # 2%


def _mlflow_promote(name: str, run_id: str | None = None):
    """Transition the model version for `name` to Production in MLflow.

    If run_id is given, only promote the version matching that run.
    Otherwise promote the latest version in 'None' stage.
    """
    if not (os.getenv("DAGSHUB_USERNAME") and os.getenv("DAGSHUB_TOKEN")):
        return
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient()

        if run_id:
            # Find the specific version matching this run_id
            versions = client.search_model_versions(f"name='{name}'")
            target_version = None
            for mv in versions:
                if mv.run_id == run_id and mv.current_stage in ("None", "Staging"):
                    target_version = mv
                    break
            if target_version:
                client.transition_model_version_stage(
                    name=name, version=target_version.version,
                    stage="Production", archive_existing_versions=True,
                )
                logger.info("%s: promoted MLflow model version %s to Production (run_id=%s)",
                            name, target_version.version, run_id)
        else:
            latest_none = client.get_latest_versions(name, stages=["None"])
            if latest_none:
                mv = latest_none[0]
                client.transition_model_version_stage(
                    name=name, version=mv.version,
                    stage="Production", archive_existing_versions=True,
                )
                logger.info("%s: promoted MLflow model version %s to Production", name, mv.version)
    except Exception as e:
        logger.warning("%s: MLflow promotion failed (non-fatal): %s", name, e)


def better_by(champion_val: float, challenger_val: float) -> float:
    """Return relative improvement of challenger over champion (>0 = better)."""
    if champion_val <= 0:
        return 0.0
    return (champion_val - challenger_val) / champion_val


def _promote_one(name: str, conn):
    """Run champion-challenger promotion for a single model name."""
    rows = conn.execute(
        text(
            "SELECT id, mlflow_run_id, metrics_json, created_at, is_champion "
            "FROM models WHERE name = :n ORDER BY id DESC LIMIT 2"
        ),
        {"n": name},
    ).fetchall()

    if len(rows) < 1:
        logger.info("%s: no models registered; skipping", name)
        return

    champion = conn.execute(
        text("SELECT id, mlflow_run_id, metrics_json FROM models WHERE name = :n AND is_champion = TRUE ORDER BY id DESC LIMIT 1"),
        {"n": name},
    ).fetchone()

    if not champion:
        conn.execute(text("UPDATE models SET is_champion = TRUE WHERE id = :id"), {"id": rows[0].id})
        logger.info("%s: no champion existed; promoted model id=%s as first champion", name, rows[0].id)
        _mlflow_promote(name, run_id=rows[0].mlflow_run_id)
        return

    challenger = rows[0]
    if challenger.id == champion.id:
        logger.info("%s: champion (id=%s) is already the latest model; no challenger", name, champion.id)
        return

    if not champion.metrics_json:
        logger.info("%s: champion (id=%s) has no metrics_json; auto-promoting challenger", name, champion.id)
        conn.execute(text("UPDATE models SET is_champion = FALSE WHERE name = :n"), {"n": name})
        conn.execute(text("UPDATE models SET is_champion = TRUE WHERE id = :id"), {"id": challenger.id})
        logger.info("%s: PROMOTED challenger (id=%s) to champion!", name, challenger.id)
        _mlflow_promote(name, run_id=challenger.mlflow_run_id)
        return
    if not challenger.metrics_json:
        logger.warning("%s: challenger (id=%s) has no metrics_json; skipping", name, challenger.id)
        return

    champ_m = json.loads(champion.metrics_json) if isinstance(champion.metrics_json, str) else champion.metrics_json
    chall_m = json.loads(challenger.metrics_json) if isinstance(challenger.metrics_json, str) else challenger.metrics_json

    # Support both new format (per-model: rmse/mae) and old format (aggregated: agg_rmse/agg_mae)
    c_rmse = champ_m.get("rmse", champ_m.get("agg_rmse"))
    c_mae = champ_m.get("mae", champ_m.get("agg_mae"))
    ch_rmse = chall_m.get("rmse", chall_m.get("agg_rmse"))
    ch_mae = chall_m.get("mae", chall_m.get("agg_mae"))

    if c_rmse is None or ch_rmse is None:
        logger.warning("%s: missing rmse/mae in metrics; auto-promoting challenger", name)
        conn.execute(text("UPDATE models SET is_champion = FALSE WHERE name = :n"), {"n": name})
        conn.execute(text("UPDATE models SET is_champion = TRUE WHERE id = :id"), {"id": challenger.id})
        logger.info("%s: PROMOTED challenger (id=%s) to champion!", name, challenger.id)
        _mlflow_promote(name, run_id=challenger.mlflow_run_id)
        return

    rmse_imp = better_by(c_rmse, ch_rmse)
    mae_imp = better_by(c_mae, ch_mae)

    logger.info(
        "%s: Champion (id=%s) rmse=%.4f mae=%.4f | Challenger (id=%s) rmse=%.4f mae=%.4f",
        name, champion.id, c_rmse, c_mae,
        challenger.id, ch_rmse, ch_mae,
    )
    logger.info("%s: Improvement: RMSE %.2f%% MAE %.2f%% (threshold %.1f%%)",
                 name, rmse_imp * 100, mae_imp * 100, PROMOTION_THRESHOLD * 100)

    if rmse_imp > PROMOTION_THRESHOLD and mae_imp > PROMOTION_THRESHOLD:
        conn.execute(text("UPDATE models SET is_champion = FALSE WHERE name = :n"), {"n": name})
        conn.execute(
            text("UPDATE models SET is_champion = TRUE WHERE id = :id"),
            {"id": challenger.id},
        )
        logger.info("%s: PROMOTED challenger (id=%s) to champion!", name, challenger.id)
        _mlflow_promote(name, run_id=challenger.mlflow_run_id)
    else:
        logger.info("%s: challenger did not beat threshold; keeping champion (id=%s)", name, champion.id)


def main():
    with db_conn() as conn:
        names = conn.execute(
            text("SELECT DISTINCT name FROM models ORDER BY name")
        ).fetchall()

        if not names:
            logger.info("No models registered; nothing to promote")
            return

        # Ensure all current champions have their MLflow Production stage set
        champions = conn.execute(
            text("SELECT name, mlflow_run_id FROM models WHERE is_champion = TRUE")
        ).fetchall()
        for name, run_id in champions:
            try:
                _mlflow_promote(name, run_id=run_id)
            except Exception as e:
                logger.warning("%s: initial MLflow promotion backfill failed: %s", name, e)

        for (name,) in names:
            try:
                _promote_one(name, conn)
            except Exception as e:
                logger.error("%s: promotion failed: %s", name, e)


if __name__ == "__main__":
    main()
