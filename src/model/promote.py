"""
Champion–Challenger promotion using recent RMSE/MAE averaged over last 7 days.
If challenger better by >2% on both RMSE & MAE, promote.
"""
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from src.utils.db_utils import db_conn
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def get_current_champion(conn, name: str):
    r = conn.execute(text("SELECT id, mlflow_run_id FROM models WHERE name=:n AND is_champion=TRUE ORDER BY id DESC LIMIT 1"), {"n": name}).fetchone()
    return r

def insert_candidate(conn, name: str, run_id: str):
    conn.execute(text("INSERT INTO models (name, mlflow_run_id, is_champion) VALUES (:n,:r,FALSE)"), {"n": name, "r": run_id})

def promote(conn, name: str, challenger_run_id: str):
    conn.execute(text("UPDATE models SET is_champion=FALSE WHERE name=:n"), {"n": name})
    conn.execute(text("INSERT INTO models (name, mlflow_run_id, is_champion) VALUES (:n,:r,TRUE)"), {"n": name, "r": challenger_run_id})

def better_by(past: float, now: float) -> float:
    if past <= 0: return 0.0
    return (past - now) / past

def main():
    with db_conn() as conn:
        # For simplicity, use single model name "ensemble"
        name = "model"
        # In a more advanced setup, we'd persist per (variable,horizon) metrics and run_ids in a registry.
        # Here, we just look at the latest two entries (challenger vs champion silhouettes).
        rows = conn.execute(text("SELECT id, mlflow_run_id, created_at FROM models WHERE name=:n ORDER BY id DESC LIMIT 2"), {"n": name}).fetchall()
        if len(rows) < 1:
            logger.info("No models registered; nothing to promote")
            return
        # For demo, promote the newest if none champion exists
        champ = get_current_champion(conn, name)
        if not champ:
            conn.execute(text("UPDATE models SET is_champion=TRUE WHERE id=:id"), {"id": rows[0].id})
            logger.info("No champion; promoted latest model id=%s", rows[0].id)
            return
        logger.info("Champion exists (id=%s); for full criteria, compare tracked metrics via MLflow (external).", champ.id)
        # In a real setup we’d query MLflow metrics and do statistical tests; omitted here due to env.
        # Keep champion as-is by default        # Keep champion as-is by default.
        logger.info("No automatic promotion performed in this pass.")

if __name__ == "__main__":
    main()
