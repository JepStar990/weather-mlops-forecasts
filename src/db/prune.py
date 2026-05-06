"""
Batch-delete old rows to stay within Neon free-tier storage limits (~0.5 GB).
Uses batched DELETE to avoid long-running transactions.
"""
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from src.config import CFG
from src.utils.db_utils import db_conn
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

TABLE_RETENTION = {
    "forecasts": CFG.FORECAST_RETENTION_DAYS,
    "observations": CFG.OBSERVATION_RETENTION_DAYS,
    "errors": CFG.ERROR_RETENTION_DAYS,
}

COLUMN_MAP = {
    "forecasts": "valid_time",
    "observations": "obs_time",
    "errors": "valid_time",
}


def prune_table(table: str, retention_days: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    col = COLUMN_MAP[table]
    total_deleted = 0
    batch_size = CFG.PRUNE_BATCH_SIZE

    with db_conn() as conn:
        while True:
            result = conn.execute(
                text(f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} WHERE {col} < :cutoff LIMIT :batch)"),
                {"cutoff": cutoff, "batch": batch_size},
            )
            deleted = result.rowcount
            total_deleted += deleted
            if deleted < batch_size:
                break

    if total_deleted:
        logger.info("Pruned %d rows from %s (cutoff: %s)", total_deleted, table, cutoff.isoformat())
    return total_deleted


def prune_all() -> dict[str, int]:
    results = {}
    for table, days in TABLE_RETENTION.items():
        results[table] = prune_table(table, days)
    return results


def table_row_counts() -> dict[str, int]:
    counts = {}
    with db_conn() as conn:
        for table in TABLE_RETENTION:
            row = conn.execute(text(f"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = '{table}'")).fetchone()
            counts[table] = row[0] if row and row[0] else 0
    return counts


def main():
    counts_before = table_row_counts()
    logger.info("Row counts before pruning: %s", counts_before)
    results = prune_all()
    counts_after = table_row_counts()
    logger.info("Row counts after pruning: %s", counts_after)
    logger.info("Prune summary: %s", results)


if __name__ == "__main__":
    main()
