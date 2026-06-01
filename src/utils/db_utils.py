import os
import time
from typing import Iterable, Mapping, Sequence
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from contextlib import contextmanager
from src.config import CFG
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

_engine: Engine | None = None

RETRY_EXCEPTIONS = (OperationalError,)
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds
QUOTA_EXCEEDED_MSG = "exceeded the data transfer quota"


def _is_quota_error(exc: Exception) -> bool:
    return QUOTA_EXCEEDED_MSG in str(exc).lower()


class QuotaExceededError(RuntimeError):
    """Raised when Neon data transfer quota is exceeded — job should exit gracefully."""


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        if not CFG.DATABASE_URL:
            raise RuntimeError("DATABASE_URL not set")
        _engine = create_engine(
            CFG.DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"connect_timeout": 30},
        )
        # Verify connection with retries — Neon can be suspended and needs
        # a cold-start wake-up that sometimes fails on the first attempt.
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with _engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Connected SQLAlchemy engine")
                break
            except RETRY_EXCEPTIONS as e:
                if _is_quota_error(e):
                    _engine = None
                    raise QuotaExceededError(
                        "Neon data transfer quota exceeded — skipping until quota resets"
                    ) from e
                if attempt == MAX_RETRIES:
                    _engine = None
                    raise
                delay = RETRY_BASE_DELAY ** attempt
                logger.warning(
                    "DB connection attempt %d/%d failed (%s); retrying in %ds",
                    attempt, MAX_RETRIES, e, delay,
                )
                time.sleep(delay)
    return _engine

@contextmanager
def db_conn():
    eng = get_engine()
    with eng.begin() as conn:
        yield conn


def insert_dataframe(df: pd.DataFrame, table: str, dtype: Mapping | None = None, chunksize: int = 1000):
    if df.empty:
        logger.info("No rows to insert into %s", table)
        return 0
    df.to_sql(table, get_engine(), if_exists="append", index=False, dtype=dtype, chunksize=chunksize, method="multi")
    logger.info("Inserted %d rows into %s", len(df), table)
    return len(df)


def insert_dataframe_dedup(df: pd.DataFrame, table: str, conflict_cols: list[str], chunksize: int = 1000):
    """Insert via temp table with ON CONFLICT DO NOTHING to skip duplicates."""
    if df.empty:
        logger.info("No rows to insert into %s", table)
        return 0
    eng = get_engine()
    tmp = f"_tmp_{table}"
    conflict_clause = ", ".join(conflict_cols)
    cols = ", ".join(df.columns)  # explicit column list avoids type mismatch with auto-increment id
    total = 0
    for start in range(0, len(df), chunksize):
        chunk = df.iloc[start:start + chunksize]
        with eng.begin() as conn:
            chunk.to_sql(tmp, conn, if_exists="replace", index=False, method="multi")
            result = conn.execute(
                text(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {tmp} ON CONFLICT ({conflict_clause}) DO NOTHING"),
            )
            conn.execute(text(f"DROP TABLE IF EXISTS {tmp}"))
            total += result.rowcount
    logger.info("Inserted %d new rows into %s (%d duplicates skipped)", total, table, len(df) - total)
    return total

def fetch_df(sql: str, params: Mapping | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), con=get_engine(), params=params or {})
