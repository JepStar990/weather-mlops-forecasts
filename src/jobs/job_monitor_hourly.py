from src.utils.logging_utils import get_logger
from src.verify.leaderboard import leaderboard
from src.db.prune import table_row_counts

logger = get_logger(__name__)

def main() -> None:
    counts = table_row_counts()
    logger.info("Table row counts: %s", counts)

    total = sum(counts.values())
    logger.info("Total estimated rows: %d (~%.1f MB)", total, total * 110 / 1_000_000)

    lb = leaderboard(7)
    if lb is None or lb.empty:
        logger.info("No leaderboard data available for the last 7 days.")
        return

    logger.info("Leaderboard (last 7 days):\n%s", lb.to_string(index=False))

if __name__ == "__main__":
    main()
