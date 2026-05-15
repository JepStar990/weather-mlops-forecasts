from src.model.predict import main as predict
from src.utils.db_utils import QuotaExceededError
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    try:
        predict()
    except QuotaExceededError:
        logger.warning("Skipping run — Neon data transfer quota exceeded")
        exit(0)
