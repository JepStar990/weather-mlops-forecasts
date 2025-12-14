"""
Seed configured locations into a helper table in Neon Postgres.
Reads TARGET_LOCATIONS from environment via src/config.py.
"""
import os
import sys
from sqlalchemy import text

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.config import CFG
from src.utils.db_utils import db_conn

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS locations (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""

INSERT_SQL = "INSERT INTO locations (name, lat, lon) VALUES (:name, :lat, :lon)"

def main() -> None:
    # Ensure we have locations configured
    if not CFG.TARGET_LOCATIONS or not isinstance(CFG.TARGET_LOCATIONS, list):
        raise RuntimeError("TARGET_LOCATIONS is empty or not a list. Check your .env")

    with db_conn() as conn:
        # Create table if it doesn't exist
        conn.execute(text(CREATE_TABLE_SQL))

        # Insert each location from config
        for loc in CFG.TARGET_LOCATIONS:
            # Basic validation of required keys
            if not all(k in loc for k in ("name", "lat", "lon")):
                raise ValueError(f"Location missing keys: {loc}")
            conn.execute(
                text(INSERT_SQL),
                {"name": loc["name"], "lat": float(loc["lat"]), "lon": float(loc["lon"])}
            )

    # Final confirmation message
    print(f"Seeded {len(CFG.TARGET_LOCATIONS)} locations into 'locations' table.")

if __name__ == "__main__":
    main()
