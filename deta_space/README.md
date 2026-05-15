# Deploy FastAPI to Deta Space

1. Create a new Deta Space project.
2. Add `src/serve/api/main.py` and a `requirements.txt` (FastAPI, uvicorn, SQLAlchemy, pandas).
3. Set environment variables:
   - `DATABASE_URL` (Neon)
4. Configure `Procfile` or start command: `uvicorn src.serve.api.main:app --host 0.0.0.0 --port 8000`
5. Deta Micros do not support websockets or long-running background tasks; endpoints here are lightweight and DB-backed.

Endpoints:
- `GET /health` → `{"status":"ok"}`
- `GET /metrics` → leaderboard (last 7 days)
- `GET /sources` → per-source metrics
- `POST /predict` → ensemble forecast for given lat/lon/variables/horizons
