```md
# Weather MLOps: Multi-API Forecast Verification + Ensemble (Zero Budget)

An end‑to‑end, production‑grade (free‑tier) MLOps system to ingest hourly weather forecasts from multiple providers, ingest observed weather, measure forecast error per source/horizon/variable, train and promote our own ensemble model, and serve predictions via FastAPI with a Gradio verification dashboard.

All pipelines run on **free tiers**:
- **Data**: Open‑Meteo (no key), MET Norway Locationforecast (User‑Agent), weather.gov (US NWS), OpenWeather One Call 3.0 (first 1,000 calls/day free), Visual Crossing (≈1,000 records/day free), Meteostat Python library (observations).
- **Warehouse**: Neon Serverless Postgres (free).
- **Experiment Tracking**: DagsHub MLflow-compatible (free for public).
- **Serving**: Deta Space (FastAPI), Hugging Face Spaces (Gradio).
- **Orchestration**: GitHub Actions (UTC; off‑hour cron to avoid congestion).

> **South Africa first**: Includes multiple SA locations out-of-the-box (Johannesburg, Cape Town, Durban, Pretoria, Gqeberha, Bloemfontein, Polokwane, Mbombela, East London). Add more anytime via `TARGET_LOCATIONS`.

---

## Overview

```

        +---------------------------+
        |  GitHub Actions (cron)    |
        +-----------+---------------+
                    |

:17  ingest forecasts (multiple APIs)
v
+--------+--------+
\| Neon Postgres   |
\|  (forecasts)    |
+--------+--------+
|
:47  ingest obs (Meteostat) + compute errors
v
+--------+--------+
\| Neon Postgres   |
\| (observations,  |
\|   errors)       |
+--------+--------+
/        
daily 03:17   /        \  :27 predict with champion
train+promote /          \  write our\_model to forecasts
v            v
+-----+----+   +---+------+
\| DagsHub  |   | FastAPI  |
\|  MLflow  |   | (Deta)   |
+----------+   +---+------+
|
+-----+------+
\|  Gradio    |
\| (HF Spaces)|
+------------+

````

---

## Required Environment Variables

Set in `.env` locally and in GitHub / Spaces / Deta secrets:

- `DATABASE_URL` – Neon connection string
- `OPENWEATHER_API_KEY` – optional, 1,000 calls/day free
- `VISUAL_CROSSING_API_KEY` – optional, ~1,000 records/day free
- `MET_NO_USER_AGENT` – e.g., `your-app/0.1 (email@example.com)`
- `NWS_USER_AGENT` – same format (used for weather.gov)
- `DAGSHUB_USERNAME` / `DAGSHUB_TOKEN` – for MLflow tracking
- `PUBLIC_REPO_NAME` – e.g., `weather-mlops-forecasts`
- `TARGET_LOCATIONS` – JSON array of locations (default SA cities provided)
- `VARIABLES` – e.g., `["temp_2m","wind_speed_10m","precipitation"]`
- `HORIZONS_HOURS` – e.g., `[1,3,6,12,24,48,72]`
- `LOCAL_TIMEZONE` – for display only (UTC is canonical)

---

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Set env
cp .env.example .env
# Edit .env with your DATABASE_URL, keys, DagsHub creds, etc.

# Bootstrap DB
psql $DATABASE_URL -f src/db/schema.sql
python scripts/seed_locations.py

# Smoke test: ingest a provider
python src/etl/ingest_open_meteo.py
````

***

## Deployment

### Neon (DB)

1.  Create a Neon project (free), obtain `DATABASE_URL`.
2.  Run `src/db/schema.sql`.
3.  (Optional) Create a read‑only role for dashboards.

### DagsHub (MLflow)

1.  Create a public repo named `weather-mlops-forecasts`.
2.  Generate a token; set `DAGSHUB_USERNAME` & `DAGSHUB_TOKEN`.
3.  MLflow tracking URI is `https://dagshub.com/<user>/<repo>.mlflow`.

### Hugging Face Spaces (Gradio)

1.  New Space (Gradio). Copy `src/serve/dashboard/app.py` + minimal utils and `requirements.txt`.
2.  Set `DATABASE_URL` secret (read-only).
3.  First charts render after data arrives.

### Deta Space (FastAPI)

1.  Create a Deta Space project.
2.  Add `src/serve/api/main.py` and `requirements.txt`.
3.  Set `DATABASE_URL`.
4.  Start command: `uvicorn src.serve.api.main:app --host 0.0.0.0 --port 8000`.

***

## Scheduling (GitHub Actions)

*   **ETL (forecasts)**: hourly at **:17** UTC
*   **Verify (obs + errors)**: hourly at **:47** UTC (≈30 min later)
*   **Predict (our\_model)**: hourly at **:27** UTC
*   **Monitor (leaderboard)**: hourly at **:37** UTC
*   **Train + Promote**: daily at **03:17** UTC

> All workflows use Python 3.11. Times avoid the top of the hour to reduce queueing.

***

## Attribution & Licenses

*   **Open‑Meteo** (free, no key; non‑commercial): <https://open-meteo.com/>
*   **MET Norway Locationforecast 2.0** (User‑Agent required): <https://api.met.no/weatherapi/locationforecast/2.0/documentation>
*   **weather.gov** (NWS API): <https://www.weather.gov/documentation/services-web-api>
*   **OpenWeather One Call 3.0** (1,000 calls/day free): <https://openweathermap.org/price>
*   **Visual Crossing** (free tier): <https://www.visualcrossing.com/resources/blog/how-do-i-get-free-weather-api-access/>
*   **Meteostat** (observations; CC BY‑NC): <https://pypi.org/project/meteostat/>
*   **Neon Serverless Postgres**: <https://neon.com/docs/introduction/plans>
*   **DagsHub MLflow**: <https://dagshub.com/pricing>
*   **Hugging Face Spaces**: <https://huggingface.co/docs/hub/en/spaces-overview>
*   **Deta Space**: <https://fastapi.xiniushu.com/az/deployment/deta/>

**License**: MIT (with attribution to data providers above).

***
