import hashlib
import json
import os
import time
from typing import Any, Dict, Optional
import requests
from src.config import CFG
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)
CACHE_DIR = os.path.join(".cache", "http")
os.makedirs(CACHE_DIR, exist_ok=True)

def _cache_path(key: str) -> str:
    return os.path.join(CACHE_DIR, f"{key}.json")

def _key_from(url: str, params: Optional[dict], headers: Optional[dict]) -> str:
    s = url + "|" + json.dumps(params or {}, sort_keys=True) + "|" + json.dumps(headers or {}, sort_keys=True)
    return hashlib.sha256(s.encode()).hexdigest()

def get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, ttl: int | None = None, timeout: int | None = None) -> Dict[str, Any]:
    ttl = ttl or CFG.REQUESTS_CACHE_TTL_SECONDS
    timeout = timeout or CFG.REQUESTS_TIMEOUT
    key = _key_from(url, params, headers)
    path = _cache_path(key)

    # serve from cache
    if os.path.exists(path):
        age = time.time() - os.path.getmtime(path)
        if age <= ttl:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass

    # backoff loop (only retry on 429 or 5xx; fail fast on 4xx auth errors)
    backoff = 1.5
    for attempt in range(6):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)

            # Fail fast on 401/403 — retrying won't fix bad credentials
            if resp.status_code in (401, 403):
                logger.warning("GET %s returned %d (check API key); skipping", url, resp.status_code)
                return {}

            # Retry on 429 or 5xx
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"HTTP {resp.status_code}: {resp.text[:200]}")

            resp.raise_for_status()
            data = resp.json()

            # Write to cache (best-effort)
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except Exception:
                pass

            return data

        except requests.HTTPError as e:
            # Only hit for retriable statuses (429/5xx) — backoff and retry
            sleep = backoff ** attempt
            logger.warning(
                "GET %s failed (attempt %d): %s; sleeping %.1fs",
                url,
                attempt + 1,
                str(e),
                sleep,
            )
            time.sleep(sleep)
        except Exception as e:
            # Connection errors, timeouts — backoff and retry
            sleep = backoff ** attempt
            logger.warning(
                "GET %s failed (attempt %d): %s; sleeping %.1fs",
                url,
                attempt + 1,
                str(e),
                sleep,
            )
            time.sleep(sleep)

    # All retries exhausted — return empty so one vendor can't break the pipeline
    logger.warning("GET %s failed after %d attempts; returning empty", url, 6)
    return {}
