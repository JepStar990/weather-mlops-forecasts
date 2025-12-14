import json
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Config:
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
    VISUAL_CROSSING_API_KEY: str = os.getenv("VISUAL_CROSSING_API_KEY", "")
    MET_NO_USER_AGENT: str = os.getenv("MET_NO_USER_AGENT", "your-app/0.1 (email@example.com)")
    NWS_USER_AGENT: str = os.getenv("NWS_USER_AGENT", "your-app/0.1 (email@example.com)")

    DAGSHUB_USERNAME: str = os.getenv("DAGSHUB_USERNAME", "")
    DAGSHUB_TOKEN: str = os.getenv("DAGSHUB_TOKEN", "")
    PUBLIC_REPO_NAME: str = os.getenv("PUBLIC_REPO_NAME", "weather-mlops-forecasts")

    TARGET_LOCATIONS: list[dict] = field(default_factory=lambda: json.loads(os.getenv("TARGET_LOCATIONS", "[]")))
    VARIABLES: list[str] = field(default_factory=lambda: json.loads(os.getenv("VARIABLES", '["temp_2m","wind_speed_10m","precipitation"]')))
    HORIZONS_HOURS: list[int] = field(default_factory=lambda: json.loads(os.getenv("HORIZONS_HOURS", "[1,3,6,12,24,48,72]")))

    LOCAL_TIMEZONE: str = os.getenv("LOCAL_TIMEZONE", "Africa/Johannesburg")

    REQUESTS_CONCURRENCY: int = int(os.getenv("REQUESTS_CONCURRENCY", "4"))
    REQUESTS_TIMEOUT: int = int(os.getenv("REQUESTS_TIMEOUT", "30"))
    REQUESTS_CACHE_TTL_SECONDS: int = int(os.getenv("REQUESTS_CACHE_TTL_SECONDS", "600"))

CFG = Config()

# API endpoints
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
MET_NO_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
OPENWEATHER_URL = "https://api.openweathermap.org/data/3.0/onecall"
VISUAL_CROSSING_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
WEATHER_GOV_POINTS_URL = "https://api.weather.gov/points/{lat},{lon}"
WEATHER_GOV_GRID_URL = "https://api.weather.gov/gridpoints/{office}/{gridX},{gridY}"

# Canonical units
UNIT_MAP = {
    "temp_2m": "C",
    "wind_speed_10m": "m/s",
    "precipitation": "mm",
}

SOURCES = ["open_meteo", "met_no", "openweather", "visual_crossing", "weather_gov", "our_model"]

def clamp_float(x: float, min_v: float = -1e6, max_v: float = 1e6) -> float:
    """Clamp a float value to a safe range to avoid extreme outliers."""
    return float(min(max(x, min_v), max_v))
