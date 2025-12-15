-- Forecasts from each vendor
CREATE TABLE IF NOT EXISTS forecasts (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,              -- 'open_meteo','met_no','openweather','visual_crossing','weather_gov','our_model'
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  variable TEXT NOT NULL,            -- 'temp_2m','wind_speed_10m','precipitation'
  issue_time TIMESTAMPTZ NOT NULL,   -- when forecast was issued
  valid_time TIMESTAMPTZ NOT NULL,   -- target time
  horizon_hours INT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  unit TEXT NOT NULL,                -- 'C','m/s','mm'
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Observed ground truth
CREATE TABLE IF NOT EXISTS observations (
  id BIGSERIAL PRIMARY KEY,
  station_id TEXT,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  variable TEXT NOT NULL,
  obs_time TIMESTAMPTZ NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  unit TEXT NOT NULL,
  source TEXT NOT NULL,              -- 'meteostat','weather_gov', etc.
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Error metrics per source/horizon/variable/time
CREATE TABLE IF NOT EXISTS errors (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  variable TEXT NOT NULL,
  valid_time TIMESTAMPTZ NOT NULL,
  horizon_hours INT NOT NULL,
  mae DOUBLE PRECISION,
  rmse DOUBLE PRECISION,
  mape DOUBLE PRECISION,
  n INT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Lightweight model registry pointer (canonical is DagsHub/MLflow)
CREATE TABLE IF NOT EXISTS models (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  mlflow_run_id TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  is_champion BOOLEAN DEFAULT FALSE
);
