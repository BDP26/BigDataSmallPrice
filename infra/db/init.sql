-- BigDataSmallPrice – TimescaleDB Schema
-- Executed automatically on first container start

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─── 1. ENTSO-E Day-Ahead Prices ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS entsoe_day_ahead_prices (
    time            TIMESTAMPTZ NOT NULL,
    domain          TEXT        NOT NULL,
    price_eur_mwh   DOUBLE PRECISION,
    currency        TEXT DEFAULT 'EUR'
);

SELECT create_hypertable(
    'entsoe_day_ahead_prices', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS entsoe_time_domain_idx
    ON entsoe_day_ahead_prices (time, domain);

-- ─── 2. Weather Hourly (open-meteo) ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_hourly (
    time                TIMESTAMPTZ      NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    temperature_2m      DOUBLE PRECISION,
    wind_speed_10m      DOUBLE PRECISION,
    shortwave_radiation DOUBLE PRECISION,
    cloud_cover         DOUBLE PRECISION,
    precipitation_mm    DOUBLE PRECISION  -- Niederschlag (mm/h) – req.md Phase 1
);

-- Migration for existing installs (idempotent)
ALTER TABLE weather_hourly ADD COLUMN IF NOT EXISTS precipitation_mm DOUBLE PRECISION;

SELECT create_hypertable(
    'weather_hourly', 'time',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS weather_time_location_idx
    ON weather_hourly (time, latitude, longitude);

-- ─── 3. EKZ Tariffs (15-min raw) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ekz_tariffs_raw (
    time            TIMESTAMPTZ     NOT NULL,
    tariff_type     TEXT            NOT NULL,
    price_chf_kwh   DOUBLE PRECISION
);

SELECT create_hypertable(
    'ekz_tariffs_raw', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS ekz_time_type_idx
    ON ekz_tariffs_raw (time, tariff_type);

-- ─── 4. BAFU Hydro Data ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bafu_hydro (
    time            TIMESTAMPTZ     NOT NULL,
    station_id      TEXT            NOT NULL,
    discharge_m3s   DOUBLE PRECISION,
    level_masl      DOUBLE PRECISION
);

SELECT create_hypertable(
    'bafu_hydro', 'time',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS bafu_time_station_idx
    ON bafu_hydro (time, station_id);

-- ─── 5. Continuous Aggregate: EKZ 15min → 1h ────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS ekz_tariffs_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    tariff_type,
    AVG(price_chf_kwh)          AS price_chf_kwh_avg,
    MIN(price_chf_kwh)          AS price_chf_kwh_min,
    MAX(price_chf_kwh)          AS price_chf_kwh_max,
    COUNT(*)                    AS sample_count
FROM ekz_tariffs_raw
GROUP BY hour, tariff_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'ekz_tariffs_hourly',
    start_offset => INTERVAL '2 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ─── 6. CKW Tariffs (15-min raw) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ckw_tariffs_raw (
    time          TIMESTAMPTZ      NOT NULL,
    tariff_type   TEXT             NOT NULL,  -- 'grid_usage'|'grid'|'electricity'|'integrated'
    price_chf_kwh DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
    'ckw_tariffs_raw', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS ckw_tariffs_raw_time_type_idx
    ON ckw_tariffs_raw (time, tariff_type);

-- ─── 7. Groupe E Tariffs (15-min raw) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS groupe_e_tariffs_raw (
    time          TIMESTAMPTZ      NOT NULL,
    tariff_type   TEXT             NOT NULL,  -- 'grid' | 'integrated'
    price_chf_kwh DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
    'groupe_e_tariffs_raw', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS groupe_e_tariffs_raw_time_type_idx
    ON groupe_e_tariffs_raw (time, tariff_type);

-- ─── 8. Continuous Aggregate: CKW 15min → 1h ─────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS ckw_tariffs_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    tariff_type,
    AVG(price_chf_kwh)          AS avg_chf_kwh,
    MIN(price_chf_kwh)          AS min_chf_kwh,
    MAX(price_chf_kwh)          AS max_chf_kwh,
    COUNT(*)                    AS interval_count
FROM ckw_tariffs_raw
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'ckw_tariffs_hourly',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => TRUE
);

-- ─── 9. Continuous Aggregate: Groupe E 15min → 1h ────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS groupe_e_tariffs_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    tariff_type,
    AVG(price_chf_kwh)          AS avg_chf_kwh,
    MIN(price_chf_kwh)          AS min_chf_kwh,
    MAX(price_chf_kwh)          AS max_chf_kwh,
    COUNT(*)                    AS interval_count
FROM groupe_e_tariffs_raw
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'groupe_e_tariffs_hourly',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => TRUE
);

-- ─── 10. Feature View: training_features (Phase 2) ────────────────────────────
-- Joins ENTSO-E, Weather, BAFU, Groupe E (primary) and CKW (secondary) into
-- one hourly feature table. Includes lag features, rolling averages, and
-- calendar features.
--
-- Primary tariff signal:  Groupe E 'integrated' (stdev≈0.076, highest variability)
-- Secondary tariff signal: CKW 'integrated' (regional price differential)

-- DROP first: CREATE OR REPLACE VIEW cannot rename existing columns.
DROP VIEW IF EXISTS training_features CASCADE;
CREATE VIEW training_features AS
WITH
  -- ENTSO-E prices with lag and rolling window features
  price_features AS (
    SELECT
      time,
      price_eur_mwh,
      LAG(price_eur_mwh, 1)   OVER (ORDER BY time) AS lag_1h,
      LAG(price_eur_mwh, 2)   OVER (ORDER BY time) AS lag_2h,
      LAG(price_eur_mwh, 24)  OVER (ORDER BY time) AS lag_24h,
      LAG(price_eur_mwh, 168) OVER (ORDER BY time) AS lag_168h,
      AVG(price_eur_mwh) OVER (
        ORDER BY time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
      ) AS rolling_avg_24h,
      AVG(price_eur_mwh) OVER (
        ORDER BY time ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
      ) AS rolling_avg_7d,
      EXTRACT(hour  FROM time)::INT                                          AS hour_of_day,
      EXTRACT(dow   FROM time)::INT                                          AS day_of_week,
      EXTRACT(month FROM time)::INT                                          AS month,
      CASE WHEN EXTRACT(dow  FROM time) IN (0, 6)       THEN 1 ELSE 0 END   AS is_weekend,
      CASE WHEN EXTRACT(hour FROM time) BETWEEN 7 AND 22 THEN 1 ELSE 0 END  AS is_peak_hour
    FROM entsoe_day_ahead_prices
    WHERE domain = '10YCH-SWISSGRIDZ'
  ),

  -- Join weather, BAFU, Groupe E, CKW; add temperature rolling average
  joined AS (
    SELECT
      pf.time,
      pf.price_eur_mwh,
      -- req.md Option 1 (EPEX direkt)
      pf.price_eur_mwh AS epex_t,
      pf.lag_1h,
      pf.lag_2h,
      pf.lag_24h,
      pf.lag_168h,
      -- req.md Option 2 aliases
      pf.lag_24h AS epex_lag_1d,
      pf.lag_168h AS epex_lag_7d,
      pf.rolling_avg_24h,
      pf.rolling_avg_7d,
      pf.hour_of_day,
      pf.day_of_week,
      pf.month,
      pf.is_weekend,
      pf.is_peak_hour,
      w.temperature_2m,
      -- Proxy until CH-wide temperature feed is added
      w.temperature_2m AS temp_ch_avg,
      w.wind_speed_10m,
      -- Proxy until ENTSO-E generation series are integrated
      w.wind_speed_10m AS wind_generation_eu,
      w.shortwave_radiation,
      -- Proxy until CH generation series are integrated
      w.shortwave_radiation AS solar_generation_ch,
      w.cloud_cover,
      w.precipitation_mm,
      AVG(w.temperature_2m) OVER (
        ORDER BY pf.time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
      ) AS temp_rolling_avg_24h,
      bh.discharge_m3s,
      bh.level_masl,
      -- Proxy until explicit reservoir source is integrated
      bh.level_masl AS hydro_reservoir,
      CASE WHEN pf.is_weekend = 1 THEN 'weekend' ELSE 'workday' END AS day_type,
      ge.avg_chf_kwh  AS tariff_price_chf_kwh_avg,
      ck.avg_chf_kwh  AS ckw_price_chf_kwh_avg
    FROM price_features pf
    LEFT JOIN weather_hourly w
      ON  w.time      = pf.time
      AND w.latitude  = 47.5001
      AND w.longitude = 8.7502
    LEFT JOIN bafu_hydro bh
      ON  bh.time       = pf.time
      AND bh.station_id = '2018'
    LEFT JOIN groupe_e_tariffs_hourly ge
      ON  ge.hour        = pf.time
      AND ge.tariff_type = 'integrated'
    LEFT JOIN ckw_tariffs_hourly ck
      ON  ck.hour        = pf.time
      AND ck.tariff_type = 'integrated'
  )

SELECT * FROM joined;

-- ─── 10b. API Call Log (rate-limit tracking — ISOLATED from ML features) ─────
-- WARNING: this table MUST NEVER be joined into training_features or
-- winterthur_net_load_features. It is operational metadata only.
CREATE TABLE IF NOT EXISTS api_call_log (
    id               BIGSERIAL,
    source           TEXT        NOT NULL,  -- 'entsoe'|'openmeteo'|'ekz'|'ckw'|'groupe_e'|'bafu'
    called_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status_code      INT         NOT NULL,  -- actual HTTP status returned
    was_rate_limited BOOLEAN     NOT NULL DEFAULT FALSE,  -- TRUE if status_code == 429
    response_ms      INT,                   -- response time in milliseconds
    date_fetched     TEXT,                  -- the data-date requested ("YYYY-MM-DD" or NULL)
    CONSTRAINT api_call_log_pkey PRIMARY KEY (id, called_at)
);

SELECT create_hypertable(
    'api_call_log', 'called_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS api_call_log_source_idx ON api_call_log (source, called_at DESC);

-- ─── 11. Winterthur Load (OGD Bruttolastgang) ────────────────────────────────
CREATE TABLE IF NOT EXISTS winterthur_load (
    time     TIMESTAMPTZ      NOT NULL,
    load_kwh DOUBLE PRECISION,
    UNIQUE (time)
);

SELECT create_hypertable(
    'winterthur_load', 'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '30 days'
);

-- ─── 12. Winterthur PV Feed-in (OGD Netzeinspeisung) ─────────────────────────
CREATE TABLE IF NOT EXISTS winterthur_pv (
    time   TIMESTAMPTZ      NOT NULL,
    pv_kwh DOUBLE PRECISION,
    UNIQUE (time)
);

SELECT create_hypertable(
    'winterthur_pv', 'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '30 days'
);

-- ─── 13. Feature View: winterthur_net_load_features (Model A) ─────────────────
-- Net load = bruttolastgang – PV feed-in. Used as target for grid-load forecasting.
-- Calendar + weather features are joined here; is_holiday computed in Python.
DROP VIEW IF EXISTS winterthur_net_load_features CASCADE;
CREATE VIEW winterthur_net_load_features AS
SELECT
    w.time,
    w.load_kwh - COALESCE(p.pv_kwh, 0)               AS net_load_kwh,
    EXTRACT(HOUR    FROM w.time)::INT                 AS hour_of_day,
    EXTRACT(HOUR    FROM w.time)::INT                 AS hour,
    EXTRACT(DOW     FROM w.time)::INT                 AS day_of_week,
    EXTRACT(DOW     FROM w.time)::INT                 AS weekday,
    EXTRACT(MONTH   FROM w.time)::INT                 AS month,
    EXTRACT(QUARTER FROM w.time)::INT                 AS quarter,
    CASE WHEN EXTRACT(DOW FROM w.time) IN (0,6) THEN 1 ELSE 0 END AS is_weekend,
    LAG(w.load_kwh - COALESCE(p.pv_kwh, 0), 1)   OVER (ORDER BY w.time) AS load_lag_1h,
    LAG(w.load_kwh - COALESCE(p.pv_kwh, 0), 24)  OVER (ORDER BY w.time) AS load_lag_1d,
    LAG(w.load_kwh - COALESCE(p.pv_kwh, 0), 168) OVER (ORDER BY w.time) AS load_lag_7d,
    AVG(w.load_kwh - COALESCE(p.pv_kwh, 0)) OVER (
        ORDER BY w.time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    )                                                 AS load_rolling_avg_24h,
    wr.temperature_2m,
    wr.temperature_2m                                 AS temp_c,
    wr.wind_speed_10m,
    wr.wind_speed_10m                                 AS wind_speed_ms,
    wr.shortwave_radiation,
    wr.shortwave_radiation                            AS ghi_wm2,
    wr.cloud_cover,
    wr.cloud_cover                                    AS cloud_cover_pct,
    wr.precipitation_mm,
    COALESCE(p.pv_kwh, 0)                            AS pv_feed_in_kwh,
    COALESCE(p.pv_kwh, 0)                            AS pv_feed_in
FROM winterthur_load w
LEFT JOIN winterthur_pv p USING (time)
LEFT JOIN weather_hourly wr
    ON  date_trunc('hour', w.time) = wr.time
    AND wr.latitude  = 47.5001
    AND wr.longitude = 8.7502;
