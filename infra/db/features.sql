-- BigDataSmallPrice – Feature Migration Script
-- Applies all Phase 2/3 schema changes to a running TimescaleDB instance.
-- Idempotent: safe to re-run on an existing database.
--
-- Usage (against running DB on port 5433):
--   psql -h localhost -p 5433 -U bdsp -d bdsp -f infra/db/features.sql

-- ─── API Call Log (rate-limit tracking — ISOLATED from ML features) ───────────
-- WARNING: this table MUST NEVER be joined into training_features or
-- winterthur_net_load_features. It is operational metadata only.
CREATE TABLE IF NOT EXISTS api_call_log (
    id               BIGSERIAL,
    source           TEXT        NOT NULL,
    called_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status_code      INT         NOT NULL,
    was_rate_limited BOOLEAN     NOT NULL DEFAULT FALSE,
    response_ms      INT,
    date_fetched     TEXT,
    CONSTRAINT api_call_log_pkey PRIMARY KEY (id, called_at)
);

SELECT create_hypertable(
    'api_call_log', 'called_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS api_call_log_source_idx ON api_call_log (source, called_at DESC);

-- ─── CKW Tariffs (15-min raw) ─────────────────────────────────────────────────
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

-- ─── Groupe E Tariffs (15-min raw) ────────────────────────────────────────────
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

-- ─── Continuous Aggregate: CKW 15min → 1h ─────────────────────────────────────
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

-- ─── Continuous Aggregate: Groupe E 15min → 1h ────────────────────────────────
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

-- ─── Migration: add precipitation column (idempotent) ─────────────────────────
ALTER TABLE weather_hourly ADD COLUMN IF NOT EXISTS precipitation_mm DOUBLE PRECISION;

-- ─── Feature View: training_features ──────────────────────────────────────────
-- DROP first: CREATE OR REPLACE VIEW cannot rename existing columns.
DROP VIEW IF EXISTS training_features CASCADE;
-- Primary tariff signal:  Groupe E 'integrated' (stdev≈0.076, highest variability)
-- Secondary tariff signal: CKW 'integrated' (regional price differential)
-- EKZ removed (flat rate, stdev≈0, not useful as price signal).
-- Weather: now includes precipitation_mm (Niederschlag – req.md Phase 1).

CREATE OR REPLACE VIEW training_features AS
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
      pf.lag_1h,
      pf.lag_2h,
      pf.lag_24h,
      pf.lag_168h,
      pf.rolling_avg_24h,
      pf.rolling_avg_7d,
      pf.hour_of_day,
      pf.day_of_week,
      pf.month,
      pf.is_weekend,
      pf.is_peak_hour,
      w.temperature_2m,
      w.wind_speed_10m,
      w.shortwave_radiation,
      w.cloud_cover,
      w.precipitation_mm,
      AVG(w.temperature_2m) OVER (
        ORDER BY pf.time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
      ) AS temp_rolling_avg_24h,
      bh.discharge_m3s,
      bh.level_masl,
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

-- ─── Winterthur Load (OGD Bruttolastgang) ─────────────────────────────────────
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

-- ─── Winterthur PV Feed-in (OGD Netzeinspeisung) ──────────────────────────────
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

-- ─── Feature View: winterthur_net_load_features (Model A) ─────────────────────
DROP VIEW IF EXISTS winterthur_net_load_features CASCADE;
CREATE VIEW winterthur_net_load_features AS
SELECT
    w.time,
    w.load_kwh - COALESCE(p.pv_kwh, 0)               AS net_load_kwh,
    EXTRACT(HOUR    FROM w.time)::INT                 AS hour_of_day,
    EXTRACT(DOW     FROM w.time)::INT                 AS day_of_week,
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
    wr.wind_speed_10m,
    wr.shortwave_radiation,
    wr.cloud_cover,
    wr.precipitation_mm,
    COALESCE(p.pv_kwh, 0)                            AS pv_feed_in_kwh
FROM winterthur_load w
LEFT JOIN winterthur_pv p USING (time)
LEFT JOIN weather_hourly wr
    ON  date_trunc('hour', w.time) = wr.time
    AND wr.latitude  = 47.5001
    AND wr.longitude = 8.7502;
