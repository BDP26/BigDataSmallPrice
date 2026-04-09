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

-- ─── ENTSO-E Actual Total Load CH (A65) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS entsoe_actual_load (
    time      TIMESTAMPTZ      NOT NULL,
    domain    TEXT             NOT NULL,
    load_mwh  DOUBLE PRECISION
);

SELECT create_hypertable(
    'entsoe_actual_load', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS entsoe_actual_load_time_domain_idx
    ON entsoe_actual_load (time, domain);

-- ─── ENTSO-E Generation Per Type (A75) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS entsoe_generation (
    time          TIMESTAMPTZ      NOT NULL,
    domain        TEXT             NOT NULL,
    psr_type      TEXT             NOT NULL,
    quantity_mwh  DOUBLE PRECISION
);
SELECT create_hypertable('entsoe_generation', 'time',
    chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS entsoe_generation_time_domain_psr_idx
    ON entsoe_generation (time, domain, psr_type);

-- ─── ENTSO-E Cross-Border Physical Flows (A11) ────────────────────────────────
CREATE TABLE IF NOT EXISTS entsoe_crossborder_flows (
    time       TIMESTAMPTZ      NOT NULL,
    in_domain  TEXT             NOT NULL,
    out_domain TEXT             NOT NULL,
    flow_mwh   DOUBLE PRECISION
);
SELECT create_hypertable('entsoe_crossborder_flows', 'time',
    chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS entsoe_crossborder_flows_time_domains_idx
    ON entsoe_crossborder_flows (time, in_domain, out_domain);

-- ─── ENTSO-E Day-Ahead Load Forecast (A65/A01) ───────────────────────────────
CREATE TABLE IF NOT EXISTS entsoe_load_forecast (
    time     TIMESTAMPTZ      NOT NULL,
    domain   TEXT             NOT NULL,
    load_mwh DOUBLE PRECISION
);
SELECT create_hypertable('entsoe_load_forecast', 'time',
    chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS entsoe_load_forecast_time_domain_idx
    ON entsoe_load_forecast (time, domain);

-- ─── Feature View: training_features (Model B – EPEX Day-Ahead) ───────────────
-- Target: price_eur_mwh
-- Design decisions:
--   - No Groupe E / CKW features: they are EPEX transformations → near-leakage,
--     and limit training window to ~3 months (API only since Jan 2026).
--   - No BAFU: single river gauge, negligible signal for EU-wide market pricing.
--   - ENTSO-E actual data (generation, load, flows) used ONLY as lag features
--     (lag_24h for D+1, lag_168h for D+7) – never as current-time values.
--   - Weather from 3 locations: CH, DE-Nord (wind proxy), DE-Süd (solar proxy).
DROP VIEW IF EXISTS training_features CASCADE;
CREATE VIEW training_features AS
WITH
  price_features AS (
    SELECT
      time,
      price_eur_mwh,
      LAG(price_eur_mwh, 1)   OVER (ORDER BY time) AS lag_1h,
      LAG(price_eur_mwh, 24)  OVER (ORDER BY time) AS lag_24h,
      LAG(price_eur_mwh, 168) OVER (ORDER BY time) AS lag_168h,
      AVG(price_eur_mwh) OVER (
        ORDER BY time ROWS BETWEEN 23  PRECEDING AND CURRENT ROW
      ) AS rolling_avg_24h,
      AVG(price_eur_mwh) OVER (
        ORDER BY time ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
      ) AS rolling_avg_7d,
      EXTRACT(hour  FROM time)::INT                                          AS hour_of_day,
      EXTRACT(dow   FROM time)::INT                                          AS day_of_week,
      EXTRACT(month FROM time)::INT                                          AS month,
      CASE WHEN EXTRACT(dow  FROM time) IN (0, 6)        THEN 1 ELSE 0 END  AS is_weekend,
      CASE WHEN EXTRACT(hour FROM time) BETWEEN 7 AND 22 THEN 1 ELSE 0 END  AS is_peak_hour
    FROM entsoe_day_ahead_prices
    WHERE domain = '10YCH-SWISSGRIDZ'
  ),

  -- CH hydro run-of-river (A75/B12) – lag only, never current value
  gen_b12 AS (
    SELECT time,
      LAG(quantity_mwh, 24)  OVER (ORDER BY time) AS hydro_ror_ch_lag_24h,
      LAG(quantity_mwh, 168) OVER (ORDER BY time) AS hydro_ror_ch_lag_168h
    FROM entsoe_generation
    WHERE domain = '10YCH-SWISSGRIDZ' AND psr_type = 'B12'
  ),

  -- CH solar generation (A75/B16) – lag only
  gen_b16 AS (
    SELECT time,
      LAG(quantity_mwh, 24)  OVER (ORDER BY time) AS solar_gen_ch_lag_24h,
      LAG(quantity_mwh, 168) OVER (ORDER BY time) AS solar_gen_ch_lag_168h
    FROM entsoe_generation
    WHERE domain = '10YCH-SWISSGRIDZ' AND psr_type = 'B16'
  ),

  -- DE wind generation (A75/B19) – strongest external price driver – lag only
  gen_de_b19 AS (
    SELECT time,
      LAG(quantity_mwh, 24)  OVER (ORDER BY time) AS wind_gen_de_lag_24h,
      LAG(quantity_mwh, 168) OVER (ORDER BY time) AS wind_gen_de_lag_168h
    FROM entsoe_generation
    WHERE domain = '10Y1001A1001A83F' AND psr_type = 'B19'
  ),

  -- CH actual load (A65/A16) – lag only
  actual_load AS (
    SELECT time,
      LAG(load_mwh, 24)  OVER (ORDER BY time) AS actual_load_ch_lag_24h,
      LAG(load_mwh, 168) OVER (ORDER BY time) AS actual_load_ch_lag_168h
    FROM entsoe_actual_load
    WHERE domain = '10YCH-SWISSGRIDZ'
  ),

  -- CH net position (imports − exports, all 4 borders) – lag only
  net_pos_raw AS (
    SELECT
      time,
      SUM(CASE WHEN out_domain = '10YCH-SWISSGRIDZ' THEN  flow_mwh ELSE 0 END)
    - SUM(CASE WHEN in_domain  = '10YCH-SWISSGRIDZ' THEN  flow_mwh ELSE 0 END)
        AS net_position_ch
    FROM entsoe_crossborder_flows
    WHERE in_domain = '10YCH-SWISSGRIDZ' OR out_domain = '10YCH-SWISSGRIDZ'
    GROUP BY time
  ),
  net_pos AS (
    SELECT time,
      LAG(net_position_ch, 24)  OVER (ORDER BY time) AS net_position_ch_lag_24h,
      LAG(net_position_ch, 168) OVER (ORDER BY time) AS net_position_ch_lag_168h
    FROM net_pos_raw
  )

SELECT
  pf.time,
  pf.price_eur_mwh,
  -- EPEX price lags
  pf.lag_1h,
  pf.lag_24h,
  pf.lag_168h,
  pf.rolling_avg_24h,
  pf.rolling_avg_7d,
  -- Calendar
  pf.hour_of_day,
  pf.day_of_week,
  pf.month,
  pf.is_weekend,
  pf.is_peak_hour,
  -- Weather CH (Winterthur, 47.5001 / 8.7502)
  w_ch.temperature_2m,
  w_ch.wind_speed_10m,
  w_ch.shortwave_radiation,
  w_ch.cloud_cover,
  w_ch.precipitation_mm,
  AVG(w_ch.temperature_2m) OVER (
    ORDER BY pf.time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
  ) AS temp_rolling_avg_24h,
  -- Weather DE-Nord (Hamburg region, 53.5 / 10.0 – DE wind generation proxy)
  w_de_n.wind_speed_10m      AS wind_speed_de_nord,
  w_de_n.shortwave_radiation AS solar_de_nord,
  -- Weather DE-Süd (Stuttgart region, 48.5 / 9.0 – DE solar generation proxy)
  w_de_s.shortwave_radiation AS solar_de_sued,
  w_de_s.wind_speed_10m      AS wind_speed_de_sued,
  -- ENTSO-E generation lags (actual values offset by 24 h or 168 h)
  gl_b12.hydro_ror_ch_lag_24h,
  gl_b12.hydro_ror_ch_lag_168h,
  gl_b16.solar_gen_ch_lag_24h,
  gl_b16.solar_gen_ch_lag_168h,
  gl_b19.wind_gen_de_lag_24h,
  gl_b19.wind_gen_de_lag_168h,
  -- ENTSO-E actual load lags
  al.actual_load_ch_lag_24h,
  al.actual_load_ch_lag_168h,
  -- CH net position lags
  np.net_position_ch_lag_24h,
  np.net_position_ch_lag_168h,
  -- ENTSO-E load forecast (published day-ahead – valid for D+1 use only)
  lf.load_mwh AS load_forecast_ch
FROM price_features pf
LEFT JOIN weather_hourly w_ch
  ON  w_ch.time      = pf.time
  AND w_ch.latitude  = 47.5001
  AND w_ch.longitude = 8.7502
LEFT JOIN weather_hourly w_de_n
  ON  w_de_n.time      = pf.time
  AND w_de_n.latitude  = 53.5
  AND w_de_n.longitude = 10.0
LEFT JOIN weather_hourly w_de_s
  ON  w_de_s.time      = pf.time
  AND w_de_s.latitude  = 48.5
  AND w_de_s.longitude = 9.0
LEFT JOIN gen_b12    gl_b12 ON gl_b12.time = pf.time
LEFT JOIN gen_b16    gl_b16 ON gl_b16.time = pf.time
LEFT JOIN gen_de_b19 gl_b19 ON gl_b19.time = pf.time
LEFT JOIN actual_load   al  ON al.time     = pf.time
LEFT JOIN net_pos       np  ON np.time     = pf.time
LEFT JOIN entsoe_load_forecast lf
  ON  lf.time   = pf.time
  AND lf.domain = '10YCH-SWISSGRIDZ';

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
