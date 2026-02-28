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
    time                TIMESTAMPTZ     NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    temperature_2m      DOUBLE PRECISION,
    wind_speed_10m      DOUBLE PRECISION,
    shortwave_radiation DOUBLE PRECISION,
    cloud_cover         DOUBLE PRECISION
);

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

-- ─── 6. Feature View: training_features (Phase 2) ────────────────────────────
-- Joins ENTSO-E, Weather, BAFU and EKZ into one hourly feature table.
-- Includes lag features, rolling averages, and calendar features.

CREATE OR REPLACE VIEW training_features AS
WITH
  -- EKZ: average across all tariff types per hour
  ekz_avg AS (
    SELECT
      hour,
      AVG(price_chf_kwh_avg) AS ekz_price_chf_kwh_avg
    FROM ekz_tariffs_hourly
    GROUP BY hour
  ),

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

  -- Join weather, BAFU, EKZ; add temperature rolling average
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
      AVG(w.temperature_2m) OVER (
        ORDER BY pf.time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
      ) AS temp_rolling_avg_24h,
      bh.discharge_m3s,
      bh.level_masl,
      ekz.ekz_price_chf_kwh_avg
    FROM price_features pf
    LEFT JOIN weather_hourly w
      ON  w.time      = pf.time
      AND w.latitude  = 47.5001
      AND w.longitude = 8.7502
    LEFT JOIN bafu_hydro bh
      ON  bh.time       = pf.time
      AND bh.station_id = '2018'
    LEFT JOIN ekz_avg ekz
      ON ekz.hour = pf.time
  )

SELECT * FROM joined;
