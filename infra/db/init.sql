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
