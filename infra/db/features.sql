-- BigDataSmallPrice – Feature View
-- Standalone SQL for the training_features view.
-- This file is also appended to infra/db/init.sql so it runs on first container start.
-- To re-apply on a running DB: psql -U bdsp -d bdsp -f infra/db/features.sql

-- ─── Feature View: training_features ─────────────────────────────────────────
-- Joins ENTSO-E, Weather, BAFU and EKZ into one hourly feature table.
-- Includes lag features, rolling averages, and calendar features.
-- All rows use the ENTSO-E Swiss grid (10YCH-SWISSGRIDZ) as the time base.

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
      -- Lag features (assumes hourly, consecutive rows = consecutive hours)
      LAG(price_eur_mwh, 1)   OVER (ORDER BY time) AS lag_1h,
      LAG(price_eur_mwh, 2)   OVER (ORDER BY time) AS lag_2h,
      LAG(price_eur_mwh, 24)  OVER (ORDER BY time) AS lag_24h,
      LAG(price_eur_mwh, 168) OVER (ORDER BY time) AS lag_168h,
      -- Rolling averages over price
      AVG(price_eur_mwh) OVER (
        ORDER BY time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
      ) AS rolling_avg_24h,
      AVG(price_eur_mwh) OVER (
        ORDER BY time ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
      ) AS rolling_avg_7d,
      -- Calendar features
      EXTRACT(hour  FROM time)::INT                                          AS hour_of_day,
      EXTRACT(dow   FROM time)::INT                                          AS day_of_week,
      EXTRACT(month FROM time)::INT                                          AS month,
      CASE WHEN EXTRACT(dow  FROM time) IN (0, 6)       THEN 1 ELSE 0 END   AS is_weekend,
      CASE WHEN EXTRACT(hour FROM time) BETWEEN 7 AND 22 THEN 1 ELSE 0 END  AS is_peak_hour
    FROM entsoe_day_ahead_prices
    WHERE domain = '10YCH-SWISSGRIDZ'
  ),

  -- Join weather, BAFU, EKZ; compute temperature rolling average after join
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
