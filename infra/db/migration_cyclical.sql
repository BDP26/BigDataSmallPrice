-- Cyclical Encoding Migration
-- Adds sin/cos features for time-based cyclical encoding
-- Safe: Recreates view with new calculated columns, preserves all existing columns
-- Date: 2026-04-09

-- Step 1: Backup current view definition (already done in separate file)
-- View will be dropped and recreated with cyclical features

-- Step 2: Drop existing view
DROP VIEW IF EXISTS training_features;

-- Step 3: Create new view with cyclical features
-- NOTE: New features inserted AFTER existing features, preserving column order for existing columns
CREATE VIEW training_features AS
WITH price_features AS (
    SELECT entsoe_day_ahead_prices."time",
       entsoe_day_ahead_prices.price_eur_mwh,
       lag(entsoe_day_ahead_prices.price_eur_mwh, 1) OVER (ORDER BY entsoe_day_ahead_prices."time") AS lag_1h,
       lag(entsoe_day_ahead_prices.price_eur_mwh, 2) OVER (ORDER BY entsoe_day_ahead_prices."time") AS lag_2h,
       lag(entsoe_day_ahead_prices.price_eur_mwh, 24) OVER (ORDER BY entsoe_day_ahead_prices."time") AS lag_24h,
       lag(entsoe_day_ahead_prices.price_eur_mwh, 168) OVER (ORDER BY entsoe_day_ahead_prices."time") AS lag_168h,
       avg(entsoe_day_ahead_prices.price_eur_mwh) OVER (ORDER BY entsoe_day_ahead_prices."time" ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_avg_24h,
       avg(entsoe_day_ahead_prices.price_eur_mwh) OVER (ORDER BY entsoe_day_ahead_prices."time" ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) AS rolling_avg_7d,
       EXTRACT(hour FROM entsoe_day_ahead_prices."time")::integer AS hour_of_day,
       EXTRACT(dow FROM entsoe_day_ahead_prices."time")::integer AS day_of_week,
       EXTRACT(month FROM entsoe_day_ahead_prices."time")::integer AS month,
           CASE
               WHEN EXTRACT(dow FROM entsoe_day_ahead_prices."time") = ANY (ARRAY[0::numeric, 6::numeric]) THEN 1
               ELSE 0
           END AS is_weekend,
           CASE
               WHEN EXTRACT(hour FROM entsoe_day_ahead_prices."time") >= 7::numeric AND EXTRACT(hour FROM entsoe_day_ahead_prices."time") <= 22::numeric THEN 1
               ELSE 0
           END AS is_peak_hour
      FROM entsoe_day_ahead_prices
     WHERE entsoe_day_ahead_prices.domain = '10YCH-SWISSGRIDZ'::text
), joined AS (
    SELECT pf."time",
       pf.price_eur_mwh,
       pf.price_eur_mwh AS epex_t,
       pf.lag_1h,
       pf.lag_2h,
       pf.lag_24h,
       pf.lag_168h,
       pf.lag_24h AS epex_lag_1d,
       pf.lag_168h AS epex_lag_7d,
       pf.rolling_avg_24h,
       pf.rolling_avg_7d,
       pf.hour_of_day,
       pf.day_of_week,
       pf.month,
       pf.is_weekend,
       pf.is_peak_hour,
       -- NEW: Cyclical encoding features (inserted after existing columns)
       SIN(2 * PI() * pf.hour_of_day / 24.0)::double precision AS hour_sin,
       COS(2 * PI() * pf.hour_of_day / 24.0)::double precision AS hour_cos,
       SIN(2 * PI() * pf.day_of_week / 7.0)::double precision AS dow_sin,
       COS(2 * PI() * pf.day_of_week / 7.0)::double precision AS dow_cos,
       SIN(2 * PI() * (pf.month - 1) / 12.0)::double precision AS month_sin,
       COS(2 * PI() * (pf.month - 1) / 12.0)::double precision AS month_cos,
       w.temperature_2m,
       w.temperature_2m AS temp_ch_avg,
       w.wind_speed_10m,
       w.wind_speed_10m AS wind_generation_eu,
       w.shortwave_radiation,
       gen_b16.quantity_mwh AS solar_generation_ch,
       w.cloud_cover,
       w.precipitation_mm,
       avg(w.temperature_2m) OVER (ORDER BY pf."time" ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS temp_rolling_avg_24h,
       bh.discharge_m3s,
       bh.level_masl,
       bh.level_masl AS hydro_reservoir,
           CASE
               WHEN pf.is_weekend = 1 THEN 'weekend'::text
               ELSE 'workday'::text
           END AS day_type,
       ge.avg_chf_kwh AS tariff_price_chf_kwh_avg,
       ck.avg_chf_kwh AS ckw_price_chf_kwh_avg,
       gen_b12.quantity_mwh AS hydro_run_of_river_ch,
       gen_de_b19.quantity_mwh AS wind_generation_de,
       f_ch_de.flow_mwh AS flow_ch_de,
       f_ch_it.flow_mwh AS flow_ch_it,
       f_ch_fr.flow_mwh AS flow_ch_fr,
       f_ch_at.flow_mwh AS flow_ch_at,
       COALESCE(f_de_ch.flow_mwh, 0::double precision) + COALESCE(f_it_ch.flow_mwh, 0::double precision) + COALESCE(f_fr_ch.flow_mwh, 0::double precision) + COALESCE(f_at_ch.flow_mwh, 0::double precision) - (COALESCE(f_ch_de.flow_mwh, 0::double precision) + COALESCE(f_ch_it.flow_mwh, 0::double precision) + COALESCE(f_ch_fr.flow_mwh, 0::double precision) + COALESCE(f_ch_at.flow_mwh, 0::double precision)) AS net_position_ch,
       lf.load_mwh AS load_forecast_ch,
       al.load_mwh AS actual_load_ch_mwh
      FROM price_features pf
        LEFT JOIN weather_hourly w ON w."time" = pf."time" AND w.latitude = 47.5001::double precision AND w.longitude = 8.7502::double precision
        LEFT JOIN bafu_hydro bh ON bh."time" = pf."time" AND bh.station_id = '2018'::text
        LEFT JOIN groupe_e_tariffs_hourly ge ON ge.hour = pf."time" AND ge.tariff_type = 'integrated'::text
        LEFT JOIN ckw_tariffs_hourly ck ON ck.hour = pf."time" AND ck.tariff_type = 'integrated'::text
        LEFT JOIN entsoe_generation gen_b12 ON gen_b12."time" = pf."time" AND gen_b12.domain = '10YCH-SWISSGRIDZ'::text AND gen_b12.psr_type = 'B12'::text
        LEFT JOIN entsoe_generation gen_b16 ON gen_b16."time" = pf."time" AND gen_b16.domain = '10YCH-SWISSGRIDZ'::text AND gen_b16.psr_type = 'B16'::text
        LEFT JOIN entsoe_generation gen_de_b19 ON gen_de_b19."time" = pf."time" AND gen_de_b19.domain = '10Y1001A1001A83F'::text AND gen_de_b19.psr_type = 'B19'::text
        LEFT JOIN entsoe_crossborder_flows f_ch_de ON f_ch_de."time" = pf."time" AND f_ch_de.in_domain = '10YCH-SWISSGRIDZ'::text AND f_ch_de.out_domain = '10Y1001A1001A83F'::text
        LEFT JOIN entsoe_crossborder_flows f_de_ch ON f_de_ch."time" = pf."time" AND f_de_ch.in_domain = '10Y1001A1001A83F'::text AND f_de_ch.out_domain = '10YCH-SWISSGRIDZ'::text
        LEFT JOIN entsoe_crossborder_flows f_ch_it ON f_ch_it."time" = pf."time" AND f_ch_it.in_domain = '10YCH-SWISSGRIDZ'::text AND f_ch_it.out_domain = '10YIT-GRTN-----B'::text
        LEFT JOIN entsoe_crossborder_flows f_it_ch ON f_it_ch."time" = pf."time" AND f_it_ch.in_domain = '10YIT-GRTN-----B'::text AND f_it_ch.out_domain = '10YCH-SWISSGRIDZ'::text
        LEFT JOIN entsoe_crossborder_flows f_ch_fr ON f_ch_fr."time" = pf."time" AND f_ch_fr.in_domain = '10YCH-SWISSGRIDZ'::text AND f_ch_fr.out_domain = '10YFR-RTE------C'::text
        LEFT JOIN entsoe_crossborder_flows f_fr_ch ON f_fr_ch."time" = pf."time" AND f_fr_ch.in_domain = '10YFR-RTE------C'::text AND f_fr_ch.out_domain = '10YCH-SWISSGRIDZ'::text
        LEFT JOIN entsoe_crossborder_flows f_ch_at ON f_ch_at."time" = pf."time" AND f_ch_at.in_domain = '10YCH-SWISSGRIDZ'::text AND f_ch_at.out_domain = '10YAT-APG------L'::text
        LEFT JOIN entsoe_crossborder_flows f_at_ch ON f_at_ch."time" = pf."time" AND f_at_ch.in_domain = '10YAT-APG------L'::text AND f_at_ch.out_domain = '10YCH-SWISSGRIDZ'::text
        LEFT JOIN entsoe_load_forecast lf ON lf."time" = pf."time" AND lf.domain = '10YCH-SWISSGRIDZ'::text
        LEFT JOIN entsoe_actual_load al ON al."time" = pf."time" AND al.domain = '10YCH-SWISSGRIDZ'::text
)
SELECT joined."time",
   joined.price_eur_mwh,
   joined.epex_t,
   joined.lag_1h,
   joined.lag_2h,
   joined.lag_24h,
   joined.lag_168h,
   joined.epex_lag_1d,
   joined.epex_lag_7d,
   joined.rolling_avg_24h,
   joined.rolling_avg_7d,
   joined.hour_of_day,
   joined.day_of_week,
   joined.month,
   joined.is_weekend,
   joined.is_peak_hour,
   -- NEW cyclical features
   joined.hour_sin,
   joined.hour_cos,
   joined.dow_sin,
   joined.dow_cos,
   joined.month_sin,
   joined.month_cos,
   joined.temperature_2m,
   joined.temp_ch_avg,
   joined.wind_speed_10m,
   joined.wind_generation_eu,
   joined.shortwave_radiation,
   joined.solar_generation_ch,
   joined.cloud_cover,
   joined.precipitation_mm,
   joined.temp_rolling_avg_24h,
   joined.discharge_m3s,
   joined.level_masl,
   joined.hydro_reservoir,
   joined.day_type,
   joined.tariff_price_chf_kwh_avg,
   joined.ckw_price_chf_kwh_avg,
   joined.hydro_run_of_river_ch,
   joined.wind_generation_de,
   joined.flow_ch_de,
   joined.flow_ch_it,
   joined.flow_ch_fr,
   joined.flow_ch_at,
   joined.net_position_ch,
   joined.load_forecast_ch,
   joined.actual_load_ch_mwh
  FROM joined;

-- Step 4: Verify the view was recreated with new columns
DO $$
DECLARE
    col_count INTEGER;
    has_hour_sin BOOLEAN;
    has_hour_cos BOOLEAN;
BEGIN
    SELECT COUNT(*) INTO col_count
    FROM information_schema.columns 
    WHERE table_name='training_features';
    
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='training_features' AND column_name='hour_sin'
    ) INTO has_hour_sin;
    
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='training_features' AND column_name='hour_cos'
    ) INTO has_hour_cos;
    
    IF has_hour_sin AND has_hour_cos THEN
        RAISE NOTICE 'SUCCESS: View recreated with cyclical features. Total columns: %', col_count;
    ELSE
        RAISE EXCEPTION 'FAILED: Cyclical columns not found in view';
    END IF;
END $$;

-- Step 5: Sample verification query
SELECT 
    hour_of_day,
    ROUND(hour_sin::numeric, 4) as hour_sin,
    ROUND(hour_cos::numeric, 4) as hour_cos
FROM training_features 
WHERE hour_of_day IN (0, 6, 12, 18, 23)
GROUP BY hour_of_day, hour_sin, hour_cos
ORDER BY hour_of_day;