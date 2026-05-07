import re

with open("src/api/main.py", "r") as f:
    content = f.read()

# The replacement for forecast_week function
new_forecast_week = '''@app.get("/api/forecast/week")
def forecast_week():
    """
    Return a 7-day price forecast in 15-min intervals (672 points).
    """
    import datetime as _dt
    import math as _math
    from modelling.predict import predict_from_dict
    from processing.export_pipeline import FEATURE_COLS, LOAD_FEATURE_COLS
    from processing.tariff_formulas import compute_tariff, energiepreis as _ep, gesamttarif as _gt, DEFAULT_NETZ_STANDARD

    HORIZON_DAYS = 7
    SLOT_MIN     = 15
    SLOTS_PER_DAY = 96

    conn = None
    try:
        conn = _connect()

        # ── 1. Forecast window ──
        now = _dt.datetime.now(_dt.timezone.utc).replace(second=0, microsecond=0)
        rem = now.minute % SLOT_MIN
        forecast_start = now + _dt.timedelta(minutes=(SLOT_MIN - rem) % SLOT_MIN or SLOT_MIN)
        forecast_start = forecast_start.replace(second=0, microsecond=0)
        n_slots    = HORIZON_DAYS * SLOTS_PER_DAY   # 672
        future_ts  = [forecast_start + _dt.timedelta(minutes=SLOT_MIN * i) for i in range(n_slots)]
        forecast_end = future_ts[-1] + _dt.timedelta(minutes=SLOT_MIN)

        # ── 2. Historical energy features ──
        hist_start = forecast_start - _dt.timedelta(days=14, hours=1)
        df_e = pd.read_sql(
            "SELECT * FROM training_features WHERE time >= %s AND time < %s ORDER BY time",
            conn, params=(hist_start, forecast_start), parse_dates=["time"],
        )
        if not df_e.empty:
            df_e = df_e.drop_duplicates(subset=["time"], keep="last")
            if df_e["time"].dt.tz is None:
                df_e["time"] = df_e["time"].dt.tz_localize("UTC")
            df_e = df_e.set_index("time")

        # ── 3. Historical net load ──
        df_nl: pd.DataFrame = pd.DataFrame()
        try:
            df_nl = pd.read_sql(
                "SELECT time, net_load_kwh, load_rolling_avg_24h FROM winterthur_net_load_features "
                "WHERE time >= %s AND time < %s ORDER BY time",
                conn, params=(hist_start, forecast_start), parse_dates=["time"],
            )
            if not df_nl.empty:
                df_nl = df_nl.drop_duplicates(subset=["time"], keep="last")
                if df_nl["time"].dt.tz is None:
                    df_nl["time"] = df_nl["time"].dt.tz_localize("UTC")
                df_nl = df_nl.set_index("time")
        except Exception:
            pass

        # ── 4. Future weather ──
        df_wf: pd.DataFrame = pd.DataFrame()
        try:
            df_wf = pd.read_sql(
                "SELECT time, temperature_2m, wind_speed_10m, shortwave_radiation, "
                "cloud_cover, precipitation_mm FROM weather_hourly "
                "WHERE time >= %s AND time <= %s "
                "AND latitude = 47.5001 AND longitude = 8.7502 ORDER BY time",
                conn, params=(forecast_start, forecast_end), parse_dates=["time"],
            )
            if not df_wf.empty:
                df_wf = df_wf.drop_duplicates(subset=["time"], keep="last")
                if df_wf["time"].dt.tz is None:
                    df_wf["time"] = df_wf["time"].dt.tz_localize("UTC")
                df_wf = df_wf.set_index("time")
        except Exception:
            pass
            
        weather_fut: dict = {} if df_wf.empty else df_wf.to_dict("index")

        # ── 5. Setup Models & Buffers ──
        energy_prefix = _best_prefix("energy", "xgb")
        load_prefix   = _best_prefix("load", "model_load")
        model_b       = _get_model(energy_prefix)
        model_a       = _get_model(load_prefix) if load_prefix else None

        epex_preds = []
        net_load_preds = []
        pred_buffer: dict[_dt.datetime, float] = {}
        load_buffer: dict[_dt.datetime, float] = {}

        def _hist_price(ts: _dt.datetime) -> float:
            if ts in pred_buffer:
                return pred_buffer[ts]
            ts_h = ts.replace(minute=0, second=0, microsecond=0)
            if df_e.empty or ts_h not in df_e.index:
                return float("nan")
            v = df_e.loc[ts_h, "price_eur_mwh"] if "price_eur_mwh" in df_e.columns else float("nan")
            return float(v)

        def _hist_load(ts: _dt.datetime) -> float:
            if ts in load_buffer:
                return load_buffer[ts]
            ts_h = ts.replace(minute=0, second=0, microsecond=0)
            if df_nl.empty or ts_h not in df_nl.index:
                return float("nan")
            v = df_nl.loc[ts_h, "net_load_kwh"] if "net_load_kwh" in df_nl.columns else float("nan")
            return float(v)

        # ── 6. Iterative Autoregressive Prediction ──
        for t in future_ts:
            t_168 = t - _dt.timedelta(hours=168)
            t_24  = t - _dt.timedelta(hours=24)
            t_1h  = t - _dt.timedelta(hours=1)
            t_h   = t.replace(minute=0, second=0, microsecond=0)
            t_168_h = t_168.replace(minute=0, second=0, microsecond=0)

            # ── Energy (Model B) row ──
            if not df_e.empty and t_168_h in df_e.index:
                e_tmpl = df_e.loc[t_168_h].to_dict()
            elif not df_e.empty:
                e_tmpl = df_e.iloc[-1].to_dict()
            else:
                e_tmpl = {}
            e_row = dict(e_tmpl)

            e_row["lag_168h"] = _hist_price(t_168)
            e_row["lag_24h"]  = _hist_price(t_24)
            e_row["lag_1h"]   = _hist_price(t_1h)

            e_row["hour_of_day"]  = t.hour
            e_row["day_of_week"]  = t.weekday()
            e_row["month"]        = t.month
            e_row["is_weekend"]   = int(t.weekday() >= 5)
            e_row["is_peak_hour"] = int(7 <= t.hour <= 22)
            e_row["hour_sin"]  = _math.sin(2 * _math.pi * t.hour / 24)
            e_row["hour_cos"]  = _math.cos(2 * _math.pi * t.hour / 24)
            e_row["dow_sin"]   = _math.sin(2 * _math.pi * t.weekday() / 7)
            e_row["dow_cos"]   = _math.cos(2 * _math.pi * t.weekday() / 7)
            e_row["month_sin"] = _math.sin(2 * _math.pi * (t.month - 1) / 12)
            e_row["month_cos"] = _math.cos(2 * _math.pi * (t.month - 1) / 12)

            if t_h in weather_fut:
                w = weather_fut[t_h]
                for key in ("temperature_2m", "wind_speed_10m", "shortwave_radiation",
                            "cloud_cover", "precipitation_mm"):
                    v = w.get(key)
                    if v is not None:
                        e_row[key] = float(v)

            epex = predict_from_dict(model_b, e_row)
            epex_preds.append(epex)
            pred_buffer[t] = float(epex)

            # ── Load (Model A) row ──
            if not df_nl.empty and t_168_h in df_nl.index:
                l_tmpl = df_nl.loc[t_168_h].to_dict()
            elif not df_nl.empty:
                l_tmpl = df_nl.iloc[-1].to_dict()
            else:
                l_tmpl = {}
            l_row = dict(l_tmpl)

            load_lag_7d = _hist_load(t_168)
            load_lag_1d = _hist_load(t_24)
            
            if not df_nl.empty and t_168_h in df_nl.index and "load_rolling_avg_24h" in df_nl.columns:
                roll_avg = float(df_nl.loc[t_168_h, "load_rolling_avg_24h"])
            elif not df_nl.empty and "load_rolling_avg_24h" in df_nl.columns:
                roll_avg = float(df_nl.iloc[-1]["load_rolling_avg_24h"])
            else:
                roll_avg = float("nan")

            temp_c   = e_row.get("temperature_2m", float("nan"))
            temp_avg = e_row.get("temp_rolling_avg_24h", temp_c)
            temp_dev = (temp_c - temp_avg) if not (_math.isnan(temp_c) or _math.isnan(temp_avg)) else float("nan")

            l_row.update({
                "load_lag_1h":          _hist_load(t_1h),
                "load_lag_1d":          load_lag_1d,
                "load_lag_7d":          load_lag_7d,
                "load_rolling_avg_24h": roll_avg,
                "hour":                 t.hour,
                "weekday":              t.weekday(),
                "month":                t.month,
                "quarter":              (t.month - 1) // 3 + 1,
                "is_weekend":           int(t.weekday() >= 5),
                "is_holiday_zh":        0,
                "is_school_holiday":    0,
                "temp_c":               temp_c,
                "wind_speed_ms":        e_row.get("wind_speed_10m", float("nan")),
                "ghi_wm2":              e_row.get("shortwave_radiation", float("nan")),
                "cloud_cover_pct":      e_row.get("cloud_cover", float("nan")),
                "precipitation_mm":     e_row.get("precipitation_mm", float("nan")),
                "temp_deviation":       temp_dev,
                "pv_feed_in":           float("nan"),
            })

            if model_a is not None:
                ld = predict_from_dict(model_a, l_row)
                net_load_preds.append(ld)
                load_buffer[t] = float(ld)
            else:
                net_load_preds.append(None)

        # ── 7. Tariff formulas ──
        sigma_eur  = _residual_std("energy")
        sigma_load = _residual_std("load")
        ci_active  = sigma_eur is not None

        netz_arr, energie_arr, gesamt_arr = [], [], []
        gesamt_lo_arr: list[float | None] = []
        gesamt_hi_arr: list[float | None] = []
        net_load_lo_arr: list[float | None] = []
        net_load_hi_arr: list[float | None] = []
        epex_lo_arr: list[float | None] = []
        epex_hi_arr: list[float | None] = []
        
        for epex, load in zip(epex_preds, net_load_preds):
            if load is not None:
                tf      = compute_tariff(net_load=load, epex_eur_mwh=epex)
                netz    = tf["netzpreis_rp_kwh"]
                energie = tf["energiepreis_rp_kwh"]
                gesamt  = tf["gesamttarif_rp_kwh"]
            else:
                energie = round(_ep(epex), 2)
                netz    = round(DEFAULT_NETZ_STANDARD, 2)
                gesamt  = round(_gt(netz, energie), 2)
            netz_arr.append(netz)
            energie_arr.append(energie)
            gesamt_arr.append(gesamt)

            if ci_active:
                half = _CI_Z * sigma_eur
                e_lo = _ep(epex - half)
                e_hi = _ep(epex + half)
                gesamt_lo_arr.append(round(_gt(netz, e_lo), 2))
                gesamt_hi_arr.append(round(_gt(netz, e_hi), 2))
                epex_lo_arr.append(round(epex - half, 2))
                epex_hi_arr.append(round(epex + half, 2))
            else:
                gesamt_lo_arr.append(None)
                gesamt_hi_arr.append(None)
                epex_lo_arr.append(None)
                epex_hi_arr.append(None)

            if load is not None and sigma_load is not None:
                half_l = _CI_Z * sigma_load
                net_load_lo_arr.append(round(load - half_l, 2))
                net_load_hi_arr.append(round(load + half_l, 2))
            else:
                net_load_lo_arr.append(None)
                net_load_hi_arr.append(None)

        # ── 8. Traffic-light ──
        sorted_g = sorted(gesamt_arr)
        q_lo = sorted_g[int(0.33 * (n_slots - 1))]
        q_hi = sorted_g[int(0.67 * (n_slots - 1))]
        level_arr = [
            "low" if g <= q_lo else ("high" if g >= q_hi else "medium")
            for g in gesamt_arr
        ]

        # ── 9. Cheapest windows ──
        WIN_SLOTS = 8  # 2 h × 4 slots/h
        scored = sorted(
            (sum(gesamt_arr[i : i + WIN_SLOTS]) / WIN_SLOTS, i)
            for i in range(n_slots - WIN_SLOTS)
        )
        cheap_windows: list[dict] = []
        used_slots: set[int] = set()
        for avg_p, si in scored:
            sl = set(range(si, si + WIN_SLOTS))
            if sl & used_slots:
                continue
            used_slots |= sl
            cheap_windows.append({
                "start":           future_ts[si].isoformat(),
                "end":             (future_ts[si] + _dt.timedelta(hours=2)).isoformat(),
                "avg_gesamttarif": round(avg_p, 2),
            })
            if len(cheap_windows) >= 6:
                break

        return {
            "generated_at":       now.isoformat(),
            "horizon_days":       HORIZON_DAYS,
            "n_points":           n_slots,
            "times":              [t.isoformat() for t in future_ts],
            "epex_eur_mwh":       [round(p, 2) for p in epex_preds],
            "netzpreis":          netz_arr,
            "energiepreis":       energie_arr,
            "gesamttarif":        gesamt_arr,
            "price_level":        level_arr,
            "cheapest_windows":   cheap_windows,
            "net_load_available": net_load_preds[0] is not None,
            "model_energy":       energy_prefix,
            "model_load":         load_prefix if net_load_preds[0] is not None else None,
            "ci_available":       ci_active,
            "gesamttarif_ci_lower": gesamt_lo_arr,
            "gesamttarif_ci_upper": gesamt_hi_arr,
            "net_load_ci_lower":    net_load_lo_arr,
            "net_load_ci_upper":    net_load_hi_arr,
            "epex_ci_lower":        epex_lo_arr,
            "epex_ci_upper":        epex_hi_arr,
            "level_thresholds":     {"low_max": round(q_lo, 2), "high_min": round(q_hi, 2)},
        }

    except FileNotFoundError:
        return JSONResponse(
            {"error": "No trained model available. Run the training pipeline first."},
            status_code=503,
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    finally:
        if conn is not None:
            conn.close()
'''

# Use regex to find and replace the function body
# Find from `@app.get("/api/forecast/week")` up to (but not including) `@app.get("/api/price-history")`
pattern = r'@app\.get\("/api/forecast/week"\).*?(?=@app\.get\("/api/price-history"\))'

new_content = re.sub(pattern, new_forecast_week + "\n\n", content, flags=re.DOTALL)

with open("src/api/main.py", "w") as f:
    f.write(new_content)

