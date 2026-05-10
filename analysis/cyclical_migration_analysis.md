# Cyclical Encoding Migration Analysis

## Executive Summary

This document analyzes the BDSP feature pipeline to identify all changes required for migrating from linear calendar features (`hour_of_day`, `day_of_week`, `month`) to cyclical encodings (sine/cosine pairs).

**Key Finding**: Migration requires coordinated changes across **4 files**, with **no schema migration system** in place. The current linear features exist in:
- Two SQL views (`training_features` and `winterthur_net_load_features`)
- Two Python feature lists (`FEATURE_COLS` and `LOAD_FEATURE_COLS`)
- Scalers in sequence models will z-score cyclical features (acceptable but suboptimal)

---

## Current State Analysis

### 1. SQL View Definitions

#### File: `/home/john/BigDataSmallPrice/infra/db/features.sql`

**training_features view** (lines 185-189):
```sql
EXTRACT(hour  FROM time)::INT                                          AS hour_of_day,
EXTRACT(dow   FROM time)::INT                                          AS day_of_week,
EXTRACT(month FROM time)::INT                                          AS month,
CASE WHEN EXTRACT(dow  FROM time) IN (0, 6)        THEN 1 ELSE 0 END  AS is_weekend,
CASE WHEN EXTRACT(hour FROM time) BETWEEN 7 AND 22 THEN 1 ELSE 0 END  AS is_peak_hour
```

**winterthur_net_load_features view** (lines 347-352):
```sql
EXTRACT(HOUR    FROM w.time)::INT                 AS hour_of_day,
EXTRACT(HOUR    FROM w.time)::INT                 AS hour,
EXTRACT(DOW     FROM w.time)::INT                 AS day_of_week,
EXTRACT(DOW     FROM w.time)::INT                 AS weekday,
EXTRACT(MONTH   FROM w.time)::INT                 AS month,
EXTRACT(QUARTER FROM w.time)::INT                 AS quarter,
```

**Note**: `winterthur_net_load_features` duplicates `hour` (as both `hour_of_day` and `hour`) and `day_of_week` (as both `day_of_week` and `weekday`).

**Also note**: `/home/john/BigDataSmallPrice/infra/db/init.sql` contains identical view definitions (duplicate schema definitions).

---

### 2. Python Feature Column Lists

#### File: `/home/john/BigDataSmallPrice/src/processing/export_pipeline.py`

**FEATURE_COLS** (Model B - EPEX price forecasting, lines 33-70):
```python
FEATURE_COLS: list[str] = [
    # ...
    # Calendar
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "is_peak_hour",
    # ...
]
```

**LOAD_FEATURE_COLS** (Model A - Winterthur load forecasting, lines 376-402):
```python
LOAD_FEATURE_COLS: list[str] = [
    # ...
    # Calendar
    "hour",
    "weekday",
    "month",
    "quarter",
    "is_weekend",
    # ...
]
```

**Key Observations**:
- Model B uses: `hour_of_day`, `day_of_week`, `month`
- Model A uses: `hour`, `weekday`, `month`, `quarter`
- Feature names differ between models (`hour` vs `hour_of_day`, `weekday` vs `day_of_week`)

---

### 3. Scaler Implementation

#### File: `/home/john/BigDataSmallPrice/src/modelling/train.py`

**_fit_scalers function** (lines 465-484):
```python
def _fit_scalers(
    X: "pd.DataFrame",
    y_col: "pd.DataFrame",
) -> "tuple[np.ndarray, np.ndarray, float, float]":
    """
    Compute z-score parameters from training data.
    ...
    """
    import numpy as np

    X_np = X.values.astype("float32")
    X_mean = np.nanmean(X_np, axis=0)
    X_scale = np.nanstd(X_np, axis=0)
    X_scale[X_scale < 1e-8] = 1.0
    # ...
```

**Impact on Cyclical Features**:
- Scalers compute z-score (mean=0, std=1) across ALL features
- Cyclical encodings (sine/cosine in [-1, 1]) will be z-scored
- This is **technically acceptable** but suboptimal - z-scoring doesn't destroy cyclicality, just rescales
- Alternative: Skip scaling for cyclical features (requires code modification)

---

## Files Referencing Calendar Features

### Complete File Inventory

| File Path | Lines | Feature | Context |
|-----------|-------|---------|---------|
| `infra/db/features.sql` | 185-189 | `hour_of_day`, `day_of_week`, `month` | `training_features` view |
| `infra/db/features.sql` | 258-262 | `hour_of_day`, `day_of_week`, `month` | View output columns |
| `infra/db/features.sql` | 347-352 | `hour_of_day`, `hour`, `day_of_week`, `weekday`, `month`, `quarter` | `winterthur_net_load_features` view |
| `infra/db/init.sql` | 252-256 | `hour_of_day`, `day_of_week`, `month` | `training_features` view (duplicate) |
| `infra/db/init.sql` | 313-317 | `hour_of_day`, `day_of_week`, `month` | View output columns (duplicate) |
| `infra/db/init.sql` | 426-430 | `hour_of_day`, `hour`, `day_of_week`, `weekday`, `month`, `quarter` | `winterthur_net_load_features` view (duplicate) |
| `src/processing/export_pipeline.py` | 37-41 | `hour_of_day`, `day_of_week`, `month` | `FEATURE_COLS` list |
| `src/processing/export_pipeline.py` | 379-383 | `hour`, `weekday`, `month`, `quarter` | `LOAD_FEATURE_COLS` list |
| `src/modelling/predict.py` | 29 | `FEATURE_COLS` import | Used for inference feature ordering |
| `src/modelling/train.py` | 465-484 | Scalers | `_fit_scalers` applies to all features |
| `src/api/main.py` | 273, 298 | `FEATURE_COLS` import | API prediction endpoint |
| `src/api/main.py` | 310, 314 | `LOAD_FEATURE_COLS` import | API prediction endpoint |

---

## FEATURE_COLS Usage Analysis

### Direct Consumers of `FEATURE_COLS`:

1. **`src/processing/export_pipeline.py`** (lines 37-70)
   - Source of truth for feature list
   - Used in `run_export()` for selecting columns

2. **`src/modelling/predict.py`** (line 29)
   - Re-exports `FEATURE_COLS` for inference
   - Used to ensure prediction input ordering matches training

3. **`src/api/main.py`** (lines 273, 298)
   - API endpoint uses `FEATURE_COLS` to filter/select features

4. **`src/testing/unittests/test_model.py`** (line 35)
   - Unit tests for model training

5. **`src/testing/unittests/test_feature_pipeline.py`** (lines 17-18)
   - Tests for both `FEATURE_COLS` and `LOAD_FEATURE_COLS`

### Direct Consumers of `LOAD_FEATURE_COLS`:

1. **`src/processing/export_pipeline.py`** (lines 376-402)
   - Source of truth for load model features

2. **`src/api/main.py`** (lines 310, 314)
   - API endpoint for load predictions

---

## Schema Migration System

### Current State: **NO MIGRATION SYSTEM**

**Evidence**:
- No Alembic, Flyway, or similar migration tool found
- Only two SQL files exist:
  - `infra/db/init.sql` (18,433 bytes) - Initial schema setup
  - `infra/db/features.sql` (15,471 bytes) - Feature view definitions
- Files contain duplicate view definitions
- Schema changes require manual application via `psql`

**Implication for Migration**:
- View changes require manual execution: `psql -h localhost -p 5433 -U bdsp -d bdsp -f infra/db/features.sql`
- No versioning or rollback mechanism
- Both `init.sql` and `features.sql` must be kept in sync manually

---

## Model Training Expectations

### Current Model Training Behavior

#### XGBoost Models (`train_xgboost()`, `train_load_model()`)
- **Location**: `src/modelling/train.py`, lines 55-105
- **Feature Handling**: Receives DataFrame with feature columns from `FEATURE_COLS`
- **No hardcoded expectations**: Uses whatever columns are present in training data
- **Impact of migration**: Low - model will adapt to new feature columns

#### LSTM/Transformer Sequence Models
- **Location**: `src/modelling/train.py`, lines 510-700+
- **Feature Handling**: 
  - Feature columns determined by `X_train.columns` (line 538)
  - Scalers fitted on training data
  - Model saved with `feature_cols` in config bundle (line ~560)
- **Impact of migration**: Medium - requires retraining; saved models will have old feature schema

#### API Prediction (`predict.py`)
- **Location**: `src/modelling/predict.py`
- **Feature Handling**: Uses `FEATURE_COLS` to ensure correct column ordering
- **Impact of migration**: High - must align with new feature names

---

## Required Changes Summary

### Phase 1: SQL Views (High Risk)

**Files to Modify**:
1. `/home/john/BigDataSmallPrice/infra/db/features.sql`
2. `/home/john/BigDataSmallPrice/infra/db/init.sql` (duplicate - must stay in sync)

**Changes for `training_features` view**:
- Lines 185-189: Replace `hour_of_day`, `day_of_week`, `month` with:
  ```sql
  SIN(2 * PI() * EXTRACT(hour FROM time)::INT / 24.0) AS hour_sin,
  COS(2 * PI() * EXTRACT(hour FROM time)::INT / 24.0) AS hour_cos,
  SIN(2 * PI() * EXTRACT(dow FROM time)::INT / 7.0) AS dow_sin,
  COS(2 * PI() * EXTRACT(dow FROM time)::INT / 7.0) AS dow_cos,
  SIN(2 * PI() * (EXTRACT(month FROM time)::INT - 1) / 12.0) AS month_sin,
  COS(2 * PI() * (EXTRACT(month FROM time)::INT - 1) / 12.0) AS month_cos,
  ```
- Line 258-262: Update output column list to match

**Changes for `winterthur_net_load_features` view**:
- Lines 347-352: Similar replacements (note: 6 features become 12: hour/hour_of_day, weekday/day_of_week, month, quarter)
- Consider removing duplicate `hour`/`hour_of_day` and `weekday`/`day_of_week`

### Phase 2: Python Feature Lists (Medium Risk)

**File**: `/home/john/BigDataSmallPrice/src/processing/export_pipeline.py`

**FEATURE_COLS changes** (lines 37-41):
```python
# BEFORE
    "hour_of_day",
    "day_of_week",
    "month",

# AFTER
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
```

**LOAD_FEATURE_COLS changes** (lines 379-383):
```python
# BEFORE
    "hour",
    "weekday",
    "month",
    "quarter",

# AFTER (consider also adding quarter_sin/cos)
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
    # "quarter_sin",
    # "quarter_cos",
```

### Phase 3: Unit Tests (Low Risk)

**File**: `/home/john/BigDataSmallPrice/src/testing/unittests/test_feature_pipeline.py`

- Lines 44, 78-80, 159, 191: Update assertions referencing old feature names

### Phase 4: Model Retraining (Required)

- All existing models must be retrained with new feature schema
- Old saved models (`.joblib` and `.pt` files) will be incompatible
- No automated retraining pipeline detected

---

## Risk Assessment

| Component | Risk Level | Rationale |
|-----------|------------|-----------|
| SQL Views | **High** | Production database changes require manual application; duplicate definitions in `init.sql` and `features.sql` must both be updated |
| `FEATURE_COLS` List | **Medium** | Widely imported; mismatch with SQL causes export failures |
| `LOAD_FEATURE_COLS` List | **Medium** | Similar to above |
| API Endpoints | **Medium** | Uses feature lists for inference ordering |
| Unit Tests | **Low** | Straightforward updates required |
| Saved Models | **High** | All models must be retrained; no automated retraining detected |
| Scalers | **Low** | Z-scoring cyclical features is suboptimal but functional |

---

## Recommendations

1. **Immediate Actions**:
   - Create a feature branch for migration
   - Update SQL views in both `features.sql` and `init.sql`
   - Update Python feature lists

2. **Before Production**:
   - Run full test suite
   - Retrain all models (naive, linear, xgb, lstm, transformer for both Model A and B)
   - Validate API predictions work correctly
   - Manually apply SQL changes to production database

3. **Future Improvements**:
   - Consider implementing a schema migration tool (Alembic)
   - Consider deduplicating SQL files (remove duplicate view definitions)
   - Add automated model retraining to CI/CD

---

## Migration Checklist

- [ ] Update `infra/db/features.sql` - `training_features` view (lines 185-189, 258-262)
- [ ] Update `infra/db/features.sql` - `winterthur_net_load_features` view (lines 347-352)
- [ ] Update `infra/db/init.sql` - duplicate views (lines 252-256, 313-317, 426-430)
- [ ] Update `src/processing/export_pipeline.py` - `FEATURE_COLS` (lines 37-41)
- [ ] Update `src/processing/export_pipeline.py` - `LOAD_FEATURE_COLS` (lines 379-383)
- [ ] Update `src/testing/unittests/test_feature_pipeline.py` - test assertions
- [ ] Apply SQL changes to production database
- [ ] Retrain Model B models (naive, linear, xgb, lstm_energy)
- [ ] Retrain Model A models (naive_load, linear_load, model_load, lstm_load, transformer_load)
- [ ] Verify API endpoints (`src/api/main.py`)
- [ ] Run full test suite

---

*Analysis generated: 2026-04-09*
*Files analyzed: 4 primary files, 2 duplicate SQL files, 3 test/consumer files*
