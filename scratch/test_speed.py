import time
import numpy as np
import pandas as pd
import joblib

model = joblib.load("models/xgb_energy_latest.joblib")
cols = list(model.feature_names_in_)

# Dummy row
row = {c: 0.0 for c in cols}

t0 = time.time()
for _ in range(672):
    # Method 1: DataFrame
    X_df = pd.DataFrame([[row.get(c, float("nan")) for c in cols]], columns=cols)
    model.predict(X_df)
print("DataFrame:", time.time() - t0)

t0 = time.time()
for _ in range(672):
    # Method 2: Numpy array
    X_np = np.array([[row.get(c, float("nan")) for c in cols]], dtype=np.float32)
    model.predict(X_np)
print("Numpy:", time.time() - t0)
