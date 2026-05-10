FROM apache/airflow:2.9.3

# Install ML packages needed by the training DAG (modelling.train / evaluate / predict)
RUN pip install --no-cache-dir \
    "joblib>=1.3" \
    "scikit-learn>=1.4" \
    "xgboost>=2.0" \
    "pyarrow>=15" \
    "holidays>=0.46"

# PyTorch with CUDA 12.4 support (sequence models: LSTM + Transformer)
# Falls back to CPU automatically when no GPU is accessible at runtime.
RUN pip install --no-cache-dir \
    "torch>=2.2" \
    --index-url https://download.pytorch.org/whl/cu124
