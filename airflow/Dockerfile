FROM apache/airflow:2.9.3

# Install ML packages needed by the training DAG (modelling.train / evaluate / predict)
RUN pip install --no-cache-dir \
    "joblib>=1.3" \
    "scikit-learn>=1.4" \
    "xgboost>=2.0" \
    "pyarrow>=15" \
    "holidays>=0.46"
