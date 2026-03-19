FROM python:3.11-slim

WORKDIR /app

# ML packages needed for model inference + training status
RUN pip install --no-cache-dir \
    fastapi uvicorn psycopg2-binary httpx PyJWT \
    pandas joblib scikit-learn xgboost pyarrow dill

# PYTHONPATH so that "from modelling.predict import ..." resolves
# against the volume-mounted ./src directory at /app/src
ENV PYTHONPATH=/app/src

# Run from the mounted source so code changes are picked up without rebuild
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
