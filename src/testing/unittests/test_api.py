"""
API integration tests for src/api/main.py – Phase 4.

Uses FastAPI's TestClient (no running server required).
All database and model calls are mocked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parents[3]))

# Set BDSP_DB_PASSWORD so main.py can be imported without env var
import os
os.environ.setdefault("BDSP_DB_PASSWORD", "test")

from fastapi.testclient import TestClient

import src.api.main as main_module
from src.api.main import app

client = TestClient(app)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def reset_stores():
    """Clear in-memory user store and model cache before/after every test."""
    main_module._USERS.clear()
    main_module._model_cache.clear()
    yield
    main_module._USERS.clear()
    main_module._model_cache.clear()


@pytest.fixture
def registered_user():
    """Register a test user and return credentials."""
    payload = {"username": "testuser", "password": "secret123"}
    client.post("/auth/register", json=payload)
    return payload


@pytest.fixture
def auth_token(registered_user):
    """Return a valid JWT for the registered test user."""
    resp = client.post("/auth/login", json=registered_user)
    return resp.json()["access_token"]


@pytest.fixture
def auth_headers(auth_token):
    """Return Authorization headers dict."""
    return {"Authorization": f"Bearer {auth_token}"}


# ── POST /auth/register ───────────────────────────────────────────────────────


def test_register_creates_user():
    resp = client.post("/auth/register", json={"username": "alice", "password": "pw1"})
    assert resp.status_code == 201
    assert "message" in resp.json()


def test_register_duplicate_returns_409():
    payload = {"username": "alice", "password": "pw1"}
    client.post("/auth/register", json=payload)
    resp = client.post("/auth/register", json=payload)
    assert resp.status_code == 409


def test_register_pydantic_validation_missing_field():
    """Missing 'password' field → 422 Unprocessable Entity."""
    resp = client.post("/auth/register", json={"username": "alice"})
    assert resp.status_code == 422


# ── POST /auth/login ──────────────────────────────────────────────────────────


def test_login_returns_token(registered_user):
    resp = client.post("/auth/login", json=registered_user)
    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"


def test_login_wrong_password(registered_user):
    resp = client.post(
        "/auth/login",
        json={"username": registered_user["username"], "password": "wrong"},
    )
    assert resp.status_code == 401


def test_login_unknown_user():
    resp = client.post("/auth/login", json={"username": "ghost", "password": "pw"})
    assert resp.status_code == 401


# ── POST /api/predict ─────────────────────────────────────────────────────────


def test_predict_requires_auth():
    """Calling /api/predict without a token → 401 or 403."""
    resp = client.post("/api/predict", json={"features": {"lag_1h": 80.0}})
    assert resp.status_code in (401, 403)


def test_predict_invalid_token():
    resp = client.post(
        "/api/predict",
        json={"features": {"lag_1h": 80.0}},
        headers={"Authorization": "Bearer totally.invalid.token"},
    )
    assert resp.status_code == 401


def test_predict_no_model_returns_503(auth_headers):
    """When no model file exists, /api/predict returns 503."""
    with patch("src.api.main._get_model", side_effect=FileNotFoundError("no model")):
        resp = client.post(
            "/api/predict",
            json={"features": {"lag_1h": 80.0}},
            headers=auth_headers,
        )
    assert resp.status_code == 503


def test_predict_returns_200_with_valid_token(auth_headers):
    """With a valid token and a mocked model, /api/predict returns 200."""
    mock_model = MagicMock()

    # The endpoint imports predict_from_dict lazily from modelling.predict;
    # patch it there so the import inside the function sees the mock.
    with (
        patch("src.api.main._get_model", return_value=mock_model),
        patch("modelling.predict.predict_from_dict", return_value=75.5),
    ):
        resp = client.post(
            "/api/predict",
            json={"features": {"lag_1h": 80.0, "temperature_2m": 10.0}},
            headers=auth_headers,
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "prediction_eur_mwh" in body
    assert body["model"] == "xgb"


def test_predict_pydantic_wrong_type(auth_headers):
    """Sending a string instead of a float value → 422."""
    resp = client.post(
        "/api/predict",
        json={"features": {"lag_1h": "not-a-number"}},
        headers=auth_headers,
    )
    assert resp.status_code == 422


def test_predict_missing_features_field(auth_headers):
    """Missing 'features' key → 422."""
    resp = client.post("/api/predict", json={}, headers=auth_headers)
    assert resp.status_code == 422


# ── GET /api/forecast (public) ────────────────────────────────────────────────


def test_forecast_no_model_returns_503():
    with patch("src.api.main._get_model", side_effect=FileNotFoundError):
        with patch("src.api.main._connect") as mock_conn:
            import pandas as pd
            from datetime import datetime, timezone

            mock_conn.return_value.__enter__ = MagicMock()
            # Simulate DB returning one feature row
            with patch("src.api.main.pd.read_sql") as mock_sql:
                mock_sql.return_value = pd.DataFrame(
                    [{"time": datetime(2026, 3, 3, tzinfo=timezone.utc), "lag_1h": 80.0}]
                )
                resp = client.get("/api/forecast")
    assert resp.status_code == 503


# ── GET / and /dashboard ──────────────────────────────────────────────────────


def test_index_returns_html():
    """Admin dashboard should return HTML with status 200."""
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_dashboard_returns_html():
    """User dashboard should return HTML with status 200."""
    resp = client.get("/dashboard")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
