"""
Airflow Pools — create via CLI or import through the Airflow UI.

Setup commands (run once after `airflow db init`):

    airflow pools set entsoe_pool 1 "Max 1 concurrent ENTSO-E request"
    airflow pools set tariff_pool 2 "Max 2 concurrent tariff API requests (EKZ/CKW/GroupeE)"
    airflow pools set meteo_pool  3 "Max 3 concurrent Open-Meteo requests"
    airflow pools set bafu_pool   1 "Max 1 concurrent BAFU request"

Or import via Airflow UI: Admin > Pools > Import (JSON).
"""

POOLS: dict[str, dict] = {
    "entsoe_pool": {
        "slots": 1,
        "description": "Max 1 concurrent ENTSO-E request",
    },
    "tariff_pool": {
        "slots": 2,
        "description": "Max 2 concurrent tariff API requests (EKZ/CKW/GroupeE)",
    },
    "meteo_pool": {
        "slots": 3,
        "description": "Max 3 concurrent Open-Meteo requests",
    },
    "bafu_pool": {
        "slots": 1,
        "description": "Max 1 concurrent BAFU request",
    },
}
