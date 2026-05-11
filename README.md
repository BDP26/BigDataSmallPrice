# Big Data Small Price

**Prognose dynamischer Strompreise für Winterthur** unter Einbezug von Day-Ahead-Marktpreisen (ENTSO-E), Echtzeit-Wetterdaten (MeteoSchweiz / Open-Meteo) und EKZ-Tariffstrukturen.

Big-Data-Projekt DS23t PM4 — ZHAW
Team: Ryan Bachmann, Miguel Dinis Silva, Gian Ruchti

---

## Was das System tut

Zwei dynamische Tarife für Winterthur in 15-Minuten-Auflösung werden bis zu 7 Tage im Voraus prognostiziert:

- **Dynamischer Netzpreis** — Prognose der lokalen Netzlast (Bruttolastgang OGD seit 2013) kombiniert mit der EKZ-Netzpreisformel.
- **Dynamischer Energiepreis** — EPEX-Day-Ahead-Spotpreis-Prognose kombiniert mit der EKZ-Energietarifformel.

Endprodukt: ein Web-Dashboard mit 7-Tage-Vorschau, Konfidenzintervallen, Ampel für günstige Zeitfenster und einem Onboarding zur Verbrauchsschätzung.

## Architektur

| Komponente | Stack |
|---|---|
| Datenquellen | ENTSO-E Transparency Platform, Open-Meteo, EKZ Tariff API, BAFU Hydro |
| ETL / Orchestration | Apache Airflow (DAGs für Backfill, Pipeline, Training) |
| Storage | TimescaleDB (Zeitreihen), PostgreSQL (Airflow-Metadaten) |
| Modellierung | XGBoost, LSTM, Transformer (PyTorch) — Auto-Select des besten Modells pro Ziel |
| API | FastAPI (`src/api/main.py`) |
| Frontend | Static HTML + JS (User- und Admin-Dashboard), Nginx |
| Compute | GPU (CUDA) für Sequenzmodelle |

## Quickstart

Voraussetzungen: Docker + Docker Compose, NVIDIA Container Toolkit für GPU-Training.

```bash
cp .env.example .env
# .env editieren: ENTSOE_API_TOKEN, DB-Passwörter, JWT-Secret setzen

docker compose up -d
```

Endpoints nach dem Start:

- User-Dashboard: <http://localhost/>
- Admin-Dashboard: <http://localhost/admin>
- API: <http://localhost:8000/docs>
- Airflow: <http://localhost:8080>

Historischen Backfill auslösen:

```bash
./scripts/trigger_historical_backfill.sh
```

## Repository-Struktur

```
airflow/         DAGs für ETL und Training
data/            Rohdaten und Feature-Splits (gitignored)
docs/            Proposal, technische Anforderungen, Tarif-Referenzen
infra/db/        Schema und Migrationen für TimescaleDB
models/          Trainierte Modelle (gitignored); best_models.json als Index
scripts/         Operative Skripte (Backfill, DB-Backup)
src/api/         FastAPI-Backend
src/data_collection/  Collectors für ENTSO-E, Open-Meteo, EKZ, BAFU
src/etl/         Fetch- und Transform-Tasks
src/frontend/    Static Dashboards (User + Admin) + Nginx-Config
src/modelling/   Training, Prediction, Evaluation
src/processing/  Feature-Pipeline und Tarif-Formeln
src/testing/     Unit-Tests
```

## Dokumentation

- [`docs/Proposal.md`](docs/Proposal.md) — Projektantrag mit Fragestellung und Methodik
- [`docs/req.md`](docs/req.md) — Technische Implementationsschritte und Architektur-Details
- [`analysis/cyclical_migration_analysis.md`](analysis/cyclical_migration_analysis.md) — Hintergrund zur zyklischen Feature-Kodierung

## Lizenz

Siehe [`LICENSE`](LICENSE).
