# BigDataSmallPrice

BigDataSmallPrice ist eine End-to-End-Plattform zur Prognose dynamischer
Stromtarife für Winterthur. Das Projekt verbindet Strommarkt-, Wetter-,
Tarif- und lokale Lastdaten, speichert sie in TimescaleDB, orchestriert die
Pipelines mit Airflow und stellt Prognosen über eine FastAPI sowie zwei
Web-Dashboards bereit.

Das Projekt entstand im Rahmen von PM4 an der ZHAW.

## Kurzüberblick

- Prognose des EPEX-Day-Ahead-Strompreises für die Schweiz.
- Prognose der lokalen Winterthurer Netzlast aus Bruttolast und PV-Einspeisung.
- Berechnung eines geschätzten Gesamttarifs in Rp./kWh aus Energie- und
  Netzpreis-Komponenten.
- Automatisierte Datenbeschaffung für ENTSO-E, Open-Meteo, EKZ, CKW,
  Groupe E, BAFU und Stadtwerk Winterthur.
- Feature Engineering für Zeitreihenmodelle inklusive Lags, rollierenden
  Kennzahlen, Kalenderfeatures und Wetterfeatures.
- Modelltraining mit Baselines, linearen Modellen, XGBoost, LSTM und
  Transformer-Varianten.
- User-Dashboard für Prognosen und Admin-Dashboard für Datenbankstatus,
  Backfills, Trainingsjobs, Rate-Limits und Modellmetriken.

## Architektur

```text
Externe Datenquellen
        |
        v
Apache Airflow DAGs  --->  TimescaleDB / PostgreSQL
        |                         |
        |                         v
        +--------------->  Feature Views / Parquet Exports
                                  |
                                  v
                          Modelltraining
                                  |
                                  v
FastAPI Backend  <-------  Modellartefakte
        |
        v
Nginx / statische Dashboards
```

| Bereich | Technologie / Pfad |
| --- | --- |
| Orchestrierung | Apache Airflow, `airflow/dags/` |
| Datenbank | TimescaleDB für Projektdaten, PostgreSQL für Airflow-Metadaten |
| Backend | FastAPI, `src/api/main.py` |
| Frontend | Statische HTML/JS-Dashboards, Nginx, `src/frontend/` |
| Datenbeschaffung | Collector in `src/data_collection/` und ETL-Tasks in `src/etl/` |
| Feature Engineering | SQL-Views in `infra/db/init.sql`, Exporte in `src/processing/` |
| Modellierung | scikit-learn, XGBoost, PyTorch, `src/modelling/` |
| Deployment | Docker Compose |
| Tests | pytest, `src/testing/` |

## Datenquellen

| Quelle | Zweck |
| --- | --- |
| ENTSO-E Transparency Platform | Day-Ahead-Preise, Last, Lastprognosen, Erzeugung, Grenzflüsse |
| Open-Meteo | Wetterdaten für Winterthur sowie deutsche Wind-/Solar-Proxies |
| EKZ | Dynamische Tarifdaten und Tarifformeln |
| CKW / Groupe E | Vergleichbare dynamische Tarife |
| BAFU | Hydrologische Daten |
| Stadtwerk Winterthur | Lokale Bruttolast und PV-Einspeisung |

## Modelle

Das Projekt trennt zwei Vorhersageaufgaben:

- **Model B / Energy:** prognostiziert den EPEX-Day-Ahead-Preis in EUR/MWh.
- **Model A / Load:** prognostiziert die lokale Netto-Netzlast in kWh.

Die API kombiniert beide Outputs mit den Tarifformeln aus
`src/processing/tariff_formulas.py`. Falls noch kein Lastmodell verfügbar ist,
fällt die Prognose auf einen Energiepreis-Only-Modus mit Standard-Netzpreis
zurück.

Trainierte Modelle und Metriken liegen unter `models/`. Das Manifest
`models/best_models.json` steuert, welche Modelle die API bevorzugt verwendet.

## Voraussetzungen

- Docker und Docker Compose
- Python 3.11 oder neuer für lokale Entwicklung
- ENTSO-E API Token
- Optional: NVIDIA Container Toolkit für GPU-beschleunigtes Training

## Schnellstart mit Docker

1. Repository klonen:

```bash
git clone https://github.com/BDP26/BigDataSmallPrice.git
cd BigDataSmallPrice
```

2. Environment-Datei erstellen:

```bash
cp .env.example .env
```

3. `.env` anpassen:

```env
ENTSOE_API_TOKEN=...
BDSP_DB_PASSWORD=...
AIRFLOW_DB_PASSWORD=...
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=...
AIRFLOW_ADMIN_EMAIL=admin@example.com
BDSP_JWT_SECRET=...
```

4. Services starten:

```bash
docker compose up -d --build
```

5. Status prüfen:

```bash
docker compose ps
```

Beim ersten Start initialisiert Docker Compose die TimescaleDB, migriert die
Airflow-Datenbank, erstellt den Airflow-Admin-User und richtet Airflow-Pools
für API-Rate-Limits ein.

## Verfügbare Services

| Dienst | URL |
| --- | --- |
| User-Dashboard | http://localhost:8002 |
| Admin-Dashboard | http://localhost:8002/admin |
| API / Swagger | http://localhost:8001/docs |
| Airflow Web UI | http://localhost:8080 |
| TimescaleDB vom Host | `localhost:5433` |

Hinweis: Die API läuft im Container auf Port `8000` und ist auf dem Host unter
Port `8001` erreichbar.

## Airflow DAGs

| DAG | Zeitplan | Zweck |
| --- | --- | --- |
| `bdsp_etl_daily` | täglich 06:00 | Importiert aktuelle Daten aus allen Quellen in TimescaleDB |
| `bdsp_feature_daily` | täglich 07:00 | Erstellt ML-Features und Parquet-Exporte |
| `bdsp_training_daily` | manuell | Trainiert Energy- und Load-Modelle |
| `bdsp_backfill` | manuell | Lädt historische Daten für einen Zeitraum nach |

Die DAGs sind beim Erstellen standardmässig pausiert. Sie können in der
Airflow Web UI oder über das Admin-Dashboard aktiviert bzw. gestartet werden.

## Historischen Backfill starten

Der Backfill kann über das Admin-Dashboard oder direkt über die API gestartet
werden:

```bash
curl -X POST http://localhost:8001/api/backfill/trigger \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
```

Alternativ gibt es ein Hilfsskript:

```bash
./scripts/trigger_historical_backfill.sh --from 2024-01-01
```

Das Skript ist für Bash/Linux-Umgebungen geschrieben und beachtet die
ENTSO-E-Rate-Limits durch chunked Backfills.

## Wichtige API-Endpunkte

| Endpoint | Beschreibung |
| --- | --- |
| `GET /api/forecast` | Aktuelle Tarifprognose mit Energie-, Netz- und Gesamttarif |
| `GET /api/forecast/week` | Wochenprognose für günstige Zeitfenster |
| `GET /api/price-history` | Letzte ENTSO-E-Preise |
| `GET /api/db-status` | Tabellenstatistiken und Datenbankstatus |
| `GET /api/db-explorer/schema` | Datenbankschema für das Admin-Dashboard |
| `GET /api/feature-status` | Status der Trainingsfeatures |
| `GET /api/airflow/dags` | Airflow-DAG-Status über API-Proxy |
| `GET /api/rate-limits` | Aktuelle API-Call- und Rate-Limit-Übersicht |
| `POST /api/backfill/estimate` | Schätzung für Backfill-Aufwand |
| `POST /api/backfill/trigger` | Startet `bdsp_backfill` |
| `GET /api/backfill/status/{dag_run_id}` | Status eines Backfill-Runs |
| `GET /api/models/status` | Trainierte Modelle und Metriken |
| `GET /api/models/validation/{model_name}` | Validierungsdaten für ein Modell |
| `POST /api/training/trigger` | Startet `bdsp_training_daily` |
| `GET /api/training/status/{dag_run_id}` | Status eines Trainings-Runs |
| `POST /auth/register` | Test-User für JWT-Endpunkte erstellen |
| `POST /auth/login` | JWT beziehen |
| `POST /api/predict` | Direkte Modellinferenz mit Feature-Dict, JWT erforderlich |

## Lokale Entwicklung

Eine lokale Python-Umgebung kann so eingerichtet werden:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Unter Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Die Tests können ohne laufende Docker-Services gestartet werden, solange keine
Integrationstests gegen die Datenbank benötigt werden:

```bash
python -m pytest src/testing/unittests
```

Alle Tests:

```bash
python -m pytest src/testing
```

## Projektstruktur


