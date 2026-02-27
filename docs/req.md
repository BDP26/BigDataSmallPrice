# Technische Implementationsschritte & Architektur (req.md)

Basierend auf dem Proposal (Big Data Small Price, DS23t PM4) schlage ich als Software Engineer folgende konkrete technische Umsetzung und Architektur vor:

## 1. Architektur-Übersicht & Tech-Stack

Um die "Big Data"-Anforderungen skalierbar umzusetzen, empfehle ich folgenden Stack:

*   **Sprache:** Python 3.10+ (für Datenverarbeitung und ML)
*   **Orchestrierung / ETL:** Apache Airflow oder Prefect
*   **Datenbank (Time-Series):** TimescaleDB (PostgreSQL-Erweiterung) oder InfluxDB.
*   **Machine Learning:** scikit-learn, XGBoost/LightGBM.
*   **Backend / API:** FastAPI (schnell, asynchron, automatische Swagger-Doku). **FastAPI wird zudem genutzt, um die statischen HTML-Dashboards (`user_dash.html`, `admin_dash.html`) direkt auszuliefern (via `Jinja2Templates` oder `StaticFiles`), welche ihre Daten asynchron über die REST-API-Endpunkte beziehen.**
*   **Frontend / Dashboard:** Native HTML/Tailwind CSS Dashboards, ausgeliefert durch das FastAPI-Backend.

---

## 2. Skalierungs- & Container-Architektur (Scaling Strategy)

Um die Anwendung für "Big Data" und hohe Lasten robust aufzustellen, wird folgende Containerisierungs- und Skalierungsstrategie empfohlen:

### Containerisierung (Docker)
Das gesamte System wird in Microservices unterteilt und via Docker containerisiert:
*   **API-Container:** Führt die FastAPI-Anwendung via `uvicorn` / `gunicorn` aus.
*   **Worker-Container (ETL & ML):** Führen die Airflow/Prefect Tasks und rechenintensive XGBoost-Inferences aus.
*   **DB-Container:** TimescaleDB Instanz (oft ausgelagert in Managed Services bei Produktion, lokal in Docker).

### Vertical Scaling (Scale Up) vs. Horizontal Scaling (Scale Out)

**1. Vertical Scaling (CPU/RAM-Upgrades) – *Der primäre Hebel für ML & TimescaleDB***
Vertical Scaling bedeutet, einer bestehenden Maschine mehr Ressourcen (z.B. 16 Cores statt 4, 64GB RAM statt 16GB) zuzuweisen.
*   *Wann sinnvoll?* 
    *   **Datenbank (TimescaleDB):** Relationale Time-Series Datenbanken profitieren enorm von Vertical Scaling (mehr RAM für In-Memory Indexing und Caching der neuesten Sensordaten).
    *   **ML-Training (XGBoost):** Das Einlesen großer `.parquet`-Dateien in Pandas und das Training kompletter Modelle erfordert punktuell massive Mengen an RAM. Dies lässt sich leichter vertikal auf einer starken Maschine abbilden (oder durch GPU-Instanzen).

**2. Horizontal Scaling (Replikation von Trail-Instanzen) – *Der primäre Hebel für API & ETL***
Horizontal Scaling bedeutet, mehrere Server-Instanzen hinter einen Load Balancer zu schalten (z.B. von 1 Container auf 5 Container).
*   *Wann sinnvoll?*
    *   **FastAPI Backend:** Da FastAPI stateless (zustandlos) ist, können beliebig viele Container-Instanzen (Pods in Kubernetes oder Tasks in AWS ECS) gestartet werden, um tausende Nutzeranfragen (Dashboard-Aufrufe, `/predict`-Requests) parallel abzufangen. Ein Load-Balancer (z.B. NGINX oder AWS ALB) verteilt den Traffic.
    *   **Airflow/Prefect Workers:** Wenn viele verschiedene Datenquellen zeitgleich abgefragt und transformiert werden müssen (Kafka-Streams, verschiedene Wetter-APIs), können mehrere Worker-Nodes diese Aufgaben parallel abarbeiten.

### Umsetzung der Skalierung (Tools)
*   **Lokal / Entwicklung:** `docker-compose.yml` (einfache Orchestrierung der Services).
*   **Produktion (Cloud):** 
    *   **Kubernetes (K8s) / AWS EKS / Google GKE:** Erlaubt automatisches Horizontal Autoscaling (HPA) der FastAPI-Pods bei CPU-Spitzen sowie automatisches Deployment auf stärkere Nodes für ML-Jobs.
    *   **Serverless Alternativen:** AWS Fargate (für Container ohne Server-Management) oder AWS Lambda (für winzige ETL-Scraping-Cronjobs).

## 2. Detaillierte Implementationsschritte & Testing

### Phase 1: Datenbeschaffung & ETL (Woche 3-4)
*   **Infrastruktur aufsetzen:** Docker-Compose Setup (Airflow/Prefect, TimescaleDB).
*   **Data Ingestion:** Python-Skripte (`httpx`) für ENTSO-E, MeteoSchweiz, EKZ API und BFU. Speichern als Raw-Backup.
*   **Data Transformation:** `pandas` für Bereinigung, UTC-Konvertierung, 1-Stunden-Aggregation. Laden in TimescaleDB.
*   **Testing dieser Phase:**
    *   *Unit Tests (`pytest`):* Mocking der API-Antworten mit `responses` oder `unittest.mock`. Testen der Parsing-Logik auf das korrekte Herausfiltern der JSON-Pfade.
    *   *Data Quality Tests:* Checks in der Pipeline (z.B. mit `Great Expectations` oder simplen Asserts), ob keine `Null`-Werte in essenziellen Spalten wie dem Datum existieren und Timestamps aufsteigend sortiert sind.

### Phase 2: Feature Engineering & Preprocessing (Woche 5-6)
*   **Feature Generation:** SQL-Views oder pandas für zeitliche Features, Lag-Features (24h/48h) und Rolling Windows (z.B. gleitender Durchschnitt der letzten 6h).
*   **Data Export Pipeline (Planung & Architektur):**
    *   *Ziel:* Performante Bereitstellung der großen Datenmengen aus der TimescaleDB für das ML-Modelltraining.
    *   *Extraktion (Queries):* Gezielte SQL-Abfragen über aggregierte und bereinigte Zeitreihen (Wetter, Preise, Time-Lags).
    *   *Transformation & Optimierung:* Laden der Batch-Daten in Pandas DataFrames, Optimierung der Datentypen (z.B. Downcasting von `float64` auf `float32` oder `int8`), um Memory zu sparen.
    *   *Speicherformat:* Speicherung primär als `.parquet` Files (spaltenorientiert, exzellente Kompression und extrem schnelle Lesezeiten für scikit-learn/XGBoost). Alternativ `.h5` für spezifische Workloads.
    *   *Orchestrierung:* Die Export-Skripte (z.B. `/src/processing/export_pipeline.py`) werden als letzter Schritt in der Airflow/Prefect-ETL-Pipeline ausgeführt.
    *   *Versionierung:* Generierte Dateien erhalten einen Timestamp (z.B. `modeling_data_YYYYMMDD_HHMM.parquet`) für die vollständige Reproduzierbarkeit bei Modell-Experimenten.
*   **Data Splitting:** Train/Validation/Test-Sets via Time-Series Split (basierend auf den exportierten Parquet/H5-Dateien).
*   **Testing dieser Phase:**
    *   *Logic Tests:* Unit Tests für die Berechnungsfunktionen der gleitenden Mittelwerte und Lag-Features mit kleinen, hartcodierten Dummy-DataFrames.
    *   *Export Pipeline Tests:* Laden und Validieren der exportierten `.h5` / `.parquet` Files auf Konsistenz und Korrektheit der gespeicherten Datentypen.
    *   *Leakage Tests:* Überprüfen durch Assertions, dass keine Daten aus der Zukunft (Validation/Test) in den Trainingsdaten des Feature Engineering auftauchen.

### Phase 3: Modellierung (Woche 8-10)
*   **Baseline:** Naive-Modell und lineare Regression.
*   **Advanced ML:** Training eines `XGBRegressor` oder `LGBMRegressor`.
*   **Evaluation:** Metriken wie MAE, RMSE und MAPE. Vergleich mit EKZ-Daten.
*   **Testing dieser Phase:**
    *   *Sanity Checks:* Überprüfen, ob das Modell auf einem winzigen Dummy-Datensatz überfittet (Loss geht gegen 0) – beweist, dass das Setup fehlerfrei kompiliert.
    *   *Baseline Vergleiche:* Automatisierter Test, der fehlschlägt, falls das XGBoost Modell auf dem Validierungsset signifikant schlechter performt als das Naive Baseline-Modell.
    *   *Inference Tests:* Testen der Export/Load-Routine (`joblib`), um sicherzustellen, dass die Predictions im Memory und die der geladenen Datei exakt übereinstimmen.

### Phase 4: Backend & User Management (Woche 7 & 11)
*   **FastAPI Backend:** API-Endpunkte für Prognose, User-Registrierung (JWT) und Verbrauchseingabe.
*   **Testing dieser Phase:**
    *   *API Intgration Tests:* Nutzen von `TestClient` aus FastAPI, um die HTTP-Requests und Statuscodes (200 OK, 401 Unauthorized bei falschem Token) zu prüfen.
    *   *Schema Validation:* Testen der Pydantic-Modelle mit absichtlich falschen JSON-Payloads, um zu sehen, ob die API die Fehler sauber abfängt.

### Phase 5: Dashboard (Woche 12)
*   **Frontend (Google Stitch):**
    *   Manuelle Einbindung und Anbindung der FastAPI-Endpunkte in die Google Stitch Umgebung.
    *   Visualisierung der Preisprognosen, Markierung günstiger Fenster und gerätespezifischer Empfehlungen.
*   **Testing dieser Phase:**
    *   *End-to-End (E2E) Tests:* Einsatz von Tools wie `Playwright` oder `Cypress`, um den kompletten User-Flow zu simulieren (Login, Eingabe von Geräten, Prüfen ob das Preis-Chart rendert).
    *   *Visual Regression Tests:* Überprüfen, ob sich das Layout im Google Stitch Projekt durch Updates ungewollt verschoben hat (Abgleich von automatisierten Screenshots).
    *   *Mock-API Tests:* Das Frontend isoliert gegen eine Mock-API testen, um sicherzustellen, dass es korrekt auf verschiedene Server-Antworten (z.B. Ladezustände oder Fehlermeldungen) reagiert.

### Phase 6: Abschluss & CI/CD (Woche 13-14)
*   **Quality Assurance & Deployment:** CI/CD Pipeline (z.B. GitHub Actions) für automatisiertes Testen.
*   **Testing dieser Phase:**
    *   *Pipeline Tests:* Die CI/CD Pipeline ausführen lassen, um sicherzustellen, dass alle oben genannten Tests (Linting, Unit Tests, Integrationstests) grün sind, bevor das finale System produktiv geschaltet wird.
