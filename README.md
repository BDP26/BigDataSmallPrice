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



























































