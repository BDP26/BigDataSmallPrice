# Technische Implementationsschritte & Architektur
## Dynamischer Stromtarif Winterthur — Big Data Small Price (DS23t PM4)

---

## 1. Projektziel

Aufbau eines datengetriebenen Systems zur Prognose und Berechnung **zweier dynamischer Tarife für Winterthur** in 15-Minuten-Auflösung (Day-Ahead):

| Tarif | Basis | Vorbild |
|---|---|---|
| **Dynamischer Netzpreis** | Lokale Netzlastprognose Winterthur | EKZ / CKW Formel |
| **Dynamischer Energiepreis** | EPEX Day-Ahead Spotmarkt CH | EKZ Energietarif |

---

## 2. Architektur-Übersicht & Tech-Stack

| Komponente | Technologie |
|---|---|
| **Sprache** | Python 3.10+ |
| **Orchestrierung / ETL** | Apache Airflow oder Prefect |
| **Datenbank (Time-Series)** | TimescaleDB (PostgreSQL-Erweiterung) oder InfluxDB |
| **Machine Learning** | scikit-learn, XGBoost / LightGBM |
| **Backend / API** | FastAPI (asynchron, automatische Swagger-Doku) |
| **Frontend / Dashboard** | Native HTML / Tailwind CSS, ausgeliefert via FastAPI (`Jinja2Templates`) |

---

## 3. Datenfluss & Modellarchitektur

```
┌─────────────────────────────────────────────────────┐
│              INPUTS (alle öffentlich)               │
├──────────────┬──────────────┬───────────────────────┤
│  Meteo       │  ENTSO-E     │  Stadtwerk Winterthur │
│  Open-Meteo  │  Spotmarkt   │  OGD Datasets         │
│  - Temperatur│  - Day-Ahead │  - Bruttolastgang     │
│  - Strahlung │    CH Preis  │    (1863) 2013-heute  │
│  - Bewölkung │  - Actual    │  - Netzeinspeisung    │
│  - Wind      │    Load CH   │    PV (3122)          │
└──────┬───────┴──────┬───────┴──────────┬────────────┘
       │              │                  │
       ▼              ▼                  ▼
┌─────────────┐  ┌──────────┐   ┌───────────────┐
│  Modell A   │  │ Modell B │   │  Feature Eng. │
│  Netzlast   │  │ Energie- │   │  Zeitmerkmale │
│  Prognose   │  │  preis   │   │  Feiertage ZH │
│  Winterthur │  │ Prognose │   │  Saison       │
└──────┬──────┘  └────┬─────┘   └───────┬───────┘
       │              │                  │
       ▼              ▼                  │
┌─────────────┐  ┌──────────┐           │
│  Tarif-     │  │  Tarif-  │◄──────────┘
│  formel     │  │  formel  │
│  Netz (EKZ) │  │  Energie │
└──────┬──────┘  └────┬─────┘
       ▼              ▼
┌─────────────────────────────┐
│   Dynamischer Gesamttarif   │
│   Winterthur [Rp./kWh]      │
│   96 x 15min pro Tag        │
└─────────────────────────────┘
```

### Datenquellen im Überblick

| Quelle | Was | Granularität | Kosten |
|---|---|---|---|
| `opendata.swiss #1863` | Bruttolastgang Winterthur | 15 min, seit 2013 | Kostenlos |
| `opendata.swiss #3122` | PV-Einspeisung Winterthur | 15 min | Kostenlos |
| `open-meteo.com` | Wetter + Prognose Winterthur | 15 min, seit 1940 | Kostenlos |
| `transparency.entsoe.eu` | EPEX Day-Ahead CH, Actual Load | 15 min / 1h | Kostenlos |
| `api.tariffs.groupe-e.ch` | Dynamischer Tarif live (Validierung) | 15 min | Kostenlos |
| `api.tariffs.ekz.ch` | EKZ Tarif ab Jan 2026 (Ground Truth) | 15 min | Kostenlos |

---

## 4. Modell A: Netzlast Winterthur

### Feature Set

```python
features = {
    # Zeitstruktur (vollständig deterministisch)
    "hour":               int,    # 0-23
    "weekday":            int,    # 0=Mo, 6=So
    "month":              int,    # 1-12
    "is_holiday_ZH":      bool,   # Schweizer + ZH Feiertage
    "is_school_holiday":  bool,   # Schulferien Kanton ZH
    "quarter":            int,    # 1-4

    # Wetter (Open-Meteo, Station Winterthur)
    "temp_c":             float,  # Temperatur C  <- stärkster Prädiktor
    "temp_deviation":     float,  # Abweichung vom Tagesdurchschnitt
    "ghi_wm2":            float,  # Globalstrahlung -> PV-Proxy
    "cloud_cover_pct":    float,  # Bewölkung %
    "wind_speed_ms":      float,  # Windgeschwindigkeit

    # Lagged Features (Autoregression)
    "load_lag_1d":        float,  # Netzlast gleiche Zeit gestern
    "load_lag_7d":        float,  # Netzlast gleiche Zeit letzte Woche
    "load_lag_1h":        float,  # Netzlast vor 1 Stunde

    # PV-Einspeisung (Dataset #3122)
    "pv_feed_in":         float,  # Aktuelle PV-Einspeisung ins Netz
}

# Ground Truth
y = bruttolastgang_1863 - pv_einspeisung_3122  # Netto-Netzlast

# Train / Validation / Test Split (immer zeitlich!)
train = 2013-2022   # ~9 Jahre
val   = 2023        # 1 Jahr
test  = 2024-2025   # Holdout
```

### Qualitätsziele
- MAE < 5% der mittleren Netzlast
- MAPE < 8%

---

## 5. Modell B: Energiepreis (EPEX Day-Ahead)

### Option 1 — EPEX direkt verwenden (empfohlen für MVP)

```python
# EKZ-Formel (aus technischem Dokument)
e_t = clip(k_pe * EPEX_t + k_le,
           lower = Standardtarif - 5,   # Rp./kWh
           upper = Standardtarif + 5)   # Rp./kWh

# k_pe und k_le täglich via SCIP optimieren
# Bedingung: Preisparität für Standardlastprofil
```

### Option 2 — EPEX prognostizieren (für echtes Day-Ahead)

```python
features_epex = {
    "epex_lag_1d":         float,  # Preis gestern gleiche Stunde
    "epex_lag_7d":         float,  # Preis letzte Woche
    "temp_ch_avg":         float,  # Durchschnittstemperatur CH
    "hydro_reservoir":     float,  # Speicherseestand CH (BFE)
    "wind_generation_eu":  float,  # Windproduktion Europa (ENTSO-E)
    "solar_generation_ch": float,  # Solarproduktion CH (ENTSO-E)
    "day_type":            str,    # Werktag / Wochenende
}
```

---

## 6. Tarifformel Netz

```python
def netzpreis(netzlast_t, netzlast_min, netzlast_max, standardtarif):
    # Quadrierte normierte Netzlast (analog EKZ/CKW)
    L_norm = (netzlast_t - netzlast_min) / (netzlast_max - netzlast_min)
    raw = alpha * L_norm**2

    # Beschnitt mit Ober-/Untergrenze
    return clip(raw,
                lower = standardtarif - 5,   # Rp./kWh
                upper = standardtarif + 15)  # Rp./kWh

# alpha = Normierungsfaktor für Preisparität (iterativ kalibriert)
```

---

## 7. Skalierungs- & Container-Architektur

### Containerisierung (Docker)

Das System wird in Microservices unterteilt:

| Container | Aufgabe |
|---|---|
| **API-Container** | FastAPI via `uvicorn` / `gunicorn` |
| **Worker-Container** | Airflow/Prefect Tasks, XGBoost Inference |
| **DB-Container** | TimescaleDB (lokal in Docker, Produktion als Managed Service) |

### Vertical Scaling (Scale Up) — primär für ML & DB

Sinnvoll für TimescaleDB (mehr RAM für In-Memory Indexing) und XGBoost-Training (grosse `.parquet`-Dateien, punktuell hoher RAM-Bedarf). Alternativ GPU-Instanzen für beschleunigtes Training.

### Horizontal Scaling (Scale Out) — primär für API & ETL

Da FastAPI stateless ist, können beliebig viele Container-Instanzen hinter einem Load Balancer (NGINX / AWS ALB) betrieben werden. Airflow/Prefect Worker lassen sich für parallele Datenquellen-Abfragen horizontal skalieren.

### Deployment-Tools

| Umgebung | Tool |
|---|---|
| Lokal / Dev | `docker-compose.yml` |
| Produktion | Kubernetes / AWS EKS / Google GKE mit HPA |
| Serverless | AWS Fargate (Container) oder AWS Lambda (ETL-Cronjobs) |

---

## 8. Implementationsphasen & Zeitplan

### Phase 1 — Datenbeschaffung & ETL (Woche 3–4)

**Ziel:** Alle Datenquellen anbinden, roh speichern, in TimescaleDB laden.

- Docker-Compose Setup (Airflow/Prefect, TimescaleDB)
- Python-Skripte (`httpx`) für ENTSO-E, Open-Meteo, EKZ API, OGD Winterthur (#1863, #3122)
- `pandas` für Bereinigung, UTC-Konvertierung, 15-min-Alignment
- Laden in TimescaleDB als Raw-Backup

**Testing:**
- *Unit Tests (`pytest`):* Mocking der API-Antworten mit `responses` / `unittest.mock`; Testen der JSON-Parsing-Logik auf korrektes Herausfiltern der Pfade
- *Data Quality Tests:* `Great Expectations` oder simple Asserts — keine Null-Werte in essenziellen Spalten, Timestamps aufsteigend sortiert

---

### Phase 2 — Feature Engineering & Preprocessing (Woche 5–6)

**Ziel:** Features aufbauen, Export-Pipeline für ML vorbereiten.

- SQL-Views oder pandas für zeitliche Features, Lag-Features (24h / 48h / 7d), Rolling Windows (gleitender Ø letzte 6h)
- Datenexport als `.parquet` (spaltenorientiert, schnell für XGBoost); Timestamp-Versionierung (`modeling_data_YYYYMMDD_HHMM.parquet`)
- Downcasting `float64` → `float32` / `int8` für Speicheroptimierung
- Train / Validation / Test-Split via Time-Series Split (basierend auf exportierten Parquet-Files)
- Export-Skripte als letzter Schritt der Airflow/Prefect-ETL-Pipeline (`/src/processing/export_pipeline.py`)

**Testing:**
- *Logic Tests:* Unit Tests für Lag- und Rolling-Window-Berechnungen mit hartcodierten Dummy-DataFrames
- *Export Pipeline Tests:* Laden und Validieren der `.parquet`-Files auf Konsistenz und korrekte Datentypen
- *Leakage Tests:* Assertions dass keine Zukunftsdaten (Validation/Test) in den Trainingsdaten auftauchen

---

### Phase 3 — Modellierung (Woche 8–10)

**Ziel:** Netzlast- und Energiepreismodell trainieren und evaluieren.

- **Baseline:** Naive-Modell (gleiche Zeit letzte Woche) + lineare Regression
- **Advanced ML:** `XGBRegressor` oder `LGBMRegressor`
- **Evaluation:** MAE, RMSE, MAPE — Vergleich mit EKZ / Groupe-E API-Daten
- **Tarifformel:** Netzlast → dynamischer Netzpreis (quadrierte Formel, analog EKZ); EPEX → dynamischer Energiepreis (SCIP-Optimierung oder direkte Transformation)

**Validierungsstrategie:**

*Phase 3a — Netzlast validieren (sofort möglich):*
```
Metriken:         MAE / RMSE / MAPE auf Testset 2024-2025
Residuen-Analyse: nach Stunde / Wochentag / Monat
Besondere Events: Hitzewellen, Feiertage, Ferienende
Baseline:         Naives Modell (letzte Woche gleiche Zeit)
```

*Phase 3b — Tarifformel via Groupe E validieren (sofort möglich):*
```
Groupe E API (api.tariffs.groupe-e.ch) seit Jan 2024 live.
grid-Komponente = reines Netzlastsignal.
Invertierte Formel erlaubt Rückrechnung der impliziten Netzlast
als Proxy-Vergleich gegen eigene Prognose.
```

*Phase 3c — EKZ Ground Truth (ab Jan 2026):*
```
Eigenes Modell generiert Tarif-Prognose für Tag D.
EKZ API liefert tatsächlichen Tarif für Tag D.
Vergleich: Korrelation, MAE, Spreizungsgenauigkeit.
```

**Testing:**
- *Sanity Checks:* Modell auf kleinem Dummy-Datensatz überfitten (Loss → 0) — beweist fehlerfreies Setup
- *Baseline Vergleiche:* Automatisierter Test schlägt fehl, falls XGBoost auf Validierungsset schlechter als Naive Baseline
- *Inference Tests:* `joblib` Export/Load-Routine — Predictions im Memory und geladene Datei müssen exakt übereinstimmen

---

### Phase 4 — Backend & User Management (Woche 7 & 11)

**Ziel:** FastAPI-Backend mit Prognose-Endpunkten und Benutzerverwaltung.

- API-Endpunkte: `/predict` (Tarif-Prognose), User-Registrierung (JWT), Verbrauchseingabe
- Statische Dashboards (`user_dash.html`, `admin_dash.html`) via `Jinja2Templates` / `StaticFiles` ausgeliefert; Daten werden asynchron über REST-Endpunkte bezogen

**Testing:**
- *API Integration Tests:* `TestClient` aus FastAPI — HTTP-Statuscodes prüfen (200 OK, 401 Unauthorized bei falschem Token)
- *Schema Validation:* Pydantic-Modelle mit absichtlich falschen JSON-Payloads testen; sauberes Abfangen von Fehlern prüfen

---

### Phase 5 — Dashboard (Woche 12)

**Ziel:** Visualisierung der Preisprognosen und günstiger Verbrauchsfenster.

- Einbindung und Anbindung der FastAPI-Endpunkte (Google Stitch)
- Visualisierung: Preisprognose-Kurve (Netz + Energie), Markierung günstiger 15-min-Fenster, gerätespezifische Empfehlungen (Waschmaschine, Wärmepumpe, E-Auto)

**Testing:**
- *End-to-End (E2E) Tests:* `Playwright` oder `Cypress` — kompletter User-Flow simulieren (Login → Geräteeingabe → Preis-Chart prüfen)
- *Visual Regression Tests:* Automatisierte Screenshots zum Erkennen von Layout-Abweichungen durch Updates
- *Mock-API Tests:* Frontend isoliert gegen Mock-API testen (Ladezustände, Fehlermeldungen)

---

### Phase 6 — Abschluss & CI/CD (Woche 13–14)

**Ziel:** Produktionsreifes System mit automatisiertem Testing.

- GitHub Actions CI/CD: Linting, Unit Tests, Integrationstests automatisch bei jedem Push
- Deployment auf Cloud-Infrastruktur (Kubernetes / AWS EKS)
- Finaldokumentation und Abgabe

**Testing:**
- *Pipeline Tests:* CI/CD vollständig grün (alle Tests bestanden) vor Produktivschaltung

---

## 9. Zeitplan Gesamtübersicht

| Woche | Phase | Hauptlieferables |
|---|---|---|
| 3–4 | ETL & Datenbeschaffung | Docker Setup, alle Datenquellen angebunden, TimescaleDB befüllt |
| 5–6 | Feature Engineering | `.parquet` Export-Pipeline, Feature-Set komplett, Train/Val/Test Split |
| 7 | Backend (Teil 1) | FastAPI Grundstruktur, JWT Auth, erste Endpunkte |
| 8–10 | Modellierung | Netzlast- + Energiepreismodell trainiert, Tarifformel implementiert, Validierung |
| 11 | Backend (Teil 2) | Alle Endpunkte fertig, Dashboard-Integration |
| 12 | Dashboard | Frontend fertig, E2E Tests grün |
| 13–14 | CI/CD & Abschluss | GitHub Actions Pipeline, Deployment, Dokumentation |
