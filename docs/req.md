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

## 1a. Verfeinerte Modellziele (Stand April 2026)

### Modell A – Dynamischer Netzpreis Winterthur

**Zweistufiger Ansatz:**

**Stufe 1 – Synthetisches Label (deterministisch):**
Die EKZ-Netzpreisformel wird auf den historischen Winterthur-Bruttolastgang (OGD, seit 2013)
angewendet. So entsteht ein synthetisches Netzpreis-Label für 13 Jahre — ohne Abhängigkeit
von einer Tarif-API. Das Label ist physikalisch korrekt: keine Schätzung, sondern eine exakte
Rückrechnung auf Messdaten.

```python
# Synthetisches Label: für jeden historischen Zeitpunkt t
netzlast_norm = (netzlast_t - netzlast_min_year) / (netzlast_max_year - netzlast_min_year)
netzpreis_t   = clip(alpha × netzlast_norm², lower=std−5, upper=std+15)
```

**Stufe 2 – Wochenprognose (ML-Modell):**
Ziel ist eine 7-Tage-Vorschau des Netzpreises in 15-Minuten-Auflösung. Das Modell kann nur
Features verwenden, die bei Prognose-Zeitpunkt t₀ tatsächlich bekannt sind:

| Feature | Verfügbarkeit bei t₀ | Begründung |
|---|---|---|
| `load_lag_168h` | ✅ immer | Last vor exakt 7 Tagen (stärkster Prädiktor) |
| `load_lag_336h` | ✅ immer | Last vor exakt 14 Tagen |
| `load_rolling_avg_7d` | ✅ immer | Gleitender Ø letzte Woche |
| `temperature_forecast` | ✅ Open-Meteo 7-Tage | Wetterdienst-Prognose |
| `shortwave_radiation_forecast` | ✅ Open-Meteo 7-Tage | PV-Proxy (Solarstrahlung) |
| `wind_speed_forecast` | ✅ Open-Meteo 7-Tage | Wetterdienst-Prognose |
| `cloud_cover_forecast` | ✅ Open-Meteo 7-Tage | Wetterdienst-Prognose |
| `hour`, `weekday`, `month` | ✅ deterministisch | Kalender |
| `is_holiday_zh` | ✅ deterministisch | Schweizer Feiertage bekannt |
| `is_school_holiday` | ✅ deterministisch | Schulferienplan bekannt |
| `load_lag_1h` | ❌ nicht verfügbar | Liegt in der Zukunft |
| `pv_feed_in` (aktuell) | ❌ nicht verfügbar | Liegt in der Zukunft |

**Trainingslogik:** Das Modell wird auf historischen Daten trainiert, wobei Features und Target
so aufgebaut werden, wie sie bei einer echten 7-Tage-Prognose vorliegen würden (kein Look-Ahead).
`load_lag_168h` ist dabei der Anker: Der Wochenrhythmus der Netzlast ist der stärkste
strukturelle Prädiktor und bleibt bei 7-Tage-Horizont immer verfügbar.

**Qualitätsziele Modell A:**
- MAPE Netzlast < 8 % (kurzfristig, bereits erreicht)
- MAPE Netzlast 7-Tage-Horizont < 15 %
- MAE Netzpreis < 0.5 Rp./kWh

---

### Modell B – Dynamischer Energiepreis (EPEX Day-Ahead CH)

**Ziel:** `price_eur_mwh` direkt prognostizieren — kein Groupe-E / CKW als Feature
(diese sind selbst EPEX-Transformationen und würden den Trainingshorizont auf ~3 Monate
beschränken). Mit reinen ENTSO-E + Wetter-Features: 2+ Jahre nach Backfill.

**Zwei Prognosehorizonte:**

#### B.1 – Day-Ahead (D+1, höchste Genauigkeit)

Entspricht dem realen EPEX-Publikationszyklus: Preis wird um ~13:00 für den Folgetag
publiziert. Features sind alles, was bis ~12:00 bekannt ist.

**Verfügbarkeitslogik ENTSO-E:**
- *Actual*-Daten (A75, A65/A16, A11): nur als Lag verfügbar (Vergangenheit)
- *Forecast*-Daten (A65/A01 Load, A69/A01 Generation): für D+1 publiziert ✅

| Feature | Quelle | Verfügbar D+1 | Begründung |
|---|---|---|---|
| `epex_lag_24h` | ENTSO-E A44 | ✅ | Preis gleiche Stunde gestern |
| `epex_lag_168h` | ENTSO-E A44 | ✅ | Wochenrhythmus-Anker |
| `rolling_avg_24h / 7d` | berechnet | ✅ | aktuelles Preisniveau |
| `temperature_forecast` | Open-Meteo D+1 | ✅ | Heiz-/Kühlnachfrage CH |
| `shortwave_radiation_forecast` | Open-Meteo D+1 | ✅ | Solar CH |
| `wind_speed_forecast_ch` | Open-Meteo D+1 (47.5N, 8.75E) | ✅ | Wind CH |
| **`wind_speed_forecast_de_nord`** | **Open-Meteo D+1 (53.5N, 10.0E)** | ✅ | **DE Windproduktion — stärkster Preistreiber** |
| **`wind_speed_forecast_de_sued`** | **Open-Meteo D+1 (48.5N, 9.0E)** | ✅ | **Solar-Proxy DE** |
| `cloud_cover_forecast` | Open-Meteo D+1 | ✅ | Solar-Dämpfung |
| `load_forecast_ch` | ENTSO-E A65/A01 | ✅ | Lastprognose CH morgen (bereits gesammelt) |
| `actual_load_ch_lag_24h` | ENTSO-E A65/A16 | ✅ | Actual Load gestern (Lag) |
| `hydro_ror_ch_lag_24h` | ENTSO-E A75/B12 | ✅ | CH Laufwasser gestern (Lag) |
| `solar_gen_ch_lag_24h` | ENTSO-E A75/B16 | ✅ | CH Solar-Produktion gestern (Lag) |
| `wind_gen_de_lag_24h` | ENTSO-E A75/B19 | ✅ | DE Windproduktion gestern (Lag) |
| `flow_ch_de_lag_24h` | ENTSO-E A11 | ✅ | Exportfluss CH→DE gestern (Lag) |
| `net_position_ch_lag_24h` | ENTSO-E A11 (4 Grenzen) | ✅ | CH Netto-Import/-Export gestern |
| `discharge_m3s`, `level_masl` | BAFU | ✅ | Hydro-Speicher CH (langsam veränderlich) |
| `hour_of_day`, `day_of_week`, `month` | deterministisch | ✅ | Tages-/Wochenmuster |
| `is_weekend`, `is_holiday_zh` | deterministisch | ✅ | Nachfragestruktur |
| Generation Forecast Wind/Solar (A69) | ENTSO-E A69/A01 | ⚠️ | Noch nicht gesammelt — ETL-Erweiterung möglich |

#### B.2 – Woche-Ahead (D+7, Planungshorizont)

Keine ENTSO-E-Echtzeit-Forecasts verfügbar — rein lag- und wetterbasiert.
Gleiche Logik wie Modell A: `epex_lag_168h` als Wochenanker.
Die ENTSO-E Actual-Daten (Generation, Load, Flows) fliessen als `lag_168h` ein.

| Feature | D+7 verfügbar? | Anmerkung |
|---|---|---|
| `epex_lag_168h` | ✅ | Preis vor exakt 7 Tagen |
| `epex_lag_336h` | ✅ | Preis vor 14 Tagen |
| `rolling_avg_7d / 30d` | ✅ | aktuelles Preisniveau |
| Wetterprognose CH (Temp, Solar, Wind, Cloud) | ✅ | Open-Meteo 7-Tage |
| **Wetterprognose DE-Nord** (Wind) | ✅ | wichtigster externer Treiber |
| **Wetterprognose DE-Süd** (Solar) | ✅ | Solar-Einspeisung DE |
| `actual_load_ch_lag_168h` | ✅ | Last gleiche Stunde letzte Woche |
| `hydro_ror_ch_lag_168h` | ✅ | CH Laufwasser letzte Woche |
| `solar_gen_ch_lag_168h` | ✅ | CH Solar letzte Woche |
| `wind_gen_de_lag_168h` | ✅ | DE Wind letzte Woche |
| `net_position_ch_lag_168h` | ✅ | CH Netto-Position letzte Woche |
| `discharge_m3s`, `level_masl` | ✅ | BAFU, langsam veränderlich |
| `is_weekend`, `is_holiday_zh`, Kalender | ✅ | deterministisch |
| `load_forecast_ch` | ❌ | nur D+1 von ENTSO-E |
| ENTSO-E Generation Forecast (A69) | ❌ | nur D+1 |

**Warum Wetterdaten aus Norddeutschland?**
Der Schweizer EPEX-Preis ist Teil des zentraleuropäischen Markts. Wenn es in Norddeutschland
(Hamburg-Region) stürmt, fluten Hunderte GWh Windstrom ins Netz → Preise fallen
CE-weit, auch in CH. Open-Meteo liefert historische und Forecast-Daten für beliebige
Koordinaten — die DE-Nord-Wetterdaten sind daher der wichtigste noch nicht genutzte Hebel.

**Neue Datenquellen (ETL-Erweiterung nötig):**
- `open_meteo_de_nord`: lat=53.5, lon=10.0 (Hamburg-Region, Windproxy)
- `open_meteo_de_sued`: lat=48.5, lon=9.0 (Stuttgart-Region, Solar-DE-Proxy)

**Training:**
- Target: `price_eur_mwh` (ENTSO-E A44, verfügbar seit 2015)
- Feature-Set für D+1: ~15 Features (Tabelle B.1 oben)
- Feature-Set für D+7: ~12 Features (Tabelle B.2, ohne ENTSO-E-Echtzeit)
- Split: train 2024–Jan 2026 / val Feb–März 2026 / test Apr 2026+
- Keine Groupe-E / CKW Features (sind EPEX-Transformationen → Near-Leakage)

**Qualitätsziele Modell B:**
- MAPE D+1 < 15 % (EPEX ist volatiler als Last)
- MAPE D+7 < 25 %
- Korrelation mit EKZ Energietarif (sobald Jahresdaten vorhanden) > 0.85

**Erweiterung ab Jan 2027:**
Ein volles Jahr EKZ-Energietarif-Daten liegt vor. Dann: `k_pe` und `k_le` der EKZ-Formel
via SCIP kalibrieren (Preisparität-Bedingung), sodass `pred_epex → EKZ-Energiepreis [Rp./kWh]`
direkt ausgegeben werden kann.

```python
# Zukünftige Kalibrierung (Jan 2027):
ekz_energie_t = clip(k_pe * epex_pred_t + k_le,
                     lower=standardtarif - 5,
                     upper=standardtarif + 5)   # Rp./kWh
```

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
