
# Big Data Small Price
## Vorhersage dynamischer Strompreise unter Einbezug von Echtzeit-Wetterdaten und Day-Ahead-Preisen
### Big Data Projekt DS23t PM4

**Team:** [Bachmann Ryan, Dinis Silva Miguel, Ruchti Gian]

---

## Problemstellung

Im Rahmen der Schweizer Energiewende sind Netzbetreiber ab 2026 verpflichtet, ihren Kunden dynamische Stromtarife anzubieten. Strompreise reagieren heute kurzfristig auf Angebots- und Nachfrageschwankungen, insbesondere auf erneuerbare Einspeisung (Photovoltaik, Wind) sowie Lastspitzen. Für Endverbraucher sind diese Preisfluktuationen jedoch nur eingeschränkt transparent und kaum prognostizierbar, obwohl sie ein erhebliches Potenzial zur Kostenoptimierung und zur Netzstabilisierung bieten.

Ziel dieses Projekts ist die Entwicklung eines datengetriebenen Prognosemodells für dynamische Strompreise unter Einbezug von Day-Ahead-Marktpreisen (ENTSO-E) und Echtzeit-Wetterdaten (MeteoSchweiz) und der API des Elektrizitätswerk Zürich (EKZ). Durch die Verknüpfung mehrerer grosser, heterogener Datenquellen und die Verarbeitung historischer sowie nahezu Echtzeit-Daten entsteht eine skalierbare Big-Data-Pipeline.

Die Neuheit des Projekts liegt in:

- der Integration von Markt-, Wetter- und Zeitreihendaten in einem konsistenten Prognosesystem  
- der Ableitung konkreter Handlungsempfehlungen für Haushalte  
- der Visualisierung der Ergebnisse in einem interaktiven Dashboard für den Standort Winterthur

---

## Fragestellung / Ziel

> Wie genau lassen sich kurzfristige Strompreise für den Standort Winterthur durch die Kombination von Day-Ahead-Preisen und meteorologischen Echtzeitdaten prognostizieren?

Daraus ergeben sich folgende Projektziele:

- Aufbau einer automatisierten Datenpipeline zur Aggregation von ENTSO-E- und MeteoSchweiz-Daten  
- Entwicklung eines Zeitreihenmodells zur Vorhersage von Strompreisen für die nächsten 24 Stunden  
- Quantitative Evaluation des Modells mit den dynamischen Preisen von EKZ
- Onboaring prozess für user, um deren Stromverbrauch zu ermitteln
- Visualisierung der Ergebnisse in einem interaktiven Dashboard für den Standort Winterthur

---

## Vorgehen / Methode / Daten

### Datenquellen

- ENTSO-E Transparency Platform (Day-Ahead-Preise, Markt- und Lastdaten)  
- MeteoSchweiz API (Temperatur, Globalstrahlung, Bewölkung, Wind, Niederschlag)
- EKZ (dynamische Strompreise)
- BFU Wasserpegel, der Flüsse und Seen in der Schweiz

Die Daten umfassen historische Zeitreihen sowie laufend aktualisierte Werte und ergeben in Kombination mehrere Millionen Datenpunkte.

---

### Methodischer Ansatz

1. Aufbau einer ETL-Pipeline (Extraction, Transformation, Loading) und speichern in einer Datenbank  
2. Feature Engineering (z.B. Lag-Variablen, gleitende Mittelwerte, Interaktionen zwischen Wetter und Preis)  
3. Erstellung einer Export-Pipeline, die Daten aus der Datenbank ausliest und für die effiziente Modellierung in Big-Data-Formaten (`.h5` oder `.parquet`) speichert  
4. Modellierung mittels Machine-Learning-Verfahren (z.B. XGBoost, Random Forest oder LSTM für Zeitreihen), welche die exportierten Dateien nutzen  
5. Evaluation der Modellgüte anhand historischer Testperioden  

---

### Geplantes Dashboard

Das Endprodukt umfasst:

- Preisprognose mit Unsicherheitsintervall  
- Markierung günstiger und teurer Zeitfenster  
- Gerätespezifische Empfehlungen (z.B. Waschmaschine 13:00–15:00)  
- Erklärkomponente zur Preisentwicklung (z.B. hohe PV-Einspeisung)  

---

### Erwartete Herausforderungen

- Datenlatenz und API-Limitierungen  
- Integration von verschiedenen Datenquellen
- Datenqualität und fehlende Werte  
- Komplexe Korrelationen zwischen Wetter und Marktpreisen  
- User management

---

## Zeitplan und Aufgabenverteilung

| Phase | Aufgabe | Verantwortlich |
|---|---|---|
| Woche 2–3 | Proposal, Requirements & API-Integration (ENTSO-E, MeteoSchweiz, EKZ, BFU) | Alle |
| Woche 4–5 | ETL-Pipeline, Datenbank-Speicherung, EDA & Datenbereinigung | Gian / Miguel |
| Woche 6–7 | Feature Engineering & User Onboarding/Management | Miguel / Ryan |
| Woche 8–9 | Baseline- & fortgeschrittene Modellierung (XGBoost, RF, LSTM) | Gian / Ryan |
| Woche 10–11 | Evaluation mit EKZ-Daten, Optimierung & Backend-Entwicklung | Gian / Miguel |
| Woche 12–13 | Dashboard Frontend, Testing & Integration | Gian |
| Woche 14 | Abschluss-Dokumentation & Präsentation | Miguel / Ryan |

---

## Quellenangaben

- ENTSO-E Transparency Platform: https://transparency.entsoe.eu  
- MeteoSchweiz Open Data / API: https://www.meteoswiss.admin.ch
- EKZ: https://api.tariffs.ekz.ch/swagger/index.html?urls.primaryName=EKZ
- BFU: https://www.bafu.admin.ch/de/datenservice-hydrologie-fuer-fliessgewaesser-und-seen

