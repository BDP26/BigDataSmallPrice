
# Big Data Small Price
## Vorhersage dynamischer Strompreise unter Einbezug von Echtzeit-Wetterdaten und Day-Ahead-Preisen
### Big Data Projekt DS23t PM4

**Team:** [Namen einfügen]

---

## Problemstellung

Im Rahmen der Schweizer Energiewende sind Netzbetreiber ab 2026 verpflichtet, ihren Kunden dynamische Stromtarife anzubieten. Strompreise reagieren heute kurzfristig auf Angebots- und Nachfrageschwankungen, insbesondere auf erneuerbare Einspeisung (Photovoltaik, Wind) sowie Lastspitzen. Für Endverbraucher sind diese Preisfluktuationen jedoch nur eingeschränkt transparent und kaum prognostizierbar, obwohl sie ein erhebliches Potenzial zur Kostenoptimierung und zur Netzstabilisierung bieten.

Ziel dieses Projekts ist die Entwicklung eines datengetriebenen Prognosemodells für dynamische Strompreise unter Einbezug von Day-Ahead-Marktpreisen (ENTSO-E) und Echtzeit-Wetterdaten (MeteoSchweiz). Durch die Verknüpfung mehrerer grosser, heterogener Datenquellen und die Verarbeitung historischer sowie nahezu Echtzeit-Daten entsteht eine skalierbare Big-Data-Pipeline.

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
- Quantitative Evaluation der Prognosegüte mittels MAE und MAPE  
- Implementierung eines Dashboards mit klaren Handlungsempfehlungen

---

## Vorgehen / Methode / Daten

### Datenquellen

- ENTSO-E Transparency Platform (Day-Ahead-Preise, Markt- und Lastdaten)  
- MeteoSchweiz API (Temperatur, Globalstrahlung, Bewölkung, Wind, Niederschlag)

Die Daten umfassen historische Zeitreihen sowie laufend aktualisierte Werte und ergeben in Kombination mehrere Millionen Datenpunkte.

---

### Methodischer Ansatz

1. Aufbau einer ETL-Pipeline (Extraction, Transformation, Loading)  
2. Feature Engineering (z.B. Lag-Variablen, gleitende Mittelwerte, Interaktionen zwischen Wetter und Preis)  
3. Modellierung mittels Regressions- und Machine-Learning-Verfahren (z.B. Gradient Boosting, Random Forest oder LSTM für Zeitreihen)  
4. Evaluation der Modellgüte anhand historischer Testperioden  

Die Prognose basiert formal auf einer Regressionsfunktion der Form:

wobei:

- **P_t** der Strompreis zum Zeitpunkt t  
- **W_t** meteorologische Variablen  
- **D_t** Nachfrageindikatoren  
- **S_t** saisonale Effekte  

darstellen.

---

### Geplantes Dashboard

Das Endprodukt umfasst:

- Preisprognose mit Unsicherheitsintervall  
- Markierung günstiger und teurer Zeitfenster  
- Gerätespezifische Empfehlungen (z.B. Waschmaschine 13:00–15:00)  
- Erklärkomponente zur Preisentwicklung (z.B. hohe PV-Einspeisung)  
- Technische Modellmetriken (MAE, MAPE, Drift-Indikator)

---

### Erwartete Herausforderungen

- Datenlatenz und API-Limitierungen  
- Nichtstationarität der Zeitreihen  
- Strukturbrüche durch regulatorische Änderungen  
- Komplexe Korrelationen zwischen Wetter und Marktpreisen  

---

## Zeitplan und Aufgabenverteilung

| Phase      | Aufgabe                              | Verantwortlich |
|-----------|--------------------------------------|---------------|
| Woche 1–2 | API-Integration, Datenpipeline       | Person A      |
| Woche 3–4 | Feature Engineering                  | Person B      |
| Woche 5–6 | Modelltraining und Evaluation        | Person C      |
| Woche 7   | Dashboard-Implementierung            | Person A + B  |
| Woche 8   | Testing, Dokumentation, Präsentation | Alle          |

---

## Quellenangaben

- ENTSO-E Transparency Platform: https://transparency.entsoe.eu  
- MeteoSchweiz Open Data / API: https://www.meteoswiss.admin.ch
