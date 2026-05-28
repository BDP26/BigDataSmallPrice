---
title: "BigDataSmallPrice: Wann ist Strom in Winterthur morgen günstig?"
authors: "Ryan Bachmann, Dinis Silva Miguel, Gian Ruchti"
category: "Forschung"
tags: ["Big Data", "Machine Learning", "Energie", "Dynamische Tarife", "Winterthur"]
---

# BigDataSmallPrice: Wann ist Strom in Winterthur morgen günstig?

Die Waschmaschine läuft, das E-Auto lädt, die Wärmepumpe springt an. Für viele Haushalte spielt es heute kaum eine Rolle, wann genau Strom verbraucht wird. Mit dynamischen Stromtarifen ändert sich das: Strom kann je nach Tageszeit, Netzbelastung und Marktsituation deutlich günstiger oder teurer sein. Wer den richtigen Zeitpunkt kennt, kann Kosten sparen und gleichzeitig das Stromnetz entlasten.

Genau hier setzt unser Projekt **BigDataSmallPrice** an. Im Rahmen des Moduls PM4 an der ZHAW School of Engineering haben wir eine Plattform entwickelt, die dynamische Stromtarife für Winterthur prognostiziert. Die Leitfrage war einfach formuliert, technisch aber anspruchsvoll: **Wie genau lässt sich vorhersagen, wann Strom in Winterthur in den nächsten Tagen günstig oder teuer wird?**

## Von Rohdaten zur Tarifprognose

Dynamische Tarife bestehen nicht nur aus einem einzigen Preis. Für Endkundinnen und Endkunden zählt am Ende der Gesamttarif in Rappen pro Kilowattstunde. Dieser setzt sich vereinfacht aus zwei Teilen zusammen:

- dem Energiepreis, der stark vom europäischen Day-Ahead-Strommarkt beeinflusst wird
- dem Netzpreis, der von der lokalen Netzlast abhängt

Um beide Komponenten prognostizieren zu können, verbindet BigDataSmallPrice mehrere Datenquellen. Dazu gehören Day-Ahead-Preise und Lastdaten der ENTSO-E Transparency Platform, Wetterdaten von Open-Meteo, hydrologische Daten des BAFU, Tarifdaten von EKZ, CKW und Groupe E sowie offene Last- und PV-Daten von Stadtwerk Winterthur.

Diese Daten kommen in unterschiedlichen Formaten, Auflösungen und Zeitzonen. Deshalb war ein grosser Teil des Projekts keine Modellierung im engeren Sinn, sondern saubere Datenarbeit: abrufen, prüfen, vereinheitlichen, speichern und für Machine-Learning-Modelle aufbereiten. Die Pipeline läuft über Apache Airflow, speichert Zeitreihen in TimescaleDB und exportiert Trainingsdaten als Parquet-Dateien.

## Zwei Modelle statt einer Blackbox

Wir haben die Prognose bewusst in zwei Teilprobleme aufgeteilt. **Modell A** prognostiziert die lokale Nettolast in Winterthur. Dafür nutzt es unter anderem Kalendermerkmale, Wetterprognosen, Feiertage sowie Lastwerte von gestern und von der Vorwoche. Aus der prognostizierten Netzlast wird anschliessend ein dynamischer Netzpreis berechnet.

**Modell B** prognostiziert den EPEX-Day-Ahead-Preis für die Schweiz. Neben vergangenen Strompreisen fliessen Wetterdaten, Lastprognosen, Produktionsdaten und internationale Einflussgrössen ein. Besonders spannend war dabei der Blick über die Schweizer Grenze: Wind in Norddeutschland kann den zentraleuropäischen Strommarkt stark beeinflussen und damit auch die Schweizer Preise bewegen.

Für beide Modelle haben wir verschiedene Verfahren getestet, darunter lineare Modelle, LSTM, Transformer-Varianten und XGBoost. Am Ende überzeugte XGBoost am meisten: Es trainiert schnell, kommt gut mit tabellarischen Zeitreihendaten zurecht und lieferte in unserem Setup die besten Resultate.

## Was kam heraus?

Die Ergebnisse zeigen, dass sich dynamische Stromtarife mit öffentlich verfügbaren Daten erstaunlich gut prognostizieren lassen. Im Test erreichte das Lastmodell einen mittleren prozentualen Fehler von **3,99 Prozent**. Das Energiepreismodell lag bei **7,72 Prozent**. Beide Werte liegen deutlich unter unseren ursprünglichen Qualitätszielen.

Für Nutzerinnen und Nutzer wird daraus eine 7-Tage-Prognose im 15-Minuten-Raster. Das Dashboard zeigt nicht nur eine Preiskurve, sondern markiert auch günstige, mittlere und teure Zeitfenster. So lässt sich zum Beispiel erkennen, wann das Laden eines E-Autos oder der Betrieb einer Waschmaschine besonders sinnvoll ist.

## Big Data im Kleinen

Der Projektname ist mit einem Augenzwinkern gemeint, trifft aber den Kern: Aus vielen kleinen Datenpunkten entsteht ein konkreter Nutzen. Für jeden einzelnen Haushalt geht es vielleicht nur um einige Rappen pro Kilowattstunde. Über viele Geräte, Haushalte und Tage hinweg entsteht daraus aber ein relevantes Potenzial.

Auch aus technischer Sicht war das Projekt ein typischer Big-Data-Anwendungsfall. Nicht weil ein einzelner Datensatz gigantisch wäre, sondern weil viele heterogene Quellen zuverlässig zusammengeführt werden müssen. Die Herausforderung lag in der Kombination aus Datenvolumen, Aktualität, Datenqualität und Automatisierung.

## Was wir gelernt haben

Die wichtigste Erkenntnis: Gute Prognosen beginnen nicht beim Modell, sondern bei der Datenpipeline. Fehlende Werte, API-Limits, verspätete Veröffentlichungen oder unterschiedliche Zeitraster beeinflussen das Ergebnis stärker, als man auf den ersten Blick vermuten würde. Apache Airflow und TimescaleDB halfen uns, diese Komplexität beherrschbar zu machen.

Eine zweite Erkenntnis betrifft die Modellwahl. Komplexere Modelle sind nicht automatisch besser. In unserem Projekt schnitten LSTM und Transformer zwar solide ab, brauchten aber deutlich mehr Trainingszeit und erreichten nicht die Genauigkeit von XGBoost. Für strukturierte Energiedaten war ein robustes Gradient-Boosting-Modell die pragmatischere Wahl.

## Ausblick

BigDataSmallPrice ist als Projektplattform entstanden, zeigt aber ein reales Anwendungsszenario. Denkbar wären Erweiterungen auf weitere Schweizer Netzgebiete, eine direkte Anbindung an steuerbare Geräte oder eine automatische Planung von Lade- und Betriebszeiten. Besonders spannend wäre auch eine laufende Aktualisierung mit Echtzeitdaten, damit Prognosen im Tagesverlauf präziser werden.

Dynamische Stromtarife machen den Strommarkt für Endkundinnen und Endkunden flexibler, aber auch komplexer. Unser Projekt zeigt: Mit offenen Daten, sauberer Datenarchitektur und Machine Learning lässt sich diese Komplexität in konkrete Handlungsempfehlungen übersetzen.

**Projekt:** BigDataSmallPrice  
**Team:** Ryan Bachmann, Dinis Silva Miguel, Gian Ruchti  
**Kontext:** PM4, ZHAW School of Engineering  
**Repository:** https://github.com/BDP26/BigDataSmallPrice
