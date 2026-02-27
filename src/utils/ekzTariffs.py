import requests
import json
from datetime import datetime

# Einstellungen
URL = "https://api.tariffs.ekz.ch/v1/tariffs"
TARGET_DATE = datetime.now().strftime("%Y-%m-%d")
REGION_NAME = "Sihl" # Fokusregion 

params = {
    "date": TARGET_DATE,
    "tariffType": "dynamic"
}

try:
    response = requests.get(URL, params=params)
    response.raise_for_status()
    data = response.json()

    # Region manuell zu den Metadaten im JSON hinzu
    data["metadata"] = {
        "region": REGION_NAME,
        "description": "Repräsentative Daten für das EKZ-Versorgungsgebiet (Limmattal)"
    }

    filename = f"ekz_15min_{REGION_NAME}_{TARGET_DATE}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
        
    print(f"Datei gespeichert: {filename}")
    print(f"Info: EKZ-Tarife sind für alle Regionen (inkl. {REGION_NAME}) identisch.")

except Exception as e:
    print(f"Fehler: {e}")
