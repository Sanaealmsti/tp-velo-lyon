"""
Producer Kafka — tape l'API JCDecaux toutes les 60s et pousse chaque station
comme un message dans le topic `velo_lyon_raw`.

Clé du message = station_id à même station = même partition Kafka = ordre préservé.
"""
import json
import os
import signal
import sys
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()

API_KEY = os.getenv("JCDECAUX_API_KEY")
CONTRACT = "lyon"
API_URL = f"https://api.jcdecaux.com/vls/v1/stations?contract={CONTRACT}&apiKey={API_KEY}"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "velo_lyon_raw"
POLL_INTERVAL_SEC = 60

if not API_KEY:
    sys.exit(" JCDECAUX_API_KEY manquant dans .env")


def make_producer(retries=10):
    for attempt in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
            )
        except NoBrokersAvailable:
            print(f" Kafka pas prêt, retry {attempt + 1}/{retries}...")
            time.sleep(5)
    sys.exit(" Impossible de joindre Kafka")


def fetch_stations():
    try:
        r = requests.get(API_URL, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"  Erreur API : {e}")
        return []


def run():
    producer = make_producer()
    print(f"Producer connecté à {KAFKA_BOOTSTRAP}, topic={TOPIC}")

    running = {"flag": True}
    signal.signal(signal.SIGINT, lambda *_: running.update(flag=False))
    signal.signal(signal.SIGTERM, lambda *_: running.update(flag=False))

    cycle = 0
    while running["flag"]:
        cycle += 1
        stations = fetch_stations()
        ts = datetime.utcnow().isoformat()

        for station in stations:
            station["ingested_at"] = ts
            producer.send(TOPIC, key=station.get("number"), value=station)

        producer.flush()
        print(f"[{ts}] cycle #{cycle} → {len(stations)} stations envoyées")

        for _ in range(POLL_INTERVAL_SEC):
            if not running["flag"]:
                break
            time.sleep(1)

    producer.close()
    print(" Producer arrêté proprement")


if __name__ == "__main__":
    run()
