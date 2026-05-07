#Appel à l'api jcdecaux et envoi dans kafka
import json
import os
import sys
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

cle = os.getenv("JCDECAUX_API_KEY")
if not cle:
    sys.exit("pas de cle API")

url = "https://api.jcdecaux.com/vls/v1/stations?contract=lyon&apiKey=" + cle

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

while True:
    try:
        r = requests.get(url, timeout=10)
        stations = r.json()
        for s in stations:
            producer.send("velo_lyon_raw", value=s)
        producer.flush()
    except Exception:
        pass
    time.sleep(60)
