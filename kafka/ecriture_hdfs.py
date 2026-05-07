#lecture du topic kafka et écriture dans hdfs
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from hdfs import InsecureClient
from kafka import KafkaConsumer

load_dotenv()

hdfs = InsecureClient(os.getenv("HDFS_URL", "http://localhost:9870"), user="hadoop")

consumer = KafkaConsumer(
    "velo_lyon_raw",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="velo_lyon_hdfs_writer")

while True:
    tout = []
    debut = time.time()

    #Collecte pendant 30 secondes
    while time.time() - debut < 30:
        messages = consumer.poll(timeout_ms=1000)
        for topic, liste in messages.items():
            for msg in liste:
                tout.append(msg.value)

    if not tout:
        continue

    #Ecriture dans hdfs
    heure = datetime.utcnow().strftime("%Y-%m-%d-%H")
    dossier = "/data-lake/raw/velo_lyon/" + heure
    fichier = dossier + "/batch_" + str(int(time.time())) + ".jsonl"

    hdfs.makedirs(dossier, permission="777")

    lignes = ""
    for s in tout:
        lignes = lignes + json.dumps(s) + "\n"

    with hdfs.write(fichier, encoding="utf-8", overwrite=True) as f:
        f.write(lignes)
