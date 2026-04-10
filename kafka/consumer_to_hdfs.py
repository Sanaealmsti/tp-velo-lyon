"""
Consumer Kafka à HDFS via WebHDFS.

Partitionne par l'heure de l'événement (last_update) et pas l'heure système.
C'est la différence 'event time' vs 'processing time' — fondamental en streaming.
"""
import json
import os
import signal
import time
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv
from hdfs import InsecureClient
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
TOPIC = "velo_lyon_raw"

FLUSH_EVERY_N = 250
FLUSH_EVERY_SEC = 30


def hour_partition(last_update_ms):
    if not last_update_ms:
        return datetime.utcnow().strftime("%Y-%m-%d-%H")
    dt = datetime.utcfromtimestamp(last_update_ms / 1000)
    return dt.strftime("%Y-%m-%d-%H")


def run():
    hdfs = InsecureClient(HDFS_URL, user=HDFS_USER)
    print(f" Connecté à HDFS {HDFS_URL}")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="velo_lyon_hdfs_writer",
    )
    print(f" Consumer abonné au topic {TOPIC}")

    buffers = defaultdict(list)
    last_flush = time.time()
    total_written = 0

    running = {"flag": True}
    signal.signal(signal.SIGINT, lambda *_: running.update(flag=False))
    signal.signal(signal.SIGTERM, lambda *_: running.update(flag=False))

    def flush_all():
        nonlocal total_written, last_flush
        for hour, msgs in list(buffers.items()):
            if not msgs:
                continue
            hdfs_dir = f"/data-lake/raw/velo_lyon/{hour}"
            filename = f"batch_{int(time.time() * 1000)}.jsonl"
            hdfs_path = f"{hdfs_dir}/{filename}"
            hdfs.makedirs(hdfs_dir, permission="777")
            content = "\n".join(json.dumps(m, ensure_ascii=False) for m in msgs) + "\n"
            with hdfs.write(hdfs_path, encoding="utf-8", overwrite=True) as writer:
                writer.write(content)
            print(f" {len(msgs):4d} msgs → {hdfs_path}")
            total_written += len(msgs)
            buffers[hour] = []
        last_flush = time.time()

    print(" Écoute du topic... (Ctrl+C pour arrêter)")
    try:
        while running["flag"]:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for msg in messages:
                    station = msg.value
                    buffers[hour_partition(station.get("last_update"))].append(station)

            total = sum(len(v) for v in buffers.values())
            if total >= FLUSH_EVERY_N or (time.time() - last_flush) >= FLUSH_EVERY_SEC:
                flush_all()
    finally:
        print(" Flush final...")
        flush_all()
        consumer.close()
        print(f" Consumer arrêté. Total : {total_written} messages")


if __name__ == "__main__":
    run()
