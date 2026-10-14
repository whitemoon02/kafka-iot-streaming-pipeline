"""Consommateur Kafka : valide, détecte les anomalies, agrège et écrit dans PostgreSQL.

Les événements rejetés sont aussi republiés sur un topic « dead letter » (`sensor-readings-dlq`).
Les offsets ne sont commités qu'après écriture en base (at-least-once + dédoublonnage).
"""
import json
import os

from confluent_kafka import Consumer, Producer

from app.processing import StreamProcessor
from app.storage import Storage

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor-readings")
DLQ = f"{TOPIC}-dlq"
BATCH = int(os.getenv("CONSUMER_BATCH", "200"))


def main() -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "iot-processor",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    dlq = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC])
    proc, db = StreamProcessor(), Storage()
    print(f"[consumer] écoute de {TOPIC}")
    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH, timeout=1.0)
            aggs, anomalies, rejected = [], [], []
            for m in msgs:
                if m.error():
                    print(f"[consumer] erreur : {m.error()}")
                    continue
                try:
                    event = json.loads(m.value())
                except json.JSONDecodeError:
                    dlq.produce(DLQ, value=m.value())
                    continue
                res = proc.process(event)
                aggs += res["aggregates"]
                if res["anomaly"]:
                    anomalies.append(res["anomaly"])
                    print(f"⚠️  anomalie {event['tank_id']} {event['metric']}={event['value']} z={res['anomaly']['z_score']}")
                if res["rejected"]:
                    rejected.append(res["rejected"])
                    dlq.produce(DLQ, value=json.dumps(res["rejected"]))
            if msgs:
                db.insert("sensor_aggregates", aggs)
                db.insert("anomalies", anomalies)
                db.insert("rejected_events", rejected)
                consumer.commit(asynchronous=False)
                dlq.poll(0)
    finally:
        consumer.close()
        print(f"[consumer] stats : {dict(proc.stats)}")


if __name__ == "__main__":
    main()
