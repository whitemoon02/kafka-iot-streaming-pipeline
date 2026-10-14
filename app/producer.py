"""Producteur Kafka : publie les mesures des capteurs sur le topic `sensor-readings`."""
import json
import os
import time

from confluent_kafka import Producer

from app.sensors import TankSimulator

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor-readings")
INTERVAL = float(os.getenv("PRODUCE_INTERVAL_S", "1"))


def on_delivery(err, msg):
    if err:
        print(f"[producer] échec de livraison : {err}")


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True,
                         "linger.ms": 50, "compression.type": "lz4"})
    sim = TankSimulator()
    sent = 0
    print(f"[producer] envoi vers {BOOTSTRAP}/{TOPIC}")
    while True:
        for r in sim.read():
            # clé = cuve -> toutes les mesures d'une cuve vont dans la même partition (ordre garanti)
            producer.produce(TOPIC, key=r.tank_id, value=json.dumps(r.to_dict()), callback=on_delivery)
            sent += 1
        producer.poll(0)
        if sent % 1000 < len(sim.tanks) * 4:
            print(f"[producer] {sent} messages envoyés")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
