"""Mode démo sans Docker ni Kafka : simule 2 heures de mesures et les fait passer dans le même traitement.

Usage : python -m app.run_local --minutes 120
"""
import argparse
import random
from datetime import datetime, timedelta, timezone

from app.processing import StreamProcessor
from app.sensors import TankSimulator
from app.storage import Storage


def main(minutes: int, db_path: str) -> dict:
    sim, proc, db = TankSimulator(seed=7), StreamProcessor(), Storage(db_path)
    rng = random.Random(7)
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    aggs, anomalies, rejected = [], [], []
    for second in range(0, minutes * 60, 5):           # une lecture toutes les 5 s
        events = [r.to_dict() for r in sim.read(t0 + timedelta(seconds=second))]
        if rng.random() < 0.02:                          # rejoue un message (doublon Kafka)
            events.append(dict(events[0]))
        for e in events:
            res = proc.process(e)
            aggs += res["aggregates"]
            anomalies += [res["anomaly"]] if res["anomaly"] else []
            rejected += [res["rejected"]] if res["rejected"] else []
    aggs += proc.aggregator.flush()
    db.insert("sensor_aggregates", aggs)
    db.insert("anomalies", anomalies)
    db.insert("rejected_events", rejected)

    print("Statistiques du flux :", dict(proc.stats))
    print("\nAnomalies par cuve :")
    for row in db.query("SELECT tank_id, metric, COUNT(*) FROM anomalies GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 8"):
        print("  ", row)
    print("\nMotifs de rejet :")
    for row in db.query("SELECT reason, COUNT(*) FROM rejected_events GROUP BY 1"):
        print("  ", row)
    return dict(proc.stats)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=int, default=120)
    p.add_argument("--db", default="data/iot.db")
    a = p.parse_args()
    main(a.minutes, a.db)
