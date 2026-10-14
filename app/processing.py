"""Logique de traitement du flux, indépendante de Kafka (donc testable unitairement).

- validation de schéma et de plages physiques
- dédoublonnage par event_id
- détection d'anomalies par z-score glissant (par cuve et par mesure)
- agrégation en fenêtres fixes (tumbling windows) d'une minute
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime

REQUIRED = {"event_id", "tank_id", "metric", "value", "ts"}
PHYSICAL_RANGE = {
    "temperature": (0.0, 80.0),
    "ph": (0.0, 14.0),
    "pressure": (0.0, 10.0),
    "dissolved_o2": (0.0, 100.0),
}


def validate(event: dict) -> tuple[bool, str | None]:
    """Retourne (valide, raison_du_rejet)."""
    missing = REQUIRED - event.keys()
    if missing:
        return False, f"champs manquants: {sorted(missing)}"
    if event["metric"] not in PHYSICAL_RANGE:
        return False, f"mesure inconnue: {event['metric']}"
    v = event["value"]
    if not isinstance(v, (int, float)) or isinstance(v, bool) or math.isnan(v):
        return False, "valeur non numérique"
    lo, hi = PHYSICAL_RANGE[event["metric"]]
    if not lo <= v <= hi:
        return False, f"hors plage physique [{lo}, {hi}]"
    try:
        datetime.fromisoformat(event["ts"])
    except (TypeError, ValueError):
        return False, "horodatage invalide"
    return True, None


class Deduplicator:
    """Garde en mémoire les N derniers event_id (sémantique at-least-once de Kafka)."""

    def __init__(self, capacity: int = 50_000):
        self.seen: set[str] = set()
        self.order: deque[str] = deque()
        self.capacity = capacity

    def is_duplicate(self, event_id: str) -> bool:
        if event_id in self.seen:
            return True
        self.seen.add(event_id)
        self.order.append(event_id)
        if len(self.order) > self.capacity:
            self.seen.discard(self.order.popleft())
        return False


class AnomalyDetector:
    """z-score sur une fenêtre glissante de `window` valeurs (algorithme de Welford non nécessaire ici)."""

    def __init__(self, window: int = 60, threshold: float = 4.0, min_points: int = 20):
        self.window, self.threshold, self.min_points = window, threshold, min_points
        self.history: dict[tuple[str, str], deque[float]] = defaultdict(lambda: deque(maxlen=self.window))

    def score(self, tank: str, metric: str, value: float) -> float | None:
        h = self.history[(tank, metric)]
        z = None
        if len(h) >= self.min_points:
            mean = sum(h) / len(h)
            std = math.sqrt(sum((x - mean) ** 2 for x in h) / (len(h) - 1)) or 1e-9
            z = (value - mean) / std
        if z is None or abs(z) < self.threshold:  # on n'apprend pas sur les anomalies
            h.append(value)
        return z

    def is_anomaly(self, z: float | None) -> bool:
        return z is not None and abs(z) >= self.threshold


@dataclass
class WindowAggregator:
    """Fenêtres fixes : émet les agrégats d'une fenêtre lorsqu'une mesure plus récente arrive."""

    window_seconds: int = 60
    buckets: dict[tuple, list[float]] = field(default_factory=lambda: defaultdict(list))

    def _start(self, ts: str) -> int:
        epoch = int(datetime.fromisoformat(ts).timestamp())
        return epoch - epoch % self.window_seconds

    def add(self, event: dict) -> list[dict]:
        start = self._start(event["ts"])
        self.buckets[(start, event["tank_id"], event["metric"])].append(event["value"])
        return self.flush(before=start)

    def flush(self, before: int | None = None) -> list[dict]:
        closed = [k for k in self.buckets if before is None or k[0] < before]
        out = []
        for key in sorted(closed):
            vals = self.buckets.pop(key)
            out.append({
                "window_start": datetime.fromtimestamp(key[0]).isoformat(),
                "tank_id": key[1], "metric": key[2], "count": len(vals),
                "avg": round(sum(vals) / len(vals), 3), "min": min(vals), "max": max(vals),
            })
        return out


class StreamProcessor:
    """Assemble les briques : un événement entre, des enregistrements à stocker sortent."""

    def __init__(self):
        self.dedup = Deduplicator()
        self.detector = AnomalyDetector()
        self.aggregator = WindowAggregator()
        self.stats = defaultdict(int)

    def process(self, event: dict) -> dict:
        result = {"rejected": None, "anomaly": None, "aggregates": []}
        self.stats["received"] += 1
        ok, reason = validate(event)
        if not ok:
            self.stats["rejected"] += 1
            result["rejected"] = {**event, "reason": reason}
            return result
        if self.dedup.is_duplicate(event["event_id"]):
            self.stats["duplicates"] += 1
            return result
        z = self.detector.score(event["tank_id"], event["metric"], event["value"])
        if self.detector.is_anomaly(z):
            self.stats["anomalies"] += 1
            result["anomaly"] = {**event, "z_score": round(z, 2)}
        result["aggregates"] = self.aggregator.add(event)
        self.stats["valid"] += 1
        return result
