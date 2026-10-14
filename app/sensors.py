"""Simulation de capteurs IoT sur des cuves de fermentation industrielles."""
from __future__ import annotations

import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

# Plages nominales par type de mesure
NOMINAL = {
    "temperature": (30.0, 0.6),   # °C (moyenne, écart-type)
    "ph": (5.0, 0.08),
    "pressure": (1.2, 0.03),      # bar
    "dissolved_o2": (40.0, 2.0),  # %
}


@dataclass
class Reading:
    event_id: str
    tank_id: str
    metric: str
    value: float | None
    ts: str

    def to_dict(self) -> dict:
        return asdict(self)


class TankSimulator:
    """Génère des mesures réalistes avec dérives, pics et données corrompues occasionnelles."""

    def __init__(self, n_tanks: int = 6, anomaly_rate: float = 0.01, corrupt_rate: float = 0.005, seed: int | None = None):
        self.rng = random.Random(seed)
        self.tanks = [f"TANK-{i:02d}" for i in range(1, n_tanks + 1)]
        self.anomaly_rate = anomaly_rate
        self.corrupt_rate = corrupt_rate
        self.drift = {t: 0.0 for t in self.tanks}

    def read(self, now: datetime | None = None) -> list[Reading]:
        ts = (now or datetime.now(timezone.utc)).isoformat()
        out = []
        for tank in self.tanks:
            # dérive lente de la température (encrassement, consigne)
            self.drift[tank] = max(-1.5, min(1.5, self.drift[tank] + self.rng.gauss(0, 0.02)))
            for metric, (mu, sigma) in NOMINAL.items():
                value = self.rng.gauss(mu, sigma)
                if metric == "temperature":
                    value += self.drift[tank]
                if self.rng.random() < self.anomaly_rate:
                    value += self.rng.choice([-1, 1]) * sigma * self.rng.uniform(6, 10)
                if self.rng.random() < self.corrupt_rate:
                    value = self.rng.choice([None, -999.0])
                out.append(Reading(str(uuid.uuid4()), tank, metric,
                                   None if value is None else round(value, 3), ts))
        return out
