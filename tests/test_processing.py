from app.processing import AnomalyDetector, Deduplicator, StreamProcessor, WindowAggregator, validate

BASE = {"event_id": "e1", "tank_id": "TANK-01", "metric": "ph", "value": 5.0, "ts": "2026-01-01T00:00:00+00:00"}


def test_validate_accepts_valid_event():
    assert validate(BASE) == (True, None)


def test_validate_rejects_bad_events():
    assert not validate({**BASE, "value": None})[0]
    assert not validate({**BASE, "value": -999.0})[0]
    assert not validate({**BASE, "metric": "humidity"})[0]
    assert not validate({**BASE, "ts": "hier"})[0]
    assert not validate({k: v for k, v in BASE.items() if k != "tank_id"})[0]


def test_deduplicator():
    d = Deduplicator(capacity=2)
    assert not d.is_duplicate("a")
    assert d.is_duplicate("a")
    d.is_duplicate("b"); d.is_duplicate("c")      # "a" sort de la mémoire
    assert not d.is_duplicate("a")


def test_anomaly_detector_flags_spike():
    det = AnomalyDetector(window=50, threshold=4, min_points=20)
    for i in range(30):
        det.score("T", "ph", 5.0 + (i % 3) * 0.01)
    assert det.is_anomaly(det.score("T", "ph", 7.0))
    assert not det.is_anomaly(det.score("T", "ph", 5.01))


def test_window_aggregator_closes_previous_window():
    agg = WindowAggregator(window_seconds=60)
    assert agg.add({**BASE, "value": 4.0}) == []
    assert agg.add({**BASE, "value": 6.0, "ts": "2026-01-01T00:00:30+00:00"}) == []
    out = agg.add({**BASE, "value": 5.0, "ts": "2026-01-01T00:01:05+00:00"})
    assert len(out) == 1 and out[0]["count"] == 2 and out[0]["avg"] == 5.0


def test_processor_counts():
    p = StreamProcessor()
    p.process(BASE); p.process(BASE); p.process({**BASE, "event_id": "e2", "value": None})
    assert p.stats["valid"] == 1 and p.stats["duplicates"] == 1 and p.stats["rejected"] == 1
