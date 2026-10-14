CREATE TABLE IF NOT EXISTS sensor_aggregates (
    window_start TEXT NOT NULL,
    tank_id      TEXT NOT NULL,
    metric       TEXT NOT NULL,
    count        INTEGER,
    avg          REAL,
    min          REAL,
    max          REAL,
    PRIMARY KEY (window_start, tank_id, metric)
);

CREATE TABLE IF NOT EXISTS anomalies (
    event_id TEXT PRIMARY KEY,
    tank_id  TEXT,
    metric   TEXT,
    value    REAL,
    z_score  REAL,
    ts       TEXT
);

CREATE TABLE IF NOT EXISTS rejected_events (
    event_id TEXT,
    tank_id  TEXT,
    metric   TEXT,
    value    REAL,
    ts       TEXT,
    reason   TEXT
);
