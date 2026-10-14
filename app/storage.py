"""Couche de stockage : PostgreSQL si POSTGRES_DSN est défini, sinon SQLite (mode local)."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

SCHEMA = (Path(__file__).resolve().parents[1] / "sql" / "init.sql").read_text(encoding="utf-8")

INSERTS = {
    "sensor_aggregates": ("window_start", "tank_id", "metric", "count", "avg", "min", "max"),
    "anomalies": ("event_id", "tank_id", "metric", "value", "z_score", "ts"),
    "rejected_events": ("event_id", "tank_id", "metric", "value", "ts", "reason"),
}


class Storage:
    def __init__(self, sqlite_path: str = "data/iot.db"):
        dsn = os.getenv("POSTGRES_DSN")
        if dsn:
            import psycopg  # import tardif : inutile en mode local
            self.conn = psycopg.connect(dsn, autocommit=True)
            self.ph = "%s"
        else:
            Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(sqlite_path)
            self.ph = "?"
        cur = self.conn.cursor()
        for stmt in filter(str.strip, SCHEMA.split(";")):
            cur.execute(stmt)
        self.conn.commit()

    def insert(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        cols = INSERTS[table]
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join([self.ph] * len(cols))})"
        self.conn.cursor().executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
        self.conn.commit()

    def query(self, sql: str) -> list[tuple]:
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
