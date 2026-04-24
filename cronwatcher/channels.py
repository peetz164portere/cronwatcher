"""Notification channels — store named delivery targets (slack, email, pagerduty, etc.)."""
import sqlite3
import json
from typing import Optional

VALID_TYPES = {"slack", "email", "pagerduty", "webhook", "teams"}


def init_channels(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT NOT NULL UNIQUE,
            type      TEXT NOT NULL,
            config    TEXT NOT NULL DEFAULT '{}',
            enabled   INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def add_channel(conn: sqlite3.Connection, name: str, type_: str, config: Optional[dict] = None) -> int:
    name = name.lower().strip()
    type_ = type_.lower().strip()
    if type_ not in VALID_TYPES:
        raise ValueError(f"Invalid channel type '{type_}'. Must be one of: {sorted(VALID_TYPES)}")
    cfg = json.dumps(config or {})
    cur = conn.execute(
        "INSERT OR IGNORE INTO channels (name, type, config) VALUES (?, ?, ?)",
        (name, type_, cfg),
    )
    conn.commit()
    return cur.lastrowid


def get_channel(conn: sqlite3.Connection, name: str) -> Optional[dict]:
    name = name.lower().strip()
    row = conn.execute(
        "SELECT id, name, type, config, enabled FROM channels WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "name": row[1], "type": row[2], "config": json.loads(row[3]), "enabled": bool(row[4])}


def remove_channel(conn: sqlite3.Connection, name: str) -> bool:
    name = name.lower().strip()
    cur = conn.execute("DELETE FROM channels WHERE name = ?", (name,))
    conn.commit()
    return cur.rowcount > 0


def set_enabled(conn: sqlite3.Connection, name: str, enabled: bool) -> bool:
    name = name.lower().strip()
    cur = conn.execute("UPDATE channels SET enabled = ? WHERE name = ?", (int(enabled), name))
    conn.commit()
    return cur.rowcount > 0


def list_channels(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT id, name, type, config, enabled FROM channels ORDER BY name"
    ).fetchall()
    return [
        {"id": r[0], "name": r[1], "type": r[2], "config": json.loads(r[3]), "enabled": bool(r[4])}
        for r in rows
    ]
