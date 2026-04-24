"""Saved search filters — store, load, and apply named query presets."""

import json
import sqlite3
from typing import Optional


def init_filters(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_filters (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL UNIQUE,
            params   TEXT NOT NULL,
            created  TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def save_filter(conn: sqlite3.Connection, name: str, params: dict) -> int:
    """Save or replace a named filter."""
    name = name.lower().strip()
    payload = json.dumps(params, sort_keys=True)
    cur = conn.execute(
        """
        INSERT INTO saved_filters (name, params)
        VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET params = excluded.params
        """,
        (name, payload),
    )
    conn.commit()
    return cur.lastrowid


def get_filter(conn: sqlite3.Connection, name: str) -> Optional[dict]:
    """Return the params dict for a named filter, or None."""
    name = name.lower().strip()
    row = conn.execute(
        "SELECT params FROM saved_filters WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        return None
    return json.loads(row[0])


def remove_filter(conn: sqlite3.Connection, name: str) -> bool:
    """Delete a named filter. Returns True if it existed."""
    name = name.lower().strip()
    cur = conn.execute("DELETE FROM saved_filters WHERE name = ?", (name,))
    conn.commit()
    return cur.rowcount > 0


def list_filters(conn: sqlite3.Connection) -> list[dict]:
    """Return all saved filters as a list of dicts."""
    rows = conn.execute(
        "SELECT name, params, created FROM saved_filters ORDER BY name"
    ).fetchall()
    return [
        {"name": r[0], "params": json.loads(r[1]), "created": r[2]}
        for r in rows
    ]
