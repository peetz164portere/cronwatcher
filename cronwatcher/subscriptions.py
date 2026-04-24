"""Manage per-job webhook subscriptions for event-based notifications."""

import json
import sqlite3
from typing import Optional

VALID_EVENTS = {"success", "failure", "start", "overdue", "hung"}


def init_subscriptions(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            event TEXT NOT NULL,
            url TEXT NOT NULL,
            headers TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(job_name, event, url)
        )
        """
    )
    conn.commit()


def add_subscription(
    conn: sqlite3.Connection,
    job_name: str,
    event: str,
    url: str,
    headers: Optional[dict] = None,
) -> int:
    event = event.lower()
    if event not in VALID_EVENTS:
        raise ValueError(f"Invalid event '{event}'. Must be one of: {sorted(VALID_EVENTS)}")
    job_name = job_name.lower()
    headers_json = json.dumps(headers or {})
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO subscriptions (job_name, event, url, headers)
        VALUES (?, ?, ?, ?)
        """,
        (job_name, event, url, headers_json),
    )
    conn.commit()
    return cur.lastrowid


def get_subscriptions(
    conn: sqlite3.Connection, job_name: str, event: str
) -> list[dict]:
    job_name = job_name.lower()
    event = event.lower()
    rows = conn.execute(
        "SELECT id, job_name, event, url, headers FROM subscriptions WHERE job_name = ? AND event = ?",
        (job_name, event),
    ).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "event": r[2], "url": r[3], "headers": json.loads(r[4])}
        for r in rows
    ]


def remove_subscription(conn: sqlite3.Connection, subscription_id: int) -> bool:
    cur = conn.execute("DELETE FROM subscriptions WHERE id = ?", (subscription_id,))
    conn.commit()
    return cur.rowcount > 0


def list_all_subscriptions(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, job_name, event, url, headers FROM subscriptions ORDER BY job_name, event"
    ).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "event": r[2], "url": r[3], "headers": json.loads(r[4])}
        for r in rows
    ]
