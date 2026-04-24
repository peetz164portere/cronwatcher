"""Per-job callback URL registry — store and retrieve HTTP callbacks
triggered on job success, failure, or both."""

import json
import sqlite3
from typing import Optional

VALID_EVENTS = {"success", "failure", "any"}


def init_callbacks(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS callbacks (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT    NOT NULL,
            event    TEXT    NOT NULL CHECK(event IN ('success','failure','any')),
            url      TEXT    NOT NULL,
            headers  TEXT    NOT NULL DEFAULT '{}',
            UNIQUE(job_name, event, url)
        )
        """
    )
    conn.commit()


def add_callback(
    conn: sqlite3.Connection,
    job_name: str,
    event: str,
    url: str,
    headers: Optional[dict] = None,
) -> int:
    event = event.lower()
    if event not in VALID_EVENTS:
        raise ValueError(f"event must be one of {VALID_EVENTS}, got {event!r}")
    job_name = job_name.lower()
    headers_json = json.dumps(headers or {})
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO callbacks (job_name, event, url, headers)
        VALUES (?, ?, ?, ?)
        """,
        (job_name, event, url, headers_json),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def get_callbacks(conn: sqlite3.Connection, job_name: str, event: str) -> list[dict]:
    """Return callbacks matching the given job and event (or 'any')."""
    job_name = job_name.lower()
    event = event.lower()
    rows = conn.execute(
        """
        SELECT id, job_name, event, url, headers
        FROM callbacks
        WHERE job_name = ? AND (event = ? OR event = 'any')
        ORDER BY id
        """,
        (job_name, event),
    ).fetchall()
    return [
        {
            "id": r[0],
            "job_name": r[1],
            "event": r[2],
            "url": r[3],
            "headers": json.loads(r[4]),
        }
        for r in rows
    ]


def remove_callback(conn: sqlite3.Connection, callback_id: int) -> bool:
    cur = conn.execute("DELETE FROM callbacks WHERE id = ?", (callback_id,))
    conn.commit()
    return cur.rowcount > 0


def list_all_callbacks(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, job_name, event, url, headers FROM callbacks ORDER BY job_name, event"
    ).fetchall()
    return [
        {
            "id": r[0],
            "job_name": r[1],
            "event": r[2],
            "url": r[3],
            "headers": json.loads(r[4]),
        }
        for r in rows
    ]
