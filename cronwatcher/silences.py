"""Silence/mute alerts for a job during a time window."""

import sqlite3
from datetime import datetime
from typing import Optional


def init_silences(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS silences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            reason TEXT,
            starts_at TEXT NOT NULL,
            ends_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def add_silence(
    conn: sqlite3.Connection,
    job_name: str,
    starts_at: datetime,
    ends_at: datetime,
    reason: Optional[str] = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO silences (job_name, reason, starts_at, ends_at) VALUES (?, ?, ?, ?)",
        (job_name.lower(), reason, starts_at.isoformat(), ends_at.isoformat()),
    )
    conn.commit()
    return cur.lastrowid


def is_silenced(conn: sqlite3.Connection, job_name: str, at: Optional[datetime] = None) -> bool:
    if at is None:
        at = datetime.utcnow()
    row = conn.execute(
        """
        SELECT 1 FROM silences
        WHERE job_name = ? AND starts_at <= ? AND ends_at >= ?
        LIMIT 1
        """,
        (job_name.lower(), at.isoformat(), at.isoformat()),
    ).fetchone()
    return row is not None


def list_silences(conn: sqlite3.Connection, job_name: Optional[str] = None) -> list:
    if job_name:
        rows = conn.execute(
            "SELECT id, job_name, reason, starts_at, ends_at FROM silences WHERE job_name = ? ORDER BY starts_at",
            (job_name.lower(),),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, job_name, reason, starts_at, ends_at FROM silences ORDER BY starts_at"
        ).fetchall()
    return [dict(r) for r in rows]


def remove_silence(conn: sqlite3.Connection, silence_id: int) -> bool:
    cur = conn.execute("DELETE FROM silences WHERE id = ?", (silence_id,))
    conn.commit()
    return cur.rowcount > 0
