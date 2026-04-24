"""Pause/resume tracking for cron jobs.

Allows jobs to be paused so that failures during the pause window
are suppressed from alerting.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def init_pauses(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pauses (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL UNIQUE,
            paused_at TEXT NOT NULL,
            reason   TEXT
        )
        """
    )
    conn.commit()


def pause_job(
    conn: sqlite3.Connection, job_name: str, reason: Optional[str] = None
) -> int:
    """Pause a job. Returns the row id. Idempotent — updates reason if already paused."""
    job_name = job_name.lower().strip()
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT INTO pauses (job_name, paused_at, reason)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET paused_at=excluded.paused_at, reason=excluded.reason
        """,
        (job_name, now, reason),
    )
    conn.commit()
    return cur.lastrowid


def resume_job(conn: sqlite3.Connection, job_name: str) -> bool:
    """Resume a job. Returns True if a row was removed, False if it wasn't paused."""
    job_name = job_name.lower().strip()
    cur = conn.execute("DELETE FROM pauses WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def is_paused(conn: sqlite3.Connection, job_name: str) -> bool:
    """Return True if the job is currently paused."""
    job_name = job_name.lower().strip()
    row = conn.execute(
        "SELECT 1 FROM pauses WHERE job_name = ?", (job_name,)
    ).fetchone()
    return row is not None


def get_pause_info(
    conn: sqlite3.Connection, job_name: str
) -> Optional[dict]:
    """Return pause metadata for a job, or None if not paused."""
    job_name = job_name.lower().strip()
    row = conn.execute(
        "SELECT id, job_name, paused_at, reason FROM pauses WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "job_name": row[1], "paused_at": row[2], "reason": row[3]}


def list_paused(conn: sqlite3.Connection) -> list[dict]:
    """Return all currently paused jobs."""
    rows = conn.execute(
        "SELECT id, job_name, paused_at, reason FROM pauses ORDER BY paused_at"
    ).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "paused_at": r[2], "reason": r[3]}
        for r in rows
    ]
