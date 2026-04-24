"""Per-job cooldown periods: prevent a job from being re-run too soon after completion."""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional


def init_cooldowns(conn: sqlite3.Connection) -> None:
    """Create the cooldowns table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cooldowns (
            job_name TEXT PRIMARY KEY,
            cooldown_seconds INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def set_cooldown(conn: sqlite3.Connection, job_name: str, seconds: int) -> None:
    """Set or update the cooldown period for a job."""
    job_name = job_name.lower()
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO cooldowns (job_name, cooldown_seconds, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET
            cooldown_seconds = excluded.cooldown_seconds,
            updated_at = excluded.updated_at
        """,
        (job_name, seconds, now),
    )
    conn.commit()


def get_cooldown(conn: sqlite3.Connection, job_name: str) -> Optional[int]:
    """Return the cooldown in seconds for a job, or None if not set."""
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT cooldown_seconds FROM cooldowns WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    return row[0] if row else None


def remove_cooldown(conn: sqlite3.Connection, job_name: str) -> bool:
    """Remove the cooldown for a job. Returns True if a row was deleted."""
    job_name = job_name.lower()
    cur = conn.execute("DELETE FROM cooldowns WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def list_cooldowns(conn: sqlite3.Connection) -> list:
    """Return all cooldown entries as a list of dicts."""
    rows = conn.execute(
        "SELECT job_name, cooldown_seconds, updated_at FROM cooldowns ORDER BY job_name"
    ).fetchall()
    return [
        {"job_name": r[0], "cooldown_seconds": r[1], "updated_at": r[2]}
        for r in rows
    ]


def is_in_cooldown(
    conn: sqlite3.Connection, job_name: str, last_finish: Optional[datetime]
) -> bool:
    """Return True if the job is still within its cooldown window."""
    seconds = get_cooldown(conn, job_name)
    if seconds is None or last_finish is None:
        return False
    return datetime.utcnow() < last_finish + timedelta(seconds=seconds)
