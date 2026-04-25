"""Track and enforce concurrency capacity limits per job."""

import sqlite3
from datetime import datetime


def init_capacity(conn: sqlite3.Connection) -> None:
    """Create the capacity_limits and active_runs tables if they don't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS capacity_limits (
            job_name TEXT PRIMARY KEY,
            max_concurrent INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def set_capacity(conn: sqlite3.Connection, job_name: str, max_concurrent: int) -> None:
    """Set the maximum concurrent run limit for a job."""
    if max_concurrent < 1:
        raise ValueError("max_concurrent must be at least 1")
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO capacity_limits (job_name, max_concurrent, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET max_concurrent=excluded.max_concurrent,
            updated_at=excluded.updated_at
        """,
        (job_name.lower(), max_concurrent, now),
    )
    conn.commit()


def get_capacity(conn: sqlite3.Connection, job_name: str) -> int:
    """Return the max concurrent limit for a job, defaulting to 1."""
    row = conn.execute(
        "SELECT max_concurrent FROM capacity_limits WHERE job_name = ?",
        (job_name.lower(),),
    ).fetchone()
    return row[0] if row else 1


def remove_capacity(conn: sqlite3.Connection, job_name: str) -> bool:
    """Remove a capacity limit. Returns True if a row was deleted."""
    cur = conn.execute(
        "DELETE FROM capacity_limits WHERE job_name = ?", (job_name.lower(),)
    )
    conn.commit()
    return cur.rowcount > 0


def list_capacity(conn: sqlite3.Connection) -> list[dict]:
    """Return all configured capacity limits."""
    rows = conn.execute(
        "SELECT job_name, max_concurrent, updated_at FROM capacity_limits ORDER BY job_name"
    ).fetchall()
    return [
        {"job_name": r[0], "max_concurrent": r[1], "updated_at": r[2]} for r in rows
    ]


def count_running(conn: sqlite3.Connection, job_name: str) -> int:
    """Count how many runs of a job are currently in 'running' status."""
    row = conn.execute(
        "SELECT COUNT(*) FROM runs WHERE job_name = ? AND status = 'running'",
        (job_name.lower(),),
    ).fetchone()
    return row[0] if row else 0


def is_at_capacity(conn: sqlite3.Connection, job_name: str) -> bool:
    """Return True if the job has reached its concurrency limit."""
    limit = get_capacity(conn, job_name)
    running = count_running(conn, job_name)
    return running >= limit
