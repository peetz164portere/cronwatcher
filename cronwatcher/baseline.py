"""Baseline: capture and compare expected job durations."""

import sqlite3
from typing import Optional
from cronwatcher.storage import get_connection


def init_baseline(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS baselines (
            job_name TEXT PRIMARY KEY,
            avg_duration REAL,
            sample_count INTEGER,
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def update_baseline(conn: sqlite3.Connection, job_name: str) -> Optional[float]:
    """Recalculate avg duration from successful runs and store it."""
    row = conn.execute(
        """
        SELECT AVG(duration), COUNT(*)
        FROM runs
        WHERE job_name = ? AND status = 'success' AND duration IS NOT NULL
        """,
        (job_name,),
    ).fetchone()
    if not row or row[1] == 0:
        return None
    avg, count = row
    conn.execute(
        """
        INSERT INTO baselines (job_name, avg_duration, sample_count, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            avg_duration = excluded.avg_duration,
            sample_count = excluded.sample_count,
            updated_at = excluded.updated_at
        """,
        (job_name, avg, count),
    )
    conn.commit()
    return avg


def get_baseline(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT job_name, avg_duration, sample_count, updated_at FROM baselines WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if not row:
        return None
    return {"job_name": row[0], "avg_duration": row[1], "sample_count": row[2], "updated_at": row[3]}


def is_slow(conn: sqlite3.Connection, job_name: str, duration: float, threshold: float = 2.0) -> bool:
    """Return True if duration exceeds threshold * avg_duration."""
    baseline = get_baseline(conn, job_name)
    if not baseline or not baseline["avg_duration"]:
        return False
    return duration > baseline["avg_duration"] * threshold
