"""Failure threshold tracking — alert when a job fails N times in a row."""

import sqlite3
from typing import Optional


def init_thresholds(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS thresholds (
            job_name TEXT PRIMARY KEY,
            max_failures INTEGER NOT NULL DEFAULT 3
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS failure_streaks (
            job_name TEXT PRIMARY KEY,
            streak INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def set_threshold(conn: sqlite3.Connection, job_name: str, max_failures: int) -> None:
    if max_failures < 1:
        raise ValueError("max_failures must be at least 1")
    conn.execute("""
        INSERT INTO thresholds (job_name, max_failures)
        VALUES (?, ?)
        ON CONFLICT(job_name) DO UPDATE SET max_failures = excluded.max_failures
    """, (job_name.lower(), max_failures))
    conn.commit()


def get_threshold(conn: sqlite3.Connection, job_name: str) -> Optional[int]:
    row = conn.execute(
        "SELECT max_failures FROM thresholds WHERE job_name = ?",
        (job_name.lower(),)
    ).fetchone()
    return row[0] if row else None


def remove_threshold(conn: sqlite3.Connection, job_name: str) -> None:
    conn.execute("DELETE FROM thresholds WHERE job_name = ?", (job_name.lower(),))
    conn.commit()


def record_streak(conn: sqlite3.Connection, job_name: str, failed: bool) -> int:
    """Update the consecutive failure streak. Returns current streak count."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    job = job_name.lower()
    if not failed:
        conn.execute("""
            INSERT INTO failure_streaks (job_name, streak, updated_at)
            VALUES (?, 0, ?)
            ON CONFLICT(job_name) DO UPDATE SET streak = 0, updated_at = excluded.updated_at
        """, (job, now))
        conn.commit()
        return 0
    row = conn.execute(
        "SELECT streak FROM failure_streaks WHERE job_name = ?", (job,)
    ).fetchone()
    new_streak = (row[0] if row else 0) + 1
    conn.execute("""
        INSERT INTO failure_streaks (job_name, streak, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET streak = excluded.streak, updated_at = excluded.updated_at
    """, (job, new_streak, now))
    conn.commit()
    return new_streak


def get_streak(conn: sqlite3.Connection, job_name: str) -> int:
    row = conn.execute(
        "SELECT streak FROM failure_streaks WHERE job_name = ?",
        (job_name.lower(),)
    ).fetchone()
    return row[0] if row else 0


def is_threshold_exceeded(conn: sqlite3.Connection, job_name: str) -> bool:
    threshold = get_threshold(conn, job_name)
    if threshold is None:
        return False
    return get_streak(conn, job_name) >= threshold
