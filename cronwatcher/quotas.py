"""Job run quotas — cap how many times a job can run within a time window."""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional


def init_quotas(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quotas (
            job_name TEXT PRIMARY KEY,
            max_runs INTEGER NOT NULL,
            window_seconds INTEGER NOT NULL
        )
    """)
    conn.commit()


def set_quota(conn: sqlite3.Connection, job_name: str, max_runs: int, window_seconds: int) -> None:
    job_name = job_name.lower()
    conn.execute("""
        INSERT INTO quotas (job_name, max_runs, window_seconds)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET max_runs=excluded.max_runs, window_seconds=excluded.window_seconds
    """, (job_name, max_runs, window_seconds))
    conn.commit()


def get_quota(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT job_name, max_runs, window_seconds FROM quotas WHERE job_name = ?",
        (job_name,)
    ).fetchone()
    if row is None:
        return None
    return {"job_name": row[0], "max_runs": row[1], "window_seconds": row[2]}


def remove_quota(conn: sqlite3.Connection, job_name: str) -> None:
    conn.execute("DELETE FROM quotas WHERE job_name = ?", (job_name.lower(),))
    conn.commit()


def count_recent_runs(conn: sqlite3.Connection, job_name: str, window_seconds: int) -> int:
    since = (datetime.utcnow() - timedelta(seconds=window_seconds)).isoformat()
    row = conn.execute("""
        SELECT COUNT(*) FROM runs
        WHERE job_name = ? AND started_at >= ?
    """, (job_name.lower(), since)).fetchone()
    return row[0] if row else 0


def is_quota_exceeded(conn: sqlite3.Connection, job_name: str) -> bool:
    quota = get_quota(conn, job_name)
    if quota is None:
        return False
    count = count_recent_runs(conn, job_name, quota["window_seconds"])
    return count >= quota["max_runs"]


def list_quotas(conn: sqlite3.Connection) -> list:
    rows = conn.execute("SELECT job_name, max_runs, window_seconds FROM quotas ORDER BY job_name").fetchall()
    return [{"job_name": r[0], "max_runs": r[1], "window_seconds": r[2]} for r in rows]
