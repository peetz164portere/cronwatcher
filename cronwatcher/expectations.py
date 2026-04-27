"""Expected duration/frequency expectations for cron jobs."""
import sqlite3
from typing import Optional


def init_expectations(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS expectations (
            job_name TEXT PRIMARY KEY,
            min_duration REAL,
            max_duration REAL,
            max_interval_seconds INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def set_expectation(
    conn: sqlite3.Connection,
    job_name: str,
    min_duration: Optional[float] = None,
    max_duration: Optional[float] = None,
    max_interval_seconds: Optional[int] = None,
) -> None:
    job_name = job_name.lower().strip()
    conn.execute("""
        INSERT INTO expectations (job_name, min_duration, max_duration, max_interval_seconds, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            min_duration = excluded.min_duration,
            max_duration = excluded.max_duration,
            max_interval_seconds = excluded.max_interval_seconds,
            updated_at = excluded.updated_at
    """, (job_name, min_duration, max_duration, max_interval_seconds))
    conn.commit()


def get_expectation(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower().strip()
    row = conn.execute(
        "SELECT job_name, min_duration, max_duration, max_interval_seconds, updated_at "
        "FROM expectations WHERE job_name = ?",
        (job_name,)
    ).fetchone()
    if row is None:
        return None
    return dict(zip(["job_name", "min_duration", "max_duration", "max_interval_seconds", "updated_at"], row))


def remove_expectation(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower().strip()
    cur = conn.execute("DELETE FROM expectations WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def list_expectations(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, min_duration, max_duration, max_interval_seconds, updated_at "
        "FROM expectations ORDER BY job_name"
    ).fetchall()
    keys = ["job_name", "min_duration", "max_duration", "max_interval_seconds", "updated_at"]
    return [dict(zip(keys, row)) for row in rows]


def check_expectation(conn: sqlite3.Connection, job_name: str, duration: float) -> list:
    """Return list of violation strings for a completed run."""
    exp = get_expectation(conn, job_name)
    if exp is None:
        return []
    violations = []
    if exp["min_duration"] is not None and duration < exp["min_duration"]:
        violations.append(f"duration {duration:.1f}s below minimum {exp['min_duration']:.1f}s")
    if exp["max_duration"] is not None and duration > exp["max_duration"]:
        violations.append(f"duration {duration:.1f}s exceeds maximum {exp['max_duration']:.1f}s")
    return violations
