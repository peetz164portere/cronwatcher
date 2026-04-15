"""Alert rate limiting and suppression logic for cronwatcher."""

import time
import sqlite3
from cronwatcher.storage import get_connection


DEFAULT_COOLDOWN_SECONDS = 3600  # 1 hour


def get_last_alert_time(db_path: str, job_name: str) -> float | None:
    """Return the timestamp of the last alert sent for a job, or None."""
    conn = get_connection(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT alerted_at FROM alert_log WHERE job_name = ? ORDER BY alerted_at DESC LIMIT 1",
        (job_name,),
    )
    row = cur.fetchone()
    conn.close()
    return row["alerted_at"] if row else None


def record_alert(db_path: str, job_name: str, run_id: int) -> None:
    """Record that an alert was sent for a job run."""
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO alert_log (job_name, run_id, alerted_at) VALUES (?, ?, ?)",
        (job_name, run_id, time.time()),
    )
    conn.commit()
    conn.close()


def init_alert_log(db_path: str) -> None:
    """Create the alert_log table if it doesn't exist."""
    conn = get_connection(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            run_id INTEGER NOT NULL,
            alerted_at REAL NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def should_suppress_alert(
    db_path: str, job_name: str, cooldown: int = DEFAULT_COOLDOWN_SECONDS
) -> bool:
    """Return True if an alert was already sent within the cooldown window."""
    last = get_last_alert_time(db_path, job_name)
    if last is None:
        return False
    return (time.time() - last) < cooldown
