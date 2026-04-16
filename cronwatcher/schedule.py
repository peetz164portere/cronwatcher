"""Check if a cron job is overdue based on its expected interval."""

from datetime import datetime, timezone
from typing import Optional
import sqlite3


def get_last_success(conn: sqlite3.Connection, job_name: str) -> Optional[datetime]:
    """Return the datetime of the last successful run for a job."""
    row = conn.execute(
        """
        SELECT finished_at FROM runs
        WHERE job_name = ? AND status = 'success'
        ORDER BY finished_at DESC LIMIT 1
        """,
        (job_name,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    dt = datetime.fromisoformat(row[0])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_overdue(last_success: Optional[datetime], interval_seconds: int) -> bool:
    """Return True if the job hasn't succeeded within the expected interval."""
    if last_success is None:
        return True
    now = datetime.now(timezone.utc)
    elapsed = (now - last_success).total_seconds()
    return elapsed > interval_seconds


def check_schedule(
    conn: sqlite3.Connection, job_name: str, interval_seconds: int
) -> dict:
    """Return a dict describing whether the job is overdue."""
    last_success = get_last_success(conn, job_name)
    overdue = is_overdue(last_success, interval_seconds)
    return {
        "job_name": job_name,
        "last_success": last_success.isoformat() if last_success else None,
        "interval_seconds": interval_seconds,
        "overdue": overdue,
    }
