"""Prune old run history from the database."""

from datetime import datetime, timedelta
from cronwatcher.storage import get_connection


def prune_history(db_path: str, older_than_days: int, job_name: str = None) -> int:
    """
    Delete run records older than `older_than_days` days.
    Optionally filter by job_name.
    Returns the number of deleted rows.
    """
    cutoff = datetime.utcnow() - timedelta(days=older_than_days)
    cutoff_str = cutoff.isoformat()

    conn = get_connection(db_path)
    try:
        if job_name:
            cur = conn.execute(
                "DELETE FROM runs WHERE started_at < ? AND job_name = ?",
                (cutoff_str, job_name),
            )
        else:
            cur = conn.execute(
                "DELETE FROM runs WHERE started_at < ?",
                (cutoff_str,),
            )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def prune_alert_log(db_path: str, older_than_days: int) -> int:
    """
    Delete alert log entries older than `older_than_days` days.
    Returns the number of deleted rows.
    """
    cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).isoformat()
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            "DELETE FROM alert_log WHERE alerted_at < ?",
            (cutoff,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()
