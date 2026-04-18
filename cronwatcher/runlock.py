"""Run lock: prevent overlapping executions of the same cron job."""
import sqlite3
import time
from cronwatcher.storage import get_connection


def init_runlock(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_locks (
            job_name TEXT PRIMARY KEY,
            pid INTEGER NOT NULL,
            locked_at REAL NOT NULL
        )
    """)
    conn.commit()


def acquire_lock(conn: sqlite3.Connection, job_name: str, pid: int) -> bool:
    """Try to acquire a lock. Returns True if acquired, False if already locked."""
    try:
        conn.execute(
            "INSERT INTO run_locks (job_name, pid, locked_at) VALUES (?, ?, ?)",
            (job_name.lower(), pid, time.time()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def release_lock(conn: sqlite3.Connection, job_name: str) -> bool:
    """Release a lock. Returns True if a lock was removed."""
    cur = conn.execute(
        "DELETE FROM run_locks WHERE job_name = ?", (job_name.lower(),)
    )
    conn.commit()
    return cur.rowcount > 0


def get_lock(conn: sqlite3.Connection, job_name: str) -> dict | None:
    """Return lock info or None if not locked."""
    row = conn.execute(
        "SELECT job_name, pid, locked_at FROM run_locks WHERE job_name = ?",
        (job_name.lower(),),
    ).fetchone()
    if row is None:
        return None
    return {"job_name": row[0], "pid": row[1], "locked_at": row[2]}


def clear_stale_locks(conn: sqlite3.Connection, max_age_seconds: float = 3600.0) -> int:
    """Remove locks older than max_age_seconds. Returns count removed."""
    cutoff = time.time() - max_age_seconds
    cur = conn.execute(
        "DELETE FROM run_locks WHERE locked_at < ?", (cutoff,)
    )
    conn.commit()
    return cur.rowcount
