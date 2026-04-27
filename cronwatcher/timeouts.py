import sqlite3
from datetime import datetime
from typing import Optional

DEFAULT_TIMEOUT = 3600  # seconds


def init_timeouts(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS timeouts (
            job_name TEXT PRIMARY KEY,
            timeout_seconds INTEGER NOT NULL,
            action TEXT NOT NULL DEFAULT 'alert',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def set_timeout(
    conn: sqlite3.Connection,
    job_name: str,
    timeout_seconds: int,
    action: str = "alert",
) -> None:
    valid_actions = {"alert", "kill", "ignore"}
    if action not in valid_actions:
        raise ValueError(f"action must be one of {valid_actions}")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    job_name = job_name.lower()
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO timeouts (job_name, timeout_seconds, action, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET
            timeout_seconds = excluded.timeout_seconds,
            action = excluded.action,
            updated_at = excluded.updated_at
        """,
        (job_name, timeout_seconds, action, now),
    )
    conn.commit()


def get_timeout(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT job_name, timeout_seconds, action, updated_at FROM timeouts WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if row is None:
        return None
    return {
        "job_name": row[0],
        "timeout_seconds": row[1],
        "action": row[2],
        "updated_at": row[3],
    }


def remove_timeout(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower()
    cur = conn.execute("DELETE FROM timeouts WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def list_timeouts(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, timeout_seconds, action, updated_at FROM timeouts ORDER BY job_name"
    ).fetchall()
    return [
        {"job_name": r[0], "timeout_seconds": r[1], "action": r[2], "updated_at": r[3]}
        for r in rows
    ]


def is_timed_out(conn: sqlite3.Connection, job_name: str, elapsed_seconds: float) -> bool:
    record = get_timeout(conn, job_name)
    if record is None:
        return elapsed_seconds > DEFAULT_TIMEOUT
    return elapsed_seconds > record["timeout_seconds"]
