"""Job blacklist — prevent specific jobs from being recorded or alerted on."""

import sqlite3
from typing import List, Optional


def init_blacklist(conn: sqlite3.Connection) -> None:
    """Create the blacklist table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT    NOT NULL UNIQUE,
            reason   TEXT,
            added_at TEXT    NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def add_to_blacklist(
    conn: sqlite3.Connection, job_name: str, reason: Optional[str] = None
) -> int:
    """Add a job to the blacklist. Returns the row id."""
    job_name = job_name.lower().strip()
    cur = conn.execute(
        "INSERT OR IGNORE INTO blacklist (job_name, reason) VALUES (?, ?)",
        (job_name, reason),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def remove_from_blacklist(conn: sqlite3.Connection, job_name: str) -> bool:
    """Remove a job from the blacklist. Returns True if a row was deleted."""
    job_name = job_name.lower().strip()
    cur = conn.execute(
        "DELETE FROM blacklist WHERE job_name = ?", (job_name,)
    )
    conn.commit()
    return cur.rowcount > 0


def is_blacklisted(conn: sqlite3.Connection, job_name: str) -> bool:
    """Return True if the job is currently blacklisted."""
    job_name = job_name.lower().strip()
    row = conn.execute(
        "SELECT 1 FROM blacklist WHERE job_name = ?", (job_name,)
    ).fetchone()
    return row is not None


def list_blacklist(conn: sqlite3.Connection) -> List[dict]:
    """Return all blacklisted jobs as a list of dicts."""
    rows = conn.execute(
        "SELECT id, job_name, reason, added_at FROM blacklist ORDER BY job_name"
    ).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "reason": r[2], "added_at": r[3]}
        for r in rows
    ]


def clear_blacklist(conn: sqlite3.Connection) -> int:
    """Remove all entries from the blacklist. Returns count removed."""
    cur = conn.execute("DELETE FROM blacklist")
    conn.commit()
    return cur.rowcount
