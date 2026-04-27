"""Curfews: define time windows during which a job must NOT run.

If a job runs inside a curfew window it is flagged as a violation.
"""

import sqlite3
from datetime import datetime, time
from typing import Optional

VALID_DAYS = {"mon", "tue", "wed", "thu", "fri", "sat", "sun", "all"}


def init_curfews(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS curfews (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT    NOT NULL,
            day      TEXT    NOT NULL,
            start    TEXT    NOT NULL,
            end      TEXT    NOT NULL,
            reason   TEXT,
            created_at TEXT  NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def add_curfew(
    conn: sqlite3.Connection,
    job_name: str,
    day: str,
    start: str,
    end: str,
    reason: Optional[str] = None,
) -> int:
    """Add a curfew window for *job_name*.  Returns the new row id."""
    day = day.lower().strip()
    if day not in VALID_DAYS:
        raise ValueError(f"Invalid day '{day}'. Must be one of {sorted(VALID_DAYS)}.")
    # Validate HH:MM format
    for t in (start, end):
        datetime.strptime(t, "%H:%M")
    cur = conn.execute(
        "INSERT INTO curfews (job_name, day, start, end, reason) VALUES (?, ?, ?, ?, ?)",
        (job_name.lower().strip(), day, start, end, reason),
    )
    conn.commit()
    return cur.lastrowid


def get_curfews(conn: sqlite3.Connection, job_name: str) -> list:
    """Return all curfew rows for *job_name*."""
    rows = conn.execute(
        "SELECT id, job_name, day, start, end, reason FROM curfews WHERE job_name = ?",
        (job_name.lower().strip(),),
    ).fetchall()
    return [dict(r) for r in rows]


def remove_curfew(conn: sqlite3.Connection, curfew_id: int) -> bool:
    """Delete a curfew by id.  Returns True if a row was deleted."""
    cur = conn.execute("DELETE FROM curfews WHERE id = ?", (curfew_id,))
    conn.commit()
    return cur.rowcount > 0


def is_in_curfew(
    conn: sqlite3.Connection,
    job_name: str,
    dt: Optional[datetime] = None,
) -> bool:
    """Return True if *dt* (default: now) falls inside any curfew for *job_name*."""
    dt = dt or datetime.now()
    day_abbr = dt.strftime("%a").lower()  # mon, tue, …
    current = dt.time().replace(second=0, microsecond=0)

    curfews = get_curfews(conn, job_name)
    for c in curfews:
        if c["day"] not in (day_abbr, "all"):
            continue
        window_start = time(*map(int, c["start"].split(":")))
        window_end = time(*map(int, c["end"].split(":")))
        if window_start <= current <= window_end:
            return True
    return False
