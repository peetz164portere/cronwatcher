"""Maintenance window management for cronwatcher.

Allows defining time windows during which alerts are suppressed
for specific jobs (e.g. during deployments or scheduled downtime).
"""

import sqlite3
from datetime import datetime, time
from typing import Optional


DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def init_windows(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_windows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            day_of_week TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def add_window(
    conn: sqlite3.Connection,
    job_name: str,
    day_of_week: str,
    start_time: str,
    end_time: str,
    note: Optional[str] = None,
) -> int:
    day = day_of_week.lower()
    if day not in DAYS:
        raise ValueError(f"Invalid day '{day}'. Must be one of: {', '.join(DAYS)}")
    # validate time format
    datetime.strptime(start_time, "%H:%M")
    datetime.strptime(end_time, "%H:%M")
    cur = conn.execute(
        """
        INSERT INTO maintenance_windows (job_name, day_of_week, start_time, end_time, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (job_name.lower(), day, start_time, end_time, note),
    )
    conn.commit()
    return cur.lastrowid


def remove_window(conn: sqlite3.Connection, window_id: int) -> bool:
    cur = conn.execute(
        "DELETE FROM maintenance_windows WHERE id = ?", (window_id,)
    )
    conn.commit()
    return cur.rowcount > 0


def list_windows(conn: sqlite3.Connection, job_name: Optional[str] = None) -> list:
    if job_name:
        cur = conn.execute(
            "SELECT * FROM maintenance_windows WHERE job_name = ? ORDER BY day_of_week, start_time",
            (job_name.lower(),),
        )
    else:
        cur = conn.execute(
            "SELECT * FROM maintenance_windows ORDER BY job_name, day_of_week, start_time"
        )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def is_in_maintenance(conn: sqlite3.Connection, job_name: str, dt: Optional[datetime] = None) -> bool:
    if dt is None:
        dt = datetime.now()
    day = DAYS[dt.weekday()]
    current = dt.strftime("%H:%M")
    cur = conn.execute(
        """
        SELECT 1 FROM maintenance_windows
        WHERE job_name = ? AND day_of_week = ?
          AND start_time <= ? AND end_time > ?
        LIMIT 1
        """,
        (job_name.lower(), day, current, current),
    )
    return cur.fetchone() is not None
