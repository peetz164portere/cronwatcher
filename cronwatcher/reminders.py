"""Reminders: notify when a job hasn't run in a while."""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict


def init_reminders(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL UNIQUE,
            interval_hours REAL NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()


def set_reminder(conn: sqlite3.Connection, job_name: str, interval_hours: float) -> int:
    job_name = job_name.lower()
    now = datetime.utcnow().isoformat()
    cur = conn.execute("""
        INSERT INTO reminders (job_name, interval_hours, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET interval_hours=excluded.interval_hours
    """, (job_name, interval_hours, now))
    conn.commit()
    return cur.lastrowid


def get_reminder(conn: sqlite3.Connection, job_name: str) -> Optional[Dict]:
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT job_name, interval_hours, created_at FROM reminders WHERE job_name=?",
        (job_name,)
    ).fetchone()
    if row is None:
        return None
    return {"job_name": row[0], "interval_hours": row[1], "created_at": row[2]}


def list_reminders(conn: sqlite3.Connection) -> List[Dict]:
    rows = conn.execute(
        "SELECT job_name, interval_hours, created_at FROM reminders ORDER BY job_name"
    ).fetchall()
    return [{"job_name": r[0], "interval_hours": r[1], "created_at": r[2]} for r in rows]


def remove_reminder(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower()
    cur = conn.execute("DELETE FROM reminders WHERE job_name=?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def check_reminders(conn: sqlite3.Connection) -> List[Dict]:
    """Return reminders where the job hasn't had a successful run within the interval."""
    reminders = list_reminders(conn)
    overdue = []
    for r in reminders:
        cutoff = (datetime.utcnow() - timedelta(hours=r["interval_hours"])).isoformat()
        row = conn.execute("""
            SELECT MAX(started_at) FROM runs
            WHERE job_name=? AND status='success' AND started_at >= ?
        """, (r["job_name"], cutoff)).fetchone()
        last_ok = row[0] if row else None
        if last_ok is None:
            overdue.append({**r, "last_success": None})
    return overdue
