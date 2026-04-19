"""Per-job notes/documentation storage."""
import sqlite3
from datetime import datetime
from typing import Optional


def init_notes(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL UNIQUE,
            note TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def set_note(conn: sqlite3.Connection, job_name: str, note: str) -> int:
    job_name = job_name.lower()
    now = datetime.utcnow().isoformat()
    cur = conn.execute("""
        INSERT INTO job_notes (job_name, note, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET note=excluded.note, updated_at=excluded.updated_at
    """, (job_name, note, now))
    conn.commit()
    return cur.lastrowid


def get_note(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT job_name, note, updated_at FROM job_notes WHERE job_name = ?",
        (job_name,)
    ).fetchone()
    if row is None:
        return None
    return {"job_name": row[0], "note": row[1], "updated_at": row[2]}


def remove_note(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower()
    cur = conn.execute("DELETE FROM job_notes WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def list_notes(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, note, updated_at FROM job_notes ORDER BY job_name"
    ).fetchall()
    return [{"job_name": r[0], "note": r[1], "updated_at": r[2]} for r in rows]
