import sqlite3
from typing import Optional


def init_bookmarks(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            run_id INTEGER NOT NULL,
            label TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(job_name, run_id)
        )
    """)
    conn.commit()


def add_bookmark(conn: sqlite3.Connection, job_name: str, run_id: int, label: Optional[str] = None) -> int:
    job_name = job_name.lower()
    cur = conn.execute(
        "INSERT OR IGNORE INTO bookmarks (job_name, run_id, label) VALUES (?, ?, ?)",
        (job_name, run_id, label),
    )
    conn.commit()
    return cur.lastrowid


def remove_bookmark(conn: sqlite3.Connection, job_name: str, run_id: int) -> bool:
    job_name = job_name.lower()
    cur = conn.execute(
        "DELETE FROM bookmarks WHERE job_name = ? AND run_id = ?",
        (job_name, run_id),
    )
    conn.commit()
    return cur.rowcount > 0


def get_bookmarks(conn: sqlite3.Connection, job_name: str) -> list:
    job_name = job_name.lower()
    cur = conn.execute(
        "SELECT id, job_name, run_id, label, created_at FROM bookmarks WHERE job_name = ? ORDER BY created_at DESC",
        (job_name,),
    )
    rows = cur.fetchall()
    return [{"id": r[0], "job_name": r[1], "run_id": r[2], "label": r[3], "created_at": r[4]} for r in rows]


def list_all_bookmarks(conn: sqlite3.Connection) -> list:
    cur = conn.execute(
        "SELECT id, job_name, run_id, label, created_at FROM bookmarks ORDER BY created_at DESC"
    )
    rows = cur.fetchall()
    return [{"id": r[0], "job_name": r[1], "run_id": r[2], "label": r[3], "created_at": r[4]} for r in rows]


def is_bookmarked(conn: sqlite3.Connection, job_name: str, run_id: int) -> bool:
    job_name = job_name.lower()
    cur = conn.execute(
        "SELECT 1 FROM bookmarks WHERE job_name = ? AND run_id = ?",
        (job_name, run_id),
    )
    return cur.fetchone() is not None
