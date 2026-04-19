"""Pin important runs for quick reference."""
import sqlite3
from typing import Optional


def init_pinned(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pinned_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL UNIQUE,
            job_name TEXT NOT NULL,
            note TEXT,
            pinned_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def pin_run(conn: sqlite3.Connection, run_id: int, job_name: str, note: Optional[str] = None) -> int:
    cur = conn.execute(
        "INSERT OR IGNORE INTO pinned_runs (run_id, job_name, note) VALUES (?, ?, ?)",
        (run_id, job_name.lower(), note),
    )
    conn.commit()
    return cur.lastrowid


def unpin_run(conn: sqlite3.Connection, run_id: int) -> bool:
    cur = conn.execute("DELETE FROM pinned_runs WHERE run_id = ?", (run_id,))
    conn.commit()
    return cur.rowcount > 0


def is_pinned(conn: sqlite3.Connection, run_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM pinned_runs WHERE run_id = ?", (run_id,)).fetchone()
    return row is not None


def list_pinned(conn: sqlite3.Connection, job_name: Optional[str] = None):
    if job_name:
        rows = conn.execute(
            "SELECT run_id, job_name, note, pinned_at FROM pinned_runs WHERE job_name = ? ORDER BY pinned_at DESC",
            (job_name.lower(),),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT run_id, job_name, note, pinned_at FROM pinned_runs ORDER BY pinned_at DESC"
        ).fetchall()
    return [
        {"run_id": r[0], "job_name": r[1], "note": r[2], "pinned_at": r[3]}
        for r in rows
    ]


def clear_pinned(conn: sqlite3.Connection, job_name: Optional[str] = None) -> int:
    if job_name:
        cur = conn.execute("DELETE FROM pinned_runs WHERE job_name = ?", (job_name.lower(),))
    else:
        cur = conn.execute("DELETE FROM pinned_runs")
    conn.commit()
    return cur.rowcount
