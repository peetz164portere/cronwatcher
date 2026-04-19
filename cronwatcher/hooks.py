"""Pre/post run hooks: register shell commands to fire before or after a job."""

import sqlite3
from typing import Optional


def init_hooks(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            hook_type TEXT NOT NULL CHECK(hook_type IN ('pre', 'post')),
            command TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hooks_job ON hooks(job_name, hook_type)")
    conn.commit()


def add_hook(conn: sqlite3.Connection, job_name: str, hook_type: str, command: str) -> int:
    job_name = job_name.lower().strip()
    if hook_type not in ("pre", "post"):
        raise ValueError(f"hook_type must be 'pre' or 'post', got {hook_type!r}")
    cur = conn.execute(
        "INSERT INTO hooks (job_name, hook_type, command) VALUES (?, ?, ?)",
        (job_name, hook_type, command),
    )
    conn.commit()
    return cur.lastrowid


def get_hooks(conn: sqlite3.Connection, job_name: str, hook_type: str) -> list:
    job_name = job_name.lower().strip()
    cur = conn.execute(
        "SELECT id, job_name, hook_type, command, created_at FROM hooks "
        "WHERE job_name = ? AND hook_type = ? ORDER BY id",
        (job_name, hook_type),
    )
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def remove_hook(conn: sqlite3.Connection, hook_id: int) -> bool:
    cur = conn.execute("DELETE FROM hooks WHERE id = ?", (hook_id,))
    conn.commit()
    return cur.rowcount > 0


def list_all_hooks(conn: sqlite3.Connection, job_name: Optional[str] = None) -> list:
    job_name_filter = job_name.lower().strip() if job_name else None
    if job_name_filter:
        cur = conn.execute(
            "SELECT id, job_name, hook_type, command, created_at FROM hooks "
            "WHERE job_name = ? ORDER BY job_name, hook_type, id",
            (job_name_filter,),
        )
    else:
        cur = conn.execute(
            "SELECT id, job_name, hook_type, command, created_at FROM hooks "
            "ORDER BY job_name, hook_type, id"
        )
    return [dict(r) for r in cur.fetchall()]
