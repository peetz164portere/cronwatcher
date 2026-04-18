"""Job dependency tracking — define run order constraints between jobs."""
import sqlite3
from typing import Optional


def init_dependencies(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_dependencies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            depends_on TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(job_name, depends_on)
        )
    """)
    conn.commit()


def add_dependency(conn: sqlite3.Connection, job_name: str, depends_on: str) -> None:
    """Record that job_name must run after depends_on."""
    if job_name == depends_on:
        raise ValueError("A job cannot depend on itself.")
    conn.execute(
        "INSERT OR IGNORE INTO job_dependencies (job_name, depends_on) VALUES (?, ?)",
        (job_name.lower(), depends_on.lower()),
    )
    conn.commit()


def remove_dependency(conn: sqlite3.Connection, job_name: str, depends_on: str) -> bool:
    cur = conn.execute(
        "DELETE FROM job_dependencies WHERE job_name=? AND depends_on=?",
        (job_name.lower(), depends_on.lower()),
    )
    conn.commit()
    return cur.rowcount > 0


def get_dependencies(conn: sqlite3.Connection, job_name: str) -> list[str]:
    """Return list of jobs that job_name depends on."""
    cur = conn.execute(
        "SELECT depends_on FROM job_dependencies WHERE job_name=? ORDER BY depends_on",
        (job_name.lower(),),
    )
    return [row[0] for row in cur.fetchall()]


def get_dependents(conn: sqlite3.Connection, job_name: str) -> list[str]:
    """Return list of jobs that depend on job_name."""
    cur = conn.execute(
        "SELECT job_name FROM job_dependencies WHERE depends_on=? ORDER BY job_name",
        (job_name.lower(),),
    )
    return [row[0] for row in cur.fetchall()]


def check_ready(conn: sqlite3.Connection, job_name: str) -> dict:
    """Check if all dependencies had a recent successful run."""
    deps = get_dependencies(conn, job_name)
    if not deps:
        return {"ready": True, "blocking": []}

    blocking = []
    for dep in deps:
        cur = conn.execute(
            "SELECT id FROM runs WHERE job_name=? AND status='success' ORDER BY finished_at DESC LIMIT 1",
            (dep,),
        )
        if cur.fetchone() is None:
            blocking.append(dep)

    return {"ready": len(blocking) == 0, "blocking": blocking}
