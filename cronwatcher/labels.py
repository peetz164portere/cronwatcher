"""Key-value label support for cron jobs."""
import sqlite3
from typing import Optional


def init_labels(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS labels (
            job_name TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (job_name, key)
        )
    """)
    conn.commit()


def set_label(conn: sqlite3.Connection, job_name: str, key: str, value: str) -> None:
    key = key.lower().strip()
    conn.execute("""
        INSERT INTO labels (job_name, key, value)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name, key) DO UPDATE SET value=excluded.value
    """, (job_name, key, value))
    conn.commit()


def get_label(conn: sqlite3.Connection, job_name: str, key: str) -> Optional[str]:
    key = key.lower().strip()
    row = conn.execute(
        "SELECT value FROM labels WHERE job_name=? AND key=?",
        (job_name, key)
    ).fetchone()
    return row[0] if row else None


def get_labels(conn: sqlite3.Connection, job_name: str) -> dict:
    rows = conn.execute(
        "SELECT key, value FROM labels WHERE job_name=? ORDER BY key",
        (job_name,)
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def remove_label(conn: sqlite3.Connection, job_name: str, key: str) -> bool:
    key = key.lower().strip()
    cur = conn.execute(
        "DELETE FROM labels WHERE job_name=? AND key=?",
        (job_name, key)
    )
    conn.commit()
    return cur.rowcount > 0


def get_jobs_by_label(conn: sqlite3.Connection, key: str, value: str) -> list:
    key = key.lower().strip()
    rows = conn.execute(
        "SELECT DISTINCT job_name FROM labels WHERE key=? AND value=? ORDER BY job_name",
        (key, value)
    ).fetchall()
    return [r[0] for r in rows]
