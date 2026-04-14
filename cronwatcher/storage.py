"""SQLite-backed storage for cron job execution history."""

import sqlite3
import os
from datetime import datetime
from typing import Optional

DEFAULT_DB_PATH = os.path.expanduser("~/.cronwatcher/history.db")


def get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Create tables if they don't exist."""
    with get_connection(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                exit_code INTEGER,
                output TEXT,
                status TEXT NOT NULL DEFAULT 'running'
            )
        """)
        conn.commit()


def record_start(job_name: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Insert a new run record and return its ID."""
    started_at = datetime.utcnow().isoformat()
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            "INSERT INTO job_runs (job_name, started_at, status) VALUES (?, ?, 'running')",
            (job_name, started_at),
        )
        conn.commit()
        return cursor.lastrowid


def record_finish(
    run_id: int,
    exit_code: int,
    output: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    """Update an existing run record with finish details."""
    finished_at = datetime.utcnow().isoformat()
    status = "success" if exit_code == 0 else "failure"
    with get_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE job_runs
            SET finished_at = ?, exit_code = ?, output = ?, status = ?
            WHERE id = ?
            """,
            (finished_at, exit_code, output, status, run_id),
        )
        conn.commit()


def fetch_history(job_name: Optional[str] = None, limit: int = 20, db_path: str = DEFAULT_DB_PATH):
    """Return recent job run records, optionally filtered by job name."""
    with get_connection(db_path) as conn:
        if job_name:
            rows = conn.execute(
                "SELECT * FROM job_runs WHERE job_name = ? ORDER BY id DESC LIMIT ?",
                (job_name, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM job_runs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
    return [dict(row) for row in rows]
