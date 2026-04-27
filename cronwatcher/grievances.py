"""Track repeated failure patterns and surface persistent problem jobs."""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def init_grievances(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS grievances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            failure_count INTEGER NOT NULL DEFAULT 0,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            resolved INTEGER NOT NULL DEFAULT 0,
            resolved_at TEXT
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_grievances_job
        ON grievances (job_name)
        WHERE resolved = 0
    """)
    conn.commit()


def record_failure(conn: sqlite3.Connection, job_name: str) -> int:
    job_name = job_name.lower()
    now = datetime.now(timezone.utc).isoformat()
    existing = conn.execute(
        "SELECT id, failure_count FROM grievances WHERE job_name = ? AND resolved = 0",
        (job_name,)
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE grievances SET failure_count = ?, last_seen = ? WHERE id = ?",
            (existing[1] + 1, now, existing[0])
        )
        conn.commit()
        return existing[0]
    cur = conn.execute(
        "INSERT INTO grievances (job_name, failure_count, first_seen, last_seen) VALUES (?, 1, ?, ?)",
        (job_name, now, now)
    )
    conn.commit()
    return cur.lastrowid


def resolve_grievance(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower()
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "UPDATE grievances SET resolved = 1, resolved_at = ? WHERE job_name = ? AND resolved = 0",
        (now, job_name)
    )
    conn.commit()
    return cur.rowcount > 0


def get_grievance(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT id, job_name, failure_count, first_seen, last_seen FROM grievances WHERE job_name = ? AND resolved = 0",
        (job_name,)
    ).fetchone()
    if not row:
        return None
    return {"id": row[0], "job_name": row[1], "failure_count": row[2], "first_seen": row[3], "last_seen": row[4]}


def list_grievances(conn: sqlite3.Connection, include_resolved: bool = False) -> list:
    if include_resolved:
        rows = conn.execute(
            "SELECT id, job_name, failure_count, first_seen, last_seen, resolved, resolved_at FROM grievances ORDER BY failure_count DESC"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, job_name, failure_count, first_seen, last_seen, resolved, resolved_at FROM grievances WHERE resolved = 0 ORDER BY failure_count DESC"
        ).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "failure_count": r[2], "first_seen": r[3], "last_seen": r[4], "resolved": bool(r[5]), "resolved_at": r[6]}
        for r in rows
    ]
