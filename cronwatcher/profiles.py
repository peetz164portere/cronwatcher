"""Job profiles: named sets of config defaults for a job."""
import sqlite3
import json
from typing import Optional


def init_profiles(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_profiles (
            job_name TEXT PRIMARY KEY,
            profile   TEXT NOT NULL,
            options   TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def set_profile(conn: sqlite3.Connection, job_name: str, profile: str, options: Optional[dict] = None) -> None:
    job_name = job_name.lower().strip()
    profile = profile.lower().strip()
    opts = json.dumps(options or {})
    conn.execute("""
        INSERT INTO job_profiles (job_name, profile, options, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            profile=excluded.profile,
            options=excluded.options,
            updated_at=excluded.updated_at
    """, (job_name, profile, opts))
    conn.commit()


def get_profile(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower().strip()
    row = conn.execute(
        "SELECT job_name, profile, options, updated_at FROM job_profiles WHERE job_name = ?",
        (job_name,)
    ).fetchone()
    if row is None:
        return None
    return {
        "job_name": row[0],
        "profile": row[1],
        "options": json.loads(row[2]),
        "updated_at": row[3],
    }


def remove_profile(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower().strip()
    cur = conn.execute("DELETE FROM job_profiles WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def list_profiles(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, profile, options, updated_at FROM job_profiles ORDER BY job_name"
    ).fetchall()
    return [
        {"job_name": r[0], "profile": r[1], "options": json.loads(r[2]), "updated_at": r[3]}
        for r in rows
    ]
