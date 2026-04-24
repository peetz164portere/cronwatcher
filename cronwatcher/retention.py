"""Retention policy management — define and enforce how long job history is kept."""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional


def init_retention(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS retention_policies (
            job_name TEXT PRIMARY KEY,
            max_days INTEGER NOT NULL,
            max_runs INTEGER,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def set_retention(
    conn: sqlite3.Connection,
    job_name: str,
    max_days: int,
    max_runs: Optional[int] = None,
) -> None:
    if max_days < 1:
        raise ValueError("max_days must be at least 1")
    if max_runs is not None and max_runs < 1:
        raise ValueError("max_runs must be at least 1 if specified")
    now = datetime.utcnow().isoformat()
    conn.execute("""
        INSERT INTO retention_policies (job_name, max_days, max_runs, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET
            max_days=excluded.max_days,
            max_runs=excluded.max_runs,
            updated_at=excluded.updated_at
    """, (job_name.lower(), max_days, max_runs, now))
    conn.commit()


def get_retention(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT job_name, max_days, max_runs, updated_at FROM retention_policies WHERE job_name = ?",
        (job_name.lower(),)
    ).fetchone()
    if row is None:
        return None
    return {"job_name": row[0], "max_days": row[1], "max_runs": row[2], "updated_at": row[3]}


def remove_retention(conn: sqlite3.Connection, job_name: str) -> bool:
    cur = conn.execute(
        "DELETE FROM retention_policies WHERE job_name = ?", (job_name.lower(),)
    )
    conn.commit()
    return cur.rowcount > 0


def list_retention(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, max_days, max_runs, updated_at FROM retention_policies ORDER BY job_name"
    ).fetchall()
    return [{"job_name": r[0], "max_days": r[1], "max_runs": r[2], "updated_at": r[3]} for r in rows]


def apply_retention(conn: sqlite3.Connection, job_name: str) -> int:
    """Delete runs that exceed the retention policy. Returns number of rows deleted."""
    policy = get_retention(conn, job_name)
    if policy is None:
        return 0

    deleted = 0
    cutoff = (datetime.utcnow() - timedelta(days=policy["max_days"])).isoformat()
    cur = conn.execute(
        "DELETE FROM runs WHERE job_name = ? AND started_at < ?",
        (job_name, cutoff)
    )
    deleted += cur.rowcount

    if policy["max_runs"] is not None:
        keep_ids = conn.execute(
            "SELECT id FROM runs WHERE job_name = ? ORDER BY started_at DESC LIMIT ?",
            (job_name, policy["max_runs"])
        ).fetchall()
        if keep_ids:
            placeholders = ",".join("?" * len(keep_ids))
            ids = [r[0] for r in keep_ids]
            cur = conn.execute(
                f"DELETE FROM runs WHERE job_name = ? AND id NOT IN ({placeholders})",
                [job_name] + ids
            )
            deleted += cur.rowcount

    conn.commit()
    return deleted
