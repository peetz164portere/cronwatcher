"""Job-to-webhook routing: map job names to specific webhook URLs."""

import sqlite3
from typing import Optional


def init_routing(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_routes (
            job_name TEXT PRIMARY KEY,
            webhook_url TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def set_route(conn: sqlite3.Connection, job_name: str, webhook_url: str) -> None:
    """Assign a webhook URL to a specific job."""
    conn.execute("""
        INSERT INTO job_routes (job_name, webhook_url)
        VALUES (?, ?)
        ON CONFLICT(job_name) DO UPDATE SET webhook_url=excluded.webhook_url
    """, (job_name.lower(), webhook_url))
    conn.commit()


def get_route(conn: sqlite3.Connection, job_name: str) -> Optional[str]:
    """Return the webhook URL for a job, or None if not set."""
    row = conn.execute(
        "SELECT webhook_url FROM job_routes WHERE job_name = ?",
        (job_name.lower(),)
    ).fetchone()
    return row[0] if row else None


def remove_route(conn: sqlite3.Connection, job_name: str) -> bool:
    """Remove a job route. Returns True if a row was deleted."""
    cur = conn.execute(
        "DELETE FROM job_routes WHERE job_name = ?",
        (job_name.lower(),)
    )
    conn.commit()
    return cur.rowcount > 0


def list_routes(conn: sqlite3.Connection) -> list[dict]:
    """Return all job routes as a list of dicts."""
    rows = conn.execute(
        "SELECT job_name, webhook_url, created_at FROM job_routes ORDER BY job_name"
    ).fetchall()
    return [{"job_name": r[0], "webhook_url": r[1], "created_at": r[2]} for r in rows]


def resolve_webhook(conn: sqlite3.Connection, job_name: str, default_url: Optional[str]) -> Optional[str]:
    """Return the job-specific webhook if set, otherwise fall back to default_url."""
    return get_route(conn, job_name) or default_url
