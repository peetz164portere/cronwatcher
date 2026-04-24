"""Job category management — group jobs into logical categories."""

import sqlite3
from typing import Optional


DEFAULT_CATEGORY = "uncategorized"


def init_categories(conn: sqlite3.Connection) -> None:
    """Create the categories table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            job_name TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            description TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def set_category(
    conn: sqlite3.Connection,
    job_name: str,
    category: str,
    description: Optional[str] = None,
) -> None:
    """Assign a category to a job, replacing any existing one."""
    job_name = job_name.lower().strip()
    category = category.lower().strip()
    conn.execute(
        """
        INSERT INTO categories (job_name, category, description)
        VALUES (?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET
            category = excluded.category,
            description = excluded.description
        """,
        (job_name, category, description),
    )
    conn.commit()


def get_category(conn: sqlite3.Connection, job_name: str) -> str:
    """Return the category for a job, or DEFAULT_CATEGORY if not set."""
    job_name = job_name.lower().strip()
    row = conn.execute(
        "SELECT category FROM categories WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    return row[0] if row else DEFAULT_CATEGORY


def remove_category(conn: sqlite3.Connection, job_name: str) -> bool:
    """Remove a job's category assignment. Returns True if a row was deleted."""
    job_name = job_name.lower().strip()
    cur = conn.execute(
        "DELETE FROM categories WHERE job_name = ?", (job_name,)
    )
    conn.commit()
    return cur.rowcount > 0


def list_categories(conn: sqlite3.Connection) -> list[dict]:
    """Return all category assignments as a list of dicts."""
    rows = conn.execute(
        "SELECT job_name, category, description, created_at FROM categories ORDER BY category, job_name"
    ).fetchall()
    return [
        {"job_name": r[0], "category": r[1], "description": r[2], "created_at": r[3]}
        for r in rows
    ]


def get_jobs_in_category(conn: sqlite3.Connection, category: str) -> list[str]:
    """Return all job names assigned to the given category."""
    category = category.lower().strip()
    rows = conn.execute(
        "SELECT job_name FROM categories WHERE category = ? ORDER BY job_name",
        (category,),
    ).fetchall()
    return [r[0] for r in rows]
