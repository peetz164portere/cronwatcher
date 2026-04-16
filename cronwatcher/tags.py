"""Tag support for cron jobs — store and query tags associated with job names."""

import sqlite3
from typing import List, Optional


def init_tags(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            tag TEXT NOT NULL,
            UNIQUE(job_name, tag)
        )
    """)
    conn.commit()


def add_tag(conn: sqlite3.Connection, job_name: str, tag: str) -> None:
    """Add a tag to a job. Silently ignores duplicates."""
    conn.execute(
        "INSERT OR IGNORE INTO job_tags (job_name, tag) VALUES (?, ?)",
        (job_name, tag.strip().lower()),
    )
    conn.commit()


def remove_tag(conn: sqlite3.Connection, job_name: str, tag: str) -> None:
    conn.execute(
        "DELETE FROM job_tags WHERE job_name = ? AND tag = ?",
        (job_name, tag.strip().lower()),
    )
    conn.commit()


def get_tags(conn: sqlite3.Connection, job_name: str) -> List[str]:
    cur = conn.execute(
        "SELECT tag FROM job_tags WHERE job_name = ? ORDER BY tag",
        (job_name,),
    )
    return [row[0] for row in cur.fetchall()]


def get_jobs_by_tag(conn: sqlite3.Connection, tag: str) -> List[str]:
    cur = conn.execute(
        "SELECT DISTINCT job_name FROM job_tags WHERE tag = ? ORDER BY job_name",
        (tag.strip().lower(),),
    )
    return [row[0] for row in cur.fetchall()]


def clear_tags(conn: sqlite3.Connection, job_name: str) -> None:
    conn.execute("DELETE FROM job_tags WHERE job_name = ?", (job_name,))
    conn.commit()


def rename_job(conn: sqlite3.Connection, old_name: str, new_name: str) -> int:
    """Update all tag records when a job is renamed.

    Returns the number of rows updated.
    """
    cur = conn.execute(
        "UPDATE job_tags SET job_name = ? WHERE job_name = ?",
        (new_name, old_name),
    )
    conn.commit()
    return cur.rowcount
