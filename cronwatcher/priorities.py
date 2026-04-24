import sqlite3
from typing import Optional

VALID_LEVELS = ("low", "normal", "high", "critical")


def init_priorities(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS priorities (
            job_name TEXT PRIMARY KEY,
            level     TEXT NOT NULL DEFAULT 'normal',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def set_priority(conn: sqlite3.Connection, job_name: str, level: str) -> None:
    level = level.lower()
    if level not in VALID_LEVELS:
        raise ValueError(f"Invalid priority level '{level}'. Choose from: {', '.join(VALID_LEVELS)}")
    job_name = job_name.lower()
    conn.execute(
        """
        INSERT INTO priorities (job_name, level, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            level = excluded.level,
            updated_at = excluded.updated_at
        """,
        (job_name, level),
    )
    conn.commit()


def get_priority(conn: sqlite3.Connection, job_name: str) -> str:
    """Return the priority level for a job, defaulting to 'normal'."""
    row = conn.execute(
        "SELECT level FROM priorities WHERE job_name = ?",
        (job_name.lower(),),
    ).fetchone()
    return row[0] if row else "normal"


def remove_priority(conn: sqlite3.Connection, job_name: str) -> bool:
    cur = conn.execute(
        "DELETE FROM priorities WHERE job_name = ?",
        (job_name.lower(),),
    )
    conn.commit()
    return cur.rowcount > 0


def list_priorities(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT job_name, level, updated_at FROM priorities ORDER BY job_name"
    ).fetchall()
    return [{"job_name": r[0], "level": r[1], "updated_at": r[2]} for r in rows]


def is_high_priority(conn: sqlite3.Connection, job_name: str) -> bool:
    level = get_priority(conn, job_name)
    return level in ("high", "critical")
