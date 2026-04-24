"""Run time budgets — set expected max duration per job and detect overruns."""

import sqlite3
from typing import Optional

VALID_ACTIONS = ("warn", "alert", "block")


def init_budgets(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS budgets (
            job_name TEXT PRIMARY KEY,
            max_seconds REAL NOT NULL,
            action TEXT NOT NULL DEFAULT 'warn',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def set_budget(
    conn: sqlite3.Connection,
    job_name: str,
    max_seconds: float,
    action: str = "warn",
) -> None:
    job_name = job_name.lower().strip()
    action = action.lower().strip()
    if action not in VALID_ACTIONS:
        raise ValueError(f"Invalid action '{action}'. Must be one of: {VALID_ACTIONS}")
    if max_seconds <= 0:
        raise ValueError("max_seconds must be positive")
    conn.execute(
        """
        INSERT INTO budgets (job_name, max_seconds, action, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            max_seconds = excluded.max_seconds,
            action = excluded.action,
            updated_at = excluded.updated_at
        """,
        (job_name, max_seconds, action),
    )
    conn.commit()


def get_budget(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower().strip()
    row = conn.execute(
        "SELECT job_name, max_seconds, action, created_at, updated_at FROM budgets WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if row is None:
        return None
    return dict(zip(["job_name", "max_seconds", "action", "created_at", "updated_at"], row))


def remove_budget(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower().strip()
    cur = conn.execute("DELETE FROM budgets WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def list_budgets(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, max_seconds, action, created_at, updated_at FROM budgets ORDER BY job_name"
    ).fetchall()
    keys = ["job_name", "max_seconds", "action", "created_at", "updated_at"]
    return [dict(zip(keys, r)) for r in rows]


def is_over_budget(conn: sqlite3.Connection, job_name: str, duration_seconds: float) -> Optional[dict]:
    """Returns the budget dict if the duration exceeds the budget, else None."""
    budget = get_budget(conn, job_name)
    if budget is None:
        return None
    if duration_seconds > budget["max_seconds"]:
        return budget
    return None
