"""Track job ownership (team/person responsible for a cron job)."""

import sqlite3
from typing import Optional


def init_ownership(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ownership (
            job_name TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            email TEXT,
            team TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def set_owner(
    conn: sqlite3.Connection,
    job_name: str,
    owner: str,
    email: Optional[str] = None,
    team: Optional[str] = None,
) -> None:
    job_name = job_name.lower()
    conn.execute(
        """
        INSERT INTO ownership (job_name, owner, email, team, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            owner=excluded.owner,
            email=excluded.email,
            team=excluded.team,
            updated_at=excluded.updated_at
        """,
        (job_name, owner, email, team),
    )
    conn.commit()


def get_owner(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT job_name, owner, email, team, updated_at FROM ownership WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if row is None:
        return None
    return dict(zip(["job_name", "owner", "email", "team", "updated_at"], row))


def remove_owner(conn: sqlite3.Connection, job_name: str) -> bool:
    job_name = job_name.lower()
    cur = conn.execute("DELETE FROM ownership WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0


def list_owners(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, owner, email, team, updated_at FROM ownership ORDER BY job_name"
    ).fetchall()
    keys = ["job_name", "owner", "email", "team", "updated_at"]
    return [dict(zip(keys, row)) for row in rows]
