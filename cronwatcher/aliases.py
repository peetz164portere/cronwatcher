"""Job alias management — map short names to full job names."""

import sqlite3


def init_aliases(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS aliases (
            alias TEXT PRIMARY KEY,
            job_name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def set_alias(conn: sqlite3.Connection, alias: str, job_name: str) -> None:
    """Create or replace an alias pointing to job_name."""
    alias = alias.strip().lower()
    conn.execute(
        "INSERT OR REPLACE INTO aliases (alias, job_name) VALUES (?, ?)",
        (alias, job_name),
    )
    conn.commit()


def get_alias(conn: sqlite3.Connection, alias: str) -> str | None:
    """Return the job_name for the given alias, or None if not found."""
    alias = alias.strip().lower()
    row = conn.execute(
        "SELECT job_name FROM aliases WHERE alias = ?", (alias,)
    ).fetchone()
    return row[0] if row else None


def remove_alias(conn: sqlite3.Connection, alias: str) -> bool:
    """Delete an alias. Returns True if it existed."""
    alias = alias.strip().lower()
    cur = conn.execute("DELETE FROM aliases WHERE alias = ?", (alias,))
    conn.commit()
    return cur.rowcount > 0


def list_aliases(conn: sqlite3.Connection) -> list[dict]:
    """Return all aliases as a list of dicts."""
    rows = conn.execute(
        "SELECT alias, job_name, created_at FROM aliases ORDER BY alias"
    ).fetchall()
    return [{"alias": r[0], "job_name": r[1], "created_at": r[2]} for r in rows]


def resolve(conn: sqlite3.Connection, name: str) -> str:
    """Return job_name if name is a known alias, otherwise return name as-is."""
    resolved = get_alias(conn, name)
    return resolved if resolved is not None else name
