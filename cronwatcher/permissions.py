import sqlite3
from typing import Optional, List, Dict

VALID_ACTIONS = {"run", "view", "edit", "delete", "admin"}


def init_permissions(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            principal TEXT NOT NULL,
            action TEXT NOT NULL,
            granted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(job_name, principal, action)
        )
    """)
    conn.commit()


def grant_permission(conn: sqlite3.Connection, job_name: str, principal: str, action: str) -> int:
    action = action.lower()
    job_name = job_name.lower()
    principal = principal.lower()
    if action not in VALID_ACTIONS:
        raise ValueError(f"Invalid action '{action}'. Must be one of: {sorted(VALID_ACTIONS)}")
    cur = conn.execute("""
        INSERT OR IGNORE INTO permissions (job_name, principal, action)
        VALUES (?, ?, ?)
    """, (job_name, principal, action))
    conn.commit()
    if cur.lastrowid and cur.rowcount:
        return cur.lastrowid
    row = conn.execute(
        "SELECT id FROM permissions WHERE job_name=? AND principal=? AND action=?",
        (job_name, principal, action)
    ).fetchone()
    return row[0]


def revoke_permission(conn: sqlite3.Connection, job_name: str, principal: str, action: str) -> bool:
    action = action.lower()
    job_name = job_name.lower()
    principal = principal.lower()
    cur = conn.execute("""
        DELETE FROM permissions WHERE job_name=? AND principal=? AND action=?
    """, (job_name, principal, action))
    conn.commit()
    return cur.rowcount > 0


def has_permission(conn: sqlite3.Connection, job_name: str, principal: str, action: str) -> bool:
    job_name = job_name.lower()
    principal = principal.lower()
    action = action.lower()
    row = conn.execute("""
        SELECT 1 FROM permissions WHERE job_name=? AND principal=? AND action=?
    """, (job_name, principal, action)).fetchone()
    return row is not None


def get_permissions(conn: sqlite3.Connection, job_name: str) -> List[Dict]:
    job_name = job_name.lower()
    rows = conn.execute("""
        SELECT id, job_name, principal, action, granted_at
        FROM permissions WHERE job_name=? ORDER BY principal, action
    """, (job_name,)).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "principal": r[2], "action": r[3], "granted_at": r[4]}
        for r in rows
    ]


def list_all_permissions(conn: sqlite3.Connection) -> List[Dict]:
    rows = conn.execute("""
        SELECT id, job_name, principal, action, granted_at
        FROM permissions ORDER BY job_name, principal, action
    """).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "principal": r[2], "action": r[3], "granted_at": r[4]}
        for r in rows
    ]
