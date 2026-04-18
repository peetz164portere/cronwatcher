"""Audit log — record CLI actions for traceability."""
import sqlite3
from datetime import datetime, timezone
from typing import Optional


def init_audit(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            target TEXT,
            detail TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def record_action(
    conn: sqlite3.Connection,
    action: str,
    target: Optional[str] = None,
    detail: Optional[str] = None,
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO audit_log (action, target, detail, created_at) VALUES (?, ?, ?, ?)",
        (action, target, detail, now),
    )
    conn.commit()
    return cur.lastrowid


def get_audit_log(
    conn: sqlite3.Connection,
    action: Optional[str] = None,
    limit: int = 50,
) -> list:
    if action:
        rows = conn.execute(
            "SELECT id, action, target, detail, created_at FROM audit_log "
            "WHERE action = ? ORDER BY id DESC LIMIT ?",
            (action, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, action, target, detail, created_at FROM audit_log "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def clear_audit_log(conn: sqlite3.Connection) -> int:
    cur = conn.execute("DELETE FROM audit_log")
    conn.commit()
    return cur.rowcount
