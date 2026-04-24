"""Trigger rules: define conditions that cause a job to fire an alert or action."""

import sqlite3
import json
from typing import Optional

VALID_CONDITIONS = {"on_failure", "on_success", "on_slow", "on_overdue", "always"}


def init_triggers(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS triggers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            condition TEXT NOT NULL,
            action TEXT NOT NULL,
            params TEXT NOT NULL DEFAULT '{}',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_trigger_job_cond_action "
        "ON triggers (job_name, condition, action)"
    )
    conn.commit()


def add_trigger(
    conn: sqlite3.Connection,
    job_name: str,
    condition: str,
    action: str,
    params: Optional[dict] = None,
) -> int:
    condition = condition.lower()
    if condition not in VALID_CONDITIONS:
        raise ValueError(f"Invalid condition '{condition}'. Must be one of {VALID_CONDITIONS}")
    job_name = job_name.lower()
    params_json = json.dumps(params or {})
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO triggers (job_name, condition, action, params)
        VALUES (?, ?, ?, ?)
        """,
        (job_name, condition, action, params_json),
    )
    conn.commit()
    if cur.lastrowid and cur.rowcount:
        return cur.lastrowid
    row = conn.execute(
        "SELECT id FROM triggers WHERE job_name=? AND condition=? AND action=?",
        (job_name, condition, action),
    ).fetchone()
    return row[0]


def get_triggers(conn: sqlite3.Connection, job_name: str, condition: Optional[str] = None) -> list:
    job_name = job_name.lower()
    if condition:
        rows = conn.execute(
            "SELECT id, job_name, condition, action, params, enabled FROM triggers "
            "WHERE job_name=? AND condition=? AND enabled=1",
            (job_name, condition.lower()),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, job_name, condition, action, params, enabled FROM triggers "
            "WHERE job_name=? AND enabled=1",
            (job_name,),
        ).fetchall()
    return [
        {
            "id": r[0],
            "job_name": r[1],
            "condition": r[2],
            "action": r[3],
            "params": json.loads(r[4]),
            "enabled": bool(r[5]),
        }
        for r in rows
    ]


def remove_trigger(conn: sqlite3.Connection, trigger_id: int) -> bool:
    cur = conn.execute("DELETE FROM triggers WHERE id=?", (trigger_id,))
    conn.commit()
    return cur.rowcount > 0


def set_enabled(conn: sqlite3.Connection, trigger_id: int, enabled: bool) -> bool:
    cur = conn.execute(
        "UPDATE triggers SET enabled=? WHERE id=?", (1 if enabled else 0, trigger_id)
    )
    conn.commit()
    return cur.rowcount > 0


def list_all_triggers(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT id, job_name, condition, action, params, enabled FROM triggers ORDER BY job_name, condition"
    ).fetchall()
    return [
        {
            "id": r[0],
            "job_name": r[1],
            "condition": r[2],
            "action": r[3],
            "params": json.loads(r[4]),
            "enabled": bool(r[5]),
        }
        for r in rows
    ]
