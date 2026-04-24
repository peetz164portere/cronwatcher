"""Workflow definitions: ordered sequences of jobs with dependency tracking."""

import sqlite3
import json
from typing import Optional


def init_workflows(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workflows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            steps TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def create_workflow(conn: sqlite3.Connection, name: str, description: str = "", steps: Optional[list] = None) -> int:
    name = name.lower().strip()
    steps_json = json.dumps(steps or [])
    cur = conn.execute(
        "INSERT OR IGNORE INTO workflows (name, description, steps) VALUES (?, ?, ?)",
        (name, description, steps_json),
    )
    conn.commit()
    return cur.lastrowid


def get_workflow(conn: sqlite3.Connection, name: str) -> Optional[dict]:
    name = name.lower().strip()
    row = conn.execute(
        "SELECT id, name, description, steps, created_at FROM workflows WHERE name = ?",
        (name,),
    ).fetchone()
    if row is None:
        return None
    return {
        "id": row[0],
        "name": row[1],
        "description": row[2],
        "steps": json.loads(row[3]),
        "created_at": row[4],
    }


def update_steps(conn: sqlite3.Connection, name: str, steps: list) -> bool:
    name = name.lower().strip()
    cur = conn.execute(
        "UPDATE workflows SET steps = ? WHERE name = ?",
        (json.dumps(steps), name),
    )
    conn.commit()
    return cur.rowcount > 0


def remove_workflow(conn: sqlite3.Connection, name: str) -> bool:
    name = name.lower().strip()
    cur = conn.execute("DELETE FROM workflows WHERE name = ?", (name,))
    conn.commit()
    return cur.rowcount > 0


def list_workflows(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT id, name, description, steps, created_at FROM workflows ORDER BY name"
    ).fetchall()
    return [
        {"id": r[0], "name": r[1], "description": r[2], "steps": json.loads(r[3]), "created_at": r[4]}
        for r in rows
    ]


def get_next_step(conn: sqlite3.Connection, name: str, completed: list) -> Optional[str]:
    """Return the first step not yet in completed, or None if all done."""
    wf = get_workflow(conn, name)
    if wf is None:
        return None
    for step in wf["steps"]:
        if step not in completed:
            return step
    return None
