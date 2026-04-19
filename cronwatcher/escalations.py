import sqlite3
from datetime import datetime
from typing import Optional


def init_escalations(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS escalations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            level INTEGER NOT NULL DEFAULT 1,
            webhook_url TEXT NOT NULL,
            threshold_minutes INTEGER NOT NULL DEFAULT 30,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()


def set_escalation(conn: sqlite3.Connection, job_name: str, level: int,
                   webhook_url: str, threshold_minutes: int = 30) -> int:
    job_name = job_name.lower()
    conn.execute("""
        INSERT INTO escalations (job_name, level, webhook_url, threshold_minutes, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (job_name, level, webhook_url, threshold_minutes, datetime.utcnow().isoformat()))
    conn.commit()
    row = conn.execute(
        "SELECT id FROM escalations WHERE job_name=? AND level=? ORDER BY id DESC LIMIT 1",
        (job_name, level)
    ).fetchone()
    return row[0]


def get_escalations(conn: sqlite3.Connection, job_name: str) -> list:
    rows = conn.execute("""
        SELECT id, job_name, level, webhook_url, threshold_minutes, created_at
        FROM escalations WHERE job_name=? ORDER BY level ASC
    """, (job_name.lower(),)).fetchall()
    return [
        {"id": r[0], "job_name": r[1], "level": r[2],
         "webhook_url": r[3], "threshold_minutes": r[4], "created_at": r[5]}
        for r in rows
    ]


def remove_escalation(conn: sqlite3.Connection, escalation_id: int) -> bool:
    cur = conn.execute("DELETE FROM escalations WHERE id=?", (escalation_id,))
    conn.commit()
    return cur.rowcount > 0


def get_next_level(conn: sqlite3.Connection, job_name: str, current_level: int) -> Optional[dict]:
    row = conn.execute("""
        SELECT id, job_name, level, webhook_url, threshold_minutes, created_at
        FROM escalations WHERE job_name=? AND level>? ORDER BY level ASC LIMIT 1
    """, (job_name.lower(), current_level)).fetchone()
    if row is None:
        return None
    return {"id": row[0], "job_name": row[1], "level": row[2],
            "webhook_url": row[3], "threshold_minutes": row[4], "created_at": row[5]}
