import sqlite3
from datetime import datetime, timezone
from typing import Optional


def init_checkpoints(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS checkpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            label TEXT NOT NULL,
            recorded_at TEXT NOT NULL,
            note TEXT,
            UNIQUE(job_name, label)
        )
    """)
    conn.commit()


def set_checkpoint(conn: sqlite3.Connection, job_name: str, label: str, note: Optional[str] = None) -> int:
    job_name = job_name.lower().strip()
    label = label.lower().strip()
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("""
        INSERT INTO checkpoints (job_name, label, recorded_at, note)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(job_name, label) DO UPDATE SET recorded_at=excluded.recorded_at, note=excluded.note
    """, (job_name, label, now, note))
    conn.commit()
    return cur.lastrowid


def get_checkpoint(conn: sqlite3.Connection, job_name: str, label: str) -> Optional[dict]:
    job_name = job_name.lower().strip()
    label = label.lower().strip()
    row = conn.execute("""
        SELECT job_name, label, recorded_at, note FROM checkpoints
        WHERE job_name = ? AND label = ?
    """, (job_name, label)).fetchone()
    if row is None:
        return None
    return {"job_name": row[0], "label": row[1], "recorded_at": row[2], "note": row[3]}


def list_checkpoints(conn: sqlite3.Connection, job_name: str) -> list:
    job_name = job_name.lower().strip()
    rows = conn.execute("""
        SELECT job_name, label, recorded_at, note FROM checkpoints
        WHERE job_name = ? ORDER BY recorded_at ASC
    """, (job_name,)).fetchall()
    return [{"job_name": r[0], "label": r[1], "recorded_at": r[2], "note": r[3]} for r in rows]


def remove_checkpoint(conn: sqlite3.Connection, job_name: str, label: str) -> bool:
    job_name = job_name.lower().strip()
    label = label.lower().strip()
    cur = conn.execute("DELETE FROM checkpoints WHERE job_name = ? AND label = ?", (job_name, label))
    conn.commit()
    return cur.rowcount > 0
