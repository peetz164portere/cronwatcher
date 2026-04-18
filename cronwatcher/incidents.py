import sqlite3
from datetime import datetime
from typing import Optional


def init_incidents(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            run_id INTEGER,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            note TEXT
        )
    """)
    conn.commit()


def open_incident(conn: sqlite3.Connection, job_name: str, run_id: Optional[int] = None, note: Optional[str] = None) -> int:
    existing = get_open_incident(conn, job_name)
    if existing:
        return existing["id"]
    cur = conn.execute(
        "INSERT INTO incidents (job_name, run_id, opened_at, note) VALUES (?, ?, ?, ?)",
        (job_name, run_id, datetime.utcnow().isoformat(), note),
    )
    conn.commit()
    return cur.lastrowid


def close_incident(conn: sqlite3.Connection, job_name: str, note: Optional[str] = None) -> bool:
    incident = get_open_incident(conn, job_name)
    if not incident:
        return False
    conn.execute(
        "UPDATE incidents SET status='closed', closed_at=?, note=COALESCE(?, note) WHERE id=?",
        (datetime.utcnow().isoformat(), note, incident["id"]),
    )
    conn.commit()
    return True


def get_open_incident(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM incidents WHERE job_name=? AND status='open' ORDER BY opened_at DESC LIMIT 1",
        (job_name,),
    ).fetchone()
    return dict(row) if row else None


def list_incidents(conn: sqlite3.Connection, job_name: Optional[str] = None, status: Optional[str] = None) -> list:
    query = "SELECT * FROM incidents WHERE 1=1"
    params = []
    if job_name:
        query += " AND job_name=?"
        params.append(job_name)
    if status:
        query += " AND status=?"
        params.append(status)
    query += " ORDER BY opened_at DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]
