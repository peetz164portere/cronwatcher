"""Annotations: attach notes to cron job runs."""
import sqlite3
from datetime import datetime
from typing import Optional


def init_annotations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            note TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def add_annotation(conn: sqlite3.Connection, run_id: int, note: str) -> int:
    cur = conn.execute(
        "INSERT INTO annotations (run_id, note, created_at) VALUES (?, ?, ?)",
        (run_id, note, datetime.utcnow().isoformat()),
    )
    conn.commit()
    return cur.lastrowid


def get_annotations(conn: sqlite3.Connection, run_id: int) -> list[dict]:
    cur = conn.execute(
        "SELECT id, run_id, note, created_at FROM annotations WHERE run_id = ? ORDER BY created_at",
        (run_id,),
    )
    rows = cur.fetchall()
    return [
        {"id": r[0], "run_id": r[1], "note": r[2], "created_at": r[3]}
        for r in rows
    ]


def delete_annotation(conn: sqlite3.Connection, annotation_id: int) -> bool:
    cur = conn.execute("DELETE FROM annotations WHERE id = ?", (annotation_id,))
    conn.commit()
    return cur.rowcount > 0


def get_all_annotations(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute(
        "SELECT id, run_id, note, created_at FROM annotations ORDER BY created_at"
    )
    rows = cur.fetchall()
    return [
        {"id": r[0], "run_id": r[1], "note": r[2], "created_at": r[3]}
        for r in rows
    ]
