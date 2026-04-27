"""Circuit breaker pattern for cron job alerting."""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional

STATES = ("closed", "open", "half_open")


def init_circuits(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS circuit_breakers (
            job_name TEXT PRIMARY KEY,
            state TEXT NOT NULL DEFAULT 'closed',
            failure_count INTEGER NOT NULL DEFAULT 0,
            last_failure_at TEXT,
            opened_at TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def get_circuit(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM circuit_breakers WHERE job_name = ?",
        (job_name.lower(),)
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def record_failure(conn: sqlite3.Connection, job_name: str, threshold: int = 3, recovery_seconds: int = 300) -> dict:
    job_name = job_name.lower()
    now = datetime.utcnow().isoformat()
    existing = get_circuit(conn, job_name)

    if existing is None:
        conn.execute(
            "INSERT INTO circuit_breakers (job_name, state, failure_count, last_failure_at, updated_at) VALUES (?, 'closed', 1, ?, ?)",
            (job_name, now, now)
        )
    else:
        new_count = existing["failure_count"] + 1
        new_state = "open" if new_count >= threshold else existing["state"]
        opened_at = now if new_state == "open" and existing["state"] != "open" else existing.get("opened_at")
        conn.execute(
            "UPDATE circuit_breakers SET failure_count = ?, state = ?, last_failure_at = ?, opened_at = ?, updated_at = ? WHERE job_name = ?",
            (new_count, new_state, now, opened_at, now, job_name)
        )
    conn.commit()
    return get_circuit(conn, job_name)


def record_success(conn: sqlite3.Connection, job_name: str) -> dict:
    job_name = job_name.lower()
    now = datetime.utcnow().isoformat()
    existing = get_circuit(conn, job_name)
    if existing is None:
        conn.execute(
            "INSERT INTO circuit_breakers (job_name, state, failure_count, updated_at) VALUES (?, 'closed', 0, ?)",
            (job_name, now)
        )
    else:
        conn.execute(
            "UPDATE circuit_breakers SET state = 'closed', failure_count = 0, opened_at = NULL, updated_at = ? WHERE job_name = ?",
            (now, job_name)
        )
    conn.commit()
    return get_circuit(conn, job_name)


def is_open(conn: sqlite3.Connection, job_name: str, recovery_seconds: int = 300) -> bool:
    circuit = get_circuit(conn, job_name)
    if circuit is None or circuit["state"] == "closed":
        return False
    if circuit["state"] == "open" and circuit.get("opened_at"):
        opened = datetime.fromisoformat(circuit["opened_at"])
        if datetime.utcnow() - opened > timedelta(seconds=recovery_seconds):
            _set_half_open(conn, job_name)
            return False
    return circuit["state"] == "open"


def _set_half_open(conn: sqlite3.Connection, job_name: str) -> None:
    now = datetime.utcnow().isoformat()
    conn.execute(
        "UPDATE circuit_breakers SET state = 'half_open', updated_at = ? WHERE job_name = ?",
        (now, job_name.lower())
    )
    conn.commit()


def reset_circuit(conn: sqlite3.Connection, job_name: str) -> None:
    conn.execute("DELETE FROM circuit_breakers WHERE job_name = ?", (job_name.lower(),))
    conn.commit()


def list_circuits(conn: sqlite3.Connection) -> list:
    rows = conn.execute("SELECT * FROM circuit_breakers ORDER BY job_name").fetchall()
    return [dict(r) for r in rows]
