"""Capture and store environment metadata for cron runs."""
import os
import socket
import json
from .storage import get_connection


def init_env_log(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_env (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            hostname TEXT,
            user TEXT,
            env_vars TEXT,
            captured_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def capture_env(extra_keys=None):
    """Return a dict of environment metadata."""
    env_keys = list(extra_keys or [])
    env_snapshot = {k: os.environ.get(k) for k in env_keys if os.environ.get(k)}
    return {
        "hostname": socket.gethostname(),
        "user": os.environ.get("USER") or os.environ.get("USERNAME") or "unknown",
        "env_vars": env_snapshot,
    }


def save_env(conn, run_id, env_data):
    """Persist env metadata tied to a run."""
    conn.execute(
        """
        INSERT INTO run_env (run_id, hostname, user, env_vars)
        VALUES (?, ?, ?, ?)
        """,
        (
            run_id,
            env_data.get("hostname"),
            env_data.get("user"),
            json.dumps(env_data.get("env_vars", {})),
        ),
    )
    conn.commit()


def get_env(conn, run_id):
    """Fetch env metadata for a run, or None if not found."""
    row = conn.execute(
        "SELECT hostname, user, env_vars FROM run_env WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "hostname": row[0],
        "user": row[1],
        "env_vars": json.loads(row[2] or "{}"),
    }
