"""Heartbeat ping support — send a GET request to a URL after successful job runs."""

import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional


def send_heartbeat(url: str, timeout: int = 10) -> bool:
    """Send a heartbeat ping to the given URL. Returns True on success."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status < 400
    except (urllib.error.URLError, OSError):
        return False


def init_heartbeat_log(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS heartbeat_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            url TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            success INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.commit()


def record_heartbeat(conn, job_name: str, url: str, success: bool) -> int:
    sent_at = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO heartbeat_log (job_name, url, sent_at, success) VALUES (?, ?, ?, ?)",
        (job_name, url, sent_at, int(success)),
    )
    conn.commit()
    return cur.lastrowid


def get_heartbeat_history(conn, job_name: str, limit: int = 20) -> list:
    cur = conn.execute(
        "SELECT job_name, url, sent_at, success FROM heartbeat_log WHERE job_name = ? ORDER BY sent_at DESC LIMIT ?",
        (job_name, limit),
    )
    return [{"job_name": r[0], "url": r[1], "sent_at": r[2], "success": bool(r[3])} for r in cur.fetchall()]


def maybe_heartbeat(conn, job_name: str, url: Optional[str], success: bool) -> None:
    """Send heartbeat only if job succeeded and URL is configured."""
    if not url or not success:
        return
    ok = send_heartbeat(url)
    record_heartbeat(conn, job_name, url, ok)
