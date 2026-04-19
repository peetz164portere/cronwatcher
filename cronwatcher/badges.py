"""Badge generation for job status (shields.io-compatible JSON)."""

import json
from cronwatcher.storage import get_connection, fetch_history


STATUS_COLORS = {
    "success": "brightgreen",
    "failure": "red",
    "running": "blue",
    "unknown": "lightgrey",
}


def get_job_status(conn, job_name: str) -> str:
    """Return the most recent terminal status for a job."""
    rows = fetch_history(conn, job_name=job_name, limit=1)
    if not rows:
        return "unknown"
    row = rows[0]
    status = row["status"] if isinstance(row, dict) else row[3]
    return status if status in STATUS_COLORS else "unknown"


def build_badge(job_name: str, status: str) -> dict:
    """Build a shields.io-compatible badge dict."""
    color = STATUS_COLORS.get(status, "lightgrey")
    return {
        "schemaVersion": 1,
        "label": job_name,
        "message": status,
        "color": color,
    }


def get_badge(db_path: str, job_name: str) -> dict:
    """High-level: fetch status and return badge dict."""
    conn = get_connection(db_path)
    status = get_job_status(conn, job_name)
    conn.close()
    return build_badge(job_name, status)


def badge_json(db_path: str, job_name: str) -> str:
    """Return badge as JSON string."""
    return json.dumps(get_badge(db_path, job_name), indent=2)
