"""Snapshot current job status summary to a JSON file."""

import json
import os
from datetime import datetime, timezone
from cronwatcher.storage import get_connection, fetch_history
from cronwatcher.search import count_by_status


def build_snapshot(db_path: str) -> dict:
    """Build a snapshot dict of all known jobs and their latest run."""
    conn = get_connection(db_path)
    rows = fetch_history(conn, limit=1000)
    conn.close()

    seen = {}
    for row in rows:
        name = row["job_name"]
        if name not in seen:
            seen[name] = row

    jobs = []
    for name, row in seen.items():
        jobs.append({
            "job_name": name,
            "last_status": row["status"],
            "last_started_at": row["started_at"],
            "last_finished_at": row["finished_at"],
            "exit_code": row["exit_code"],
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_jobs": len(jobs),
        "jobs": jobs,
    }


def save_snapshot(snapshot: dict, output_path: str) -> None:
    """Write snapshot dict to a JSON file."""
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(snapshot, f, indent=2)


def load_snapshot(path: str) -> dict:
    """Load a previously saved snapshot from disk."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)
