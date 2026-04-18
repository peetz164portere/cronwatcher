"""Compare two snapshots to detect changes in job health."""

from datetime import datetime
from typing import Optional


def compare_snapshots(old: dict, new: dict) -> dict:
    """Return a diff summary between two snapshots."""
    old_jobs = {j["job_name"]: j for j in old.get("jobs", [])}
    new_jobs = {j["job_name"]: j for j in new.get("jobs", [])}

    added = [name for name in new_jobs if name not in old_jobs]
    removed = [name for name in old_jobs if name not in new_jobs]
    changed = []

    for name in old_jobs:
        if name not in new_jobs:
            continue
        o = old_jobs[name]
        n = new_jobs[name]
        diffs = {}
        for key in ("last_status", "total_runs", "failure_count"):
            if o.get(key) != n.get(key):
                diffs[key] = {"old": o.get(key), "new": n.get(key)}
        if diffs:
            changed.append({"job_name": name, "changes": diffs})

    return {
        "compared_at": datetime.utcnow().isoformat(),
        "old_snapshot": old.get("created_at"),
        "new_snapshot": new.get("created_at"),
        "added": added,
        "removed": removed,
        "changed": changed,
        "summary": {
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
        },
    }


def format_compare_text(diff: dict) -> str:
    lines = [
        f"Snapshot comparison at {diff['compared_at']}",
        f"  Old: {diff['old_snapshot']}  New: {diff['new_snapshot']}",
        "",
    ]
    if diff["added"]:
        lines.append(f"Added jobs ({len(diff['added'])}):")
        for name in diff["added"]:
            lines.append(f"  + {name}")
    if diff["removed"]:
        lines.append(f"Removed jobs ({len(diff['removed'])}):")
        for name in diff["removed"]:
            lines.append(f"  - {name}")
    if diff["changed"]:
        lines.append(f"Changed jobs ({len(diff['changed'])}):")
        for entry in diff["changed"]:
            lines.append(f"  ~ {entry['job_name']}")
            for k, v in entry["changes"].items():
                lines.append(f"      {k}: {v['old']} -> {v['new']}")
    if not any([diff["added"], diff["removed"], diff["changed"]]):
        lines.append("No changes detected.")
    return "\n".join(lines)
