"""Snapshot diffing utilities — compare two snapshots and summarize changes."""

from __future__ import annotations

from typing import Any


DIFF_ADDED = "added"
DIFF_REMOVED = "removed"
DIFF_CHANGED = "changed"
DIFF_UNCHANGED = "unchanged"


def diff_snapshots(old: dict[str, Any], new: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a list of per-job diff entries between two snapshots."""
    old_jobs: dict[str, str] = old.get("jobs", {})
    new_jobs: dict[str, str] = new.get("jobs", {})

    all_jobs = set(old_jobs) | set(new_jobs)
    result = []

    for job in sorted(all_jobs):
        if job not in old_jobs:
            result.append({"job": job, "change": DIFF_ADDED, "old": None, "new": new_jobs[job]})
        elif job not in new_jobs:
            result.append({"job": job, "change": DIFF_REMOVED, "old": old_jobs[job], "new": None})
        elif old_jobs[job] != new_jobs[job]:
            result.append({"job": job, "change": DIFF_CHANGED, "old": old_jobs[job], "new": new_jobs[job]})
        else:
            result.append({"job": job, "change": DIFF_UNCHANGED, "old": old_jobs[job], "new": new_jobs[job]})

    return result


def has_changes(diff: list[dict[str, Any]]) -> bool:
    """Return True if any entry in the diff is not UNCHANGED."""
    return any(entry["change"] != DIFF_UNCHANGED for entry in diff)


def format_diff_text(diff: list[dict[str, Any]], show_unchanged: bool = False) -> str:
    """Render a human-readable diff summary."""
    lines = []
    symbols = {
        DIFF_ADDED: "[+]",
        DIFF_REMOVED: "[-]",
        DIFF_CHANGED: "[~]",
        DIFF_UNCHANGED: "[ ]",
    }

    for entry in diff:
        change = entry["change"]
        if change == DIFF_UNCHANGED and not show_unchanged:
            continue
        sym = symbols.get(change, "[?]")
        if change == DIFF_CHANGED:
            lines.append(f"{sym} {entry['job']}: {entry['old']} → {entry['new']}")
        elif change == DIFF_ADDED:
            lines.append(f"{sym} {entry['job']}: (new) {entry['new']}")
        elif change == DIFF_REMOVED:
            lines.append(f"{sym} {entry['job']}: {entry['old']} (removed)")
        else:
            lines.append(f"{sym} {entry['job']}: {entry['new']}")

    return "\n".join(lines) if lines else "No changes."


def summary_counts(diff: list[dict[str, Any]]) -> dict[str, int]:
    """Return counts of each change type."""
    counts: dict[str, int] = {DIFF_ADDED: 0, DIFF_REMOVED: 0, DIFF_CHANGED: 0, DIFF_UNCHANGED: 0}
    for entry in diff:
        counts[entry["change"]] = counts.get(entry["change"], 0) + 1
    return counts
