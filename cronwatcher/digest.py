"""Daily/periodic digest report generation for cron job summaries."""

from datetime import datetime, timedelta
from typing import Optional
from cronwatcher.storage import fetch_history
from cronwatcher.formatter import format_duration, format_timestamp


def build_digest(db_path: str, hours: int = 24) -> dict:
    """Build a digest summary of cron job runs over the last N hours."""
    rows = fetch_history(db_path, limit=500)
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    recent = [
        r for r in rows
        if r["started_at"] and _parse_dt(r["started_at"]) >= cutoff
    ]

    total = len(recent)
    failures = [r for r in recent if r["exit_code"] not in (None, 0)]
    successes = [r for r in recent if r["exit_code"] == 0]
    running = [r for r in recent if r["exit_code"] is None]

    jobs: dict = {}
    for row in recent:
        name = row["job_name"]
        if name not in jobs:
            jobs[name] = {"total": 0, "failures": 0}
        jobs[name]["total"] += 1
        if row["exit_code"] not in (None, 0):
            jobs[name]["failures"] += 1

    return {
        "period_hours": hours,
        "generated_at": datetime.utcnow().isoformat(),
        "total_runs": total,
        "successful_runs": len(successes),
        "failed_runs": len(failures),
        "running_runs": len(running),
        "jobs": jobs,
        "failure_rate": round(len(failures) / total * 100, 1) if total else 0.0,
    }


def format_digest_text(digest: dict) -> str:
    """Render a digest dict as a human-readable text report."""
    lines = [
        f"=== CronWatcher Digest (last {digest['period_hours']}h) ===",
        f"Generated: {digest['generated_at']}",
        "",
        f"Total runs  : {digest['total_runs']}",
        f"Successes   : {digest['successful_runs']}",
        f"Failures    : {digest['failed_runs']}",
        f"Still running: {digest['running_runs']}",
        f"Failure rate: {digest['failure_rate']}%",
        "",
        "Per-job breakdown:",
    ]
    for job, stats in digest["jobs"].items():
        rate = round(stats["failures"] / stats["total"] * 100, 1) if stats["total"] else 0
        lines.append(f"  {job}: {stats['total']} runs, {stats['failures']} failures ({rate}%)")

    if not digest["jobs"]:
        lines.append("  (no runs recorded in this period)")

    return "\n".join(lines)


def get_most_failing_job(digest: dict) -> Optional[str]:
    """Return the job name with the highest failure count, or None if no jobs."""
    jobs = digest.get("jobs", {})
    if not jobs:
        return None
    return max(jobs, key=lambda name: jobs[name]["failures"])


def _parse_dt(value: str) -> datetime:
    """Parse an ISO datetime string, tolerating missing microseconds."""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {value}")
