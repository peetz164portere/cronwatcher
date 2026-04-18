"""Watchdog: detect jobs that started but never finished (hung/zombie runs)."""

from datetime import datetime, timedelta
from cronwatcher.storage import get_connection


def get_hung_runs(db_path: str, timeout_minutes: int = 60) -> list[dict]:
    """Return runs that are still 'running' after timeout_minutes."""
    cutoff = datetime.utcnow() - timedelta(minutes=timeout_minutes)
    conn = get_connection(db_path)
    rows = conn.execute(
        """
        SELECT id, job_name, started_at
        FROM runs
        WHERE status = 'running'
          AND started_at < ?
        ORDER BY started_at ASC
        """,
        (cutoff.isoformat(),),
    ).fetchall()
    conn.close()
    return [{"id": r[0], "job_name": r[1], "started_at": r[2]} for r in rows]


def mark_hung_as_failed(db_path: str, run_id: int, note: str = "hung") -> None:
    """Mark a specific hung run as failed with an optional note."""
    conn = get_connection(db_path)
    conn.execute(
        """
        UPDATE runs
        SET status = 'failure', output = ?
        WHERE id = ? AND status = 'running'
        """,
        (note, run_id),
    )
    conn.commit()
    conn.close()


def resolve_hung_runs(
    db_path: str, timeout_minutes: int = 60, dry_run: bool = False
) -> list[dict]:
    """Find and optionally mark all hung runs as failed. Returns affected rows."""
    hung = get_hung_runs(db_path, timeout_minutes)
    if not dry_run:
        for run in hung:
            mark_hung_as_failed(db_path, run["id"])
    return hung
