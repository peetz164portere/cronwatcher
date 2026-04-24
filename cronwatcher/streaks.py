"""Track consecutive success/failure streaks per job."""

import sqlite3
from typing import Optional


def init_streaks(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS streaks (
            job_name TEXT PRIMARY KEY,
            current_streak INTEGER NOT NULL DEFAULT 0,
            streak_type TEXT NOT NULL DEFAULT 'success',
            longest_success INTEGER NOT NULL DEFAULT 0,
            longest_failure INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def update_streak(conn: sqlite3.Connection, job_name: str, status: str) -> dict:
    """Update streak counters for a job after a run. Returns updated row."""
    job_name = job_name.lower()
    now = __import__("datetime").datetime.utcnow().isoformat()

    row = conn.execute(
        "SELECT current_streak, streak_type, longest_success, longest_failure FROM streaks WHERE job_name = ?",
        (job_name,),
    ).fetchone()

    if row is None:
        current_streak, streak_type, longest_success, longest_failure = 0, None, 0, 0
    else:
        current_streak, streak_type, longest_success, longest_failure = row

    if streak_type == status:
        current_streak += 1
    else:
        current_streak = 1
        streak_type = status

    if status == "success":
        longest_success = max(longest_success, current_streak)
    else:
        longest_failure = max(longest_failure, current_streak)

    conn.execute(
        """
        INSERT INTO streaks (job_name, current_streak, streak_type, longest_success, longest_failure, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET
            current_streak = excluded.current_streak,
            streak_type = excluded.streak_type,
            longest_success = excluded.longest_success,
            longest_failure = excluded.longest_failure,
            updated_at = excluded.updated_at
        """,
        (job_name, current_streak, streak_type, longest_success, longest_failure, now),
    )
    conn.commit()

    return {
        "job_name": job_name,
        "current_streak": current_streak,
        "streak_type": streak_type,
        "longest_success": longest_success,
        "longest_failure": longest_failure,
        "updated_at": now,
    }


def get_streak(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    """Return streak info for a job, or None if not tracked yet."""
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT job_name, current_streak, streak_type, longest_success, longest_failure, updated_at FROM streaks WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if row is None:
        return None
    keys = ["job_name", "current_streak", "streak_type", "longest_success", "longest_failure", "updated_at"]
    return dict(zip(keys, row))


def list_streaks(conn: sqlite3.Connection) -> list:
    """Return streak info for all tracked jobs, ordered by job name."""
    rows = conn.execute(
        "SELECT job_name, current_streak, streak_type, longest_success, longest_failure, updated_at FROM streaks ORDER BY job_name"
    ).fetchall()
    keys = ["job_name", "current_streak", "streak_type", "longest_success", "longest_failure", "updated_at"]
    return [dict(zip(keys, row)) for row in rows]


def reset_streak(conn: sqlite3.Connection, job_name: str) -> bool:
    """Remove streak data for a job. Returns True if a row was deleted."""
    job_name = job_name.lower()
    cur = conn.execute("DELETE FROM streaks WHERE job_name = ?", (job_name,))
    conn.commit()
    return cur.rowcount > 0
