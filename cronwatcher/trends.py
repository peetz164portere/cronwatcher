"""Trend analysis: detect improving/degrading job performance over time."""

from __future__ import annotations
import sqlite3
from typing import Optional
from datetime import datetime, timedelta


def get_recent_durations(
    conn: sqlite3.Connection, job_name: str, limit: int = 20
) -> list[float]:
    """Return the last `limit` successful run durations for a job."""
    rows = conn.execute(
        """
        SELECT duration_seconds FROM runs
        WHERE job_name = ? AND status = 'success' AND duration_seconds IS NOT NULL
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (job_name, limit),
    ).fetchall()
    return [r[0] for r in reversed(rows)]


def _linear_slope(values: list[float]) -> float:
    """Compute slope via simple linear regression."""
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    return num / den if den else 0.0


def analyze_trend(
    conn: sqlite3.Connection, job_name: str, limit: int = 20
) -> dict:
    """Return trend info for a job's duration over recent runs."""
    durations = get_recent_durations(conn, job_name, limit)
    if len(durations) < 2:
        return {"job": job_name, "trend": "insufficient_data", "slope": None, "runs": len(durations)}

    slope = _linear_slope(durations)
    avg = sum(durations) / len(durations)

    if slope > avg * 0.05:
        trend = "degrading"
    elif slope < -avg * 0.05:
        trend = "improving"
    else:
        trend = "stable"

    return {
        "job": job_name,
        "trend": trend,
        "slope": round(slope, 4),
        "avg_duration": round(avg, 4),
        "runs": len(durations),
    }


def get_all_job_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT job_name FROM runs ORDER BY job_name"
    ).fetchall()
    return [r[0] for r in rows]


def analyze_all_trends(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    """Return trend analysis for every known job."""
    return [analyze_trend(conn, name, limit) for name in get_all_job_names(conn)]
