"""Forecast next run time and expected duration based on historical data."""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from cronwatcher.trends import get_recent_durations, _linear_slope


def init_forecasts(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            job_name TEXT PRIMARY KEY,
            predicted_duration_s REAL,
            predicted_next_run TEXT,
            confidence TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def compute_forecast(
    conn: sqlite3.Connection,
    job_name: str,
    interval_seconds: Optional[int] = None,
    sample_size: int = 10,
) -> Optional[dict]:
    """Compute a forecast for next run time and expected duration."""
    durations = get_recent_durations(conn, job_name, limit=sample_size)
    if not durations:
        return None

    avg_duration = sum(durations) / len(durations)
    slope = _linear_slope(durations)
    predicted_duration = max(0.0, avg_duration + slope * len(durations))

    confidence = "high" if len(durations) >= 7 else "medium" if len(durations) >= 3 else "low"

    predicted_next = None
    if interval_seconds:
        row = conn.execute(
            "SELECT started_at FROM runs WHERE job_name = ? AND status = 'success' "
            "ORDER BY started_at DESC LIMIT 1",
            (job_name,),
        ).fetchone()
        if row:
            last_run = datetime.fromisoformat(row[0])
            predicted_next = (last_run + timedelta(seconds=interval_seconds)).isoformat()

    return {
        "job_name": job_name,
        "predicted_duration_s": round(predicted_duration, 2),
        "predicted_next_run": predicted_next,
        "confidence": confidence,
        "sample_size": len(durations),
    }


def save_forecast(conn: sqlite3.Connection, forecast: dict) -> None:
    conn.execute(
        """
        INSERT INTO forecasts (job_name, predicted_duration_s, predicted_next_run, confidence, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET
            predicted_duration_s = excluded.predicted_duration_s,
            predicted_next_run = excluded.predicted_next_run,
            confidence = excluded.confidence,
            updated_at = excluded.updated_at
        """,
        (
            forecast["job_name"],
            forecast["predicted_duration_s"],
            forecast.get("predicted_next_run"),
            forecast["confidence"],
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()


def get_forecast(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT job_name, predicted_duration_s, predicted_next_run, confidence, updated_at "
        "FROM forecasts WHERE job_name = ?",
        (job_name.lower(),),
    ).fetchone()
    if not row:
        return None
    return dict(zip(["job_name", "predicted_duration_s", "predicted_next_run", "confidence", "updated_at"], row))


def list_forecasts(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, predicted_duration_s, predicted_next_run, confidence, updated_at "
        "FROM forecasts ORDER BY job_name"
    ).fetchall()
    keys = ["job_name", "predicted_duration_s", "predicted_next_run", "confidence", "updated_at"]
    return [dict(zip(keys, row)) for row in rows]
