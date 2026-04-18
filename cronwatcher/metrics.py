"""Collect and store per-job runtime metrics (p50/p95/max duration, success rate)."""
import sqlite3
from typing import Optional
from cronwatcher.storage import get_connection


def init_metrics(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_metrics (
            job_name TEXT PRIMARY KEY,
            run_count INTEGER NOT NULL DEFAULT 0,
            success_count INTEGER NOT NULL DEFAULT 0,
            failure_count INTEGER NOT NULL DEFAULT 0,
            avg_duration REAL,
            p50_duration REAL,
            p95_duration REAL,
            max_duration REAL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def compute_metrics(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    rows = conn.execute("""
        SELECT status, duration FROM runs
        WHERE job_name = ? AND duration IS NOT NULL
        ORDER BY started_at DESC LIMIT 200
    """, (job_name,)).fetchall()

    if not rows:
        return None

    total = len(rows)
    successes = [r[1] for r in rows if r[0] == "success"]
    failures = [r for r in rows if r[0] == "failure"]
    durations = sorted([r[1] for r in rows])

    def percentile(data, pct):
        if not data:
            return None
        idx = int(len(data) * pct / 100)
        return data[min(idx, len(data) - 1)]

    return {
        "job_name": job_name,
        "run_count": total,
        "success_count": len(successes),
        "failure_count": len(failures),
        "avg_duration": round(sum(durations) / len(durations), 3) if durations else None,
        "p50_duration": percentile(durations, 50),
        "p95_duration": percentile(durations, 95),
        "max_duration": max(durations) if durations else None,
    }


def refresh_metrics(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    from datetime import datetime, timezone
    m = compute_metrics(conn, job_name)
    if m is None:
        return None
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO job_metrics
            (job_name, run_count, success_count, failure_count,
             avg_duration, p50_duration, p95_duration, max_duration, updated_at)
        VALUES (:job_name, :run_count, :success_count, :failure_count,
                :avg_duration, :p50_duration, :p95_duration, :max_duration, ?)
        ON CONFLICT(job_name) DO UPDATE SET
            run_count=excluded.run_count, success_count=excluded.success_count,
            failure_count=excluded.failure_count, avg_duration=excluded.avg_duration,
            p50_duration=excluded.p50_duration, p95_duration=excluded.p95_duration,
            max_duration=excluded.max_duration, updated_at=excluded.updated_at
    """, {**m, **{}} | {"job_name": m["job_name"]}, (now,))
    conn.commit()
    return m


def get_metrics(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM job_metrics WHERE job_name = ?", (job_name,)
    ).fetchone()
    if row is None:
        return None
    keys = ["job_name", "run_count", "success_count", "failure_count",
            "avg_duration", "p50_duration", "p95_duration", "max_duration", "updated_at"]
    return dict(zip(keys, row))


def get_all_metrics(conn: sqlite3.Connection) -> list:
    rows = conn.execute("SELECT * FROM job_metrics ORDER BY job_name").fetchall()
    keys = ["job_name", "run_count", "success_count", "failure_count",
            "avg_duration", "p50_duration", "p95_duration", "max_duration", "updated_at"]
    return [dict(zip(keys, r)) for r in rows]
