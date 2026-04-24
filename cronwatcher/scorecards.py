"""Scorecards: compute a reliability score (0-100) for each job."""

import sqlite3
from typing import Optional


def init_scorecards(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scorecards (
            job_name  TEXT PRIMARY KEY,
            score     REAL NOT NULL,
            runs      INTEGER NOT NULL,
            failures  INTEGER NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def compute_score(runs: int, failures: int) -> float:
    """Return a 0-100 reliability score."""
    if runs == 0:
        return 100.0
    success_rate = (runs - failures) / runs
    return round(success_rate * 100, 2)


def refresh_scorecard(conn: sqlite3.Connection, job_name: str) -> float:
    """Recompute and persist the score for *job_name* from run history."""
    job_name = job_name.lower()
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS runs,
            SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) AS failures
        FROM runs
        WHERE job_name = ?
        """,
        (job_name,),
    ).fetchone()
    runs = row[0] or 0
    failures = row[1] or 0
    score = compute_score(runs, failures)
    conn.execute(
        """
        INSERT INTO scorecards (job_name, score, runs, failures, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            score = excluded.score,
            runs = excluded.runs,
            failures = excluded.failures,
            updated_at = excluded.updated_at
        """,
        (job_name, score, runs, failures),
    )
    conn.commit()
    return score


def get_scorecard(conn: sqlite3.Connection, job_name: str) -> Optional[dict]:
    job_name = job_name.lower()
    row = conn.execute(
        "SELECT job_name, score, runs, failures, updated_at FROM scorecards WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if row is None:
        return None
    return {"job_name": row[0], "score": row[1], "runs": row[2], "failures": row[3], "updated_at": row[4]}


def list_scorecards(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT job_name, score, runs, failures, updated_at FROM scorecards ORDER BY score ASC"
    ).fetchall()
    return [
        {"job_name": r[0], "score": r[1], "runs": r[2], "failures": r[3], "updated_at": r[4]}
        for r in rows
    ]
