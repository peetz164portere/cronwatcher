import sqlite3
import pytest
from cronwatcher.trends import (
    get_recent_durations,
    analyze_trend,
    analyze_all_trends,
    _linear_slope,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute(
        """
        CREATE TABLE runs (
            id INTEGER PRIMARY KEY,
            job_name TEXT,
            status TEXT,
            duration_seconds REAL,
            started_at TEXT
        )
        """
    )
    c.commit()
    return c


def _insert(conn, job, status, duration, ts="2024-01-01T00:00:00"):
    conn.execute(
        "INSERT INTO runs (job_name, status, duration_seconds, started_at) VALUES (?,?,?,?)",
        (job, status, duration, ts),
    )
    conn.commit()


def test_get_recent_durations_empty(conn):
    assert get_recent_durations(conn, "nojob") == []


def test_get_recent_durations_ignores_failures(conn):
    _insert(conn, "job", "failure", 99.0)
    assert get_recent_durations(conn, "job") == []


def test_get_recent_durations_returns_successes(conn):
    for d in [1.0, 2.0, 3.0]:
        _insert(conn, "job", "success", d)
    result = get_recent_durations(conn, "job")
    assert len(result) == 3
    assert result == sorted(result)  # chronological order


def test_linear_slope_increasing():
    slope = _linear_slope([1.0, 2.0, 3.0, 4.0])
    assert slope > 0


def test_linear_slope_flat():
    slope = _linear_slope([5.0, 5.0, 5.0, 5.0])
    assert slope == 0.0


def test_linear_slope_single_value():
    assert _linear_slope([42.0]) == 0.0


def test_analyze_trend_insufficient_data(conn):
    result = analyze_trend(conn, "job")
    assert result["trend"] == "insufficient_data"
    assert result["slope"] is None


def test_analyze_trend_degrading(conn):
    for d in [1.0, 5.0, 10.0, 20.0, 40.0]:
        _insert(conn, "job", "success", d)
    result = analyze_trend(conn, "job")
    assert result["trend"] == "degrading"
    assert result["slope"] > 0


def test_analyze_trend_stable(conn):
    for d in [10.0, 10.1, 9.9, 10.0, 10.05]:
        _insert(conn, "job", "success", d)
    result = analyze_trend(conn, "job")
    assert result["trend"] == "stable"


def test_analyze_all_trends_multiple_jobs(conn):
    for d in [1.0, 2.0, 3.0]:
        _insert(conn, "alpha", "success", d)
    for d in [10.0, 10.0, 10.0]:
        _insert(conn, "beta", "success", d)
    results = analyze_all_trends(conn)
    jobs = {r["job"] for r in results}
    assert "alpha" in jobs
    assert "beta" in jobs
