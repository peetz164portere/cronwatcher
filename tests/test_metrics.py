"""Tests for cronwatcher/metrics.py"""
import sqlite3
import pytest
from datetime import datetime, timezone
from cronwatcher.storage import init_db
from cronwatcher.metrics import (
    init_metrics, compute_metrics, refresh_metrics, get_metrics, get_all_metrics
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_metrics(c)
    return c


def _insert(conn, job, status, duration):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO runs (job_name, status, duration, started_at, finished_at) VALUES (?,?,?,?,?)",
        (job, status, duration, now, now)
    )
    conn.commit()


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='job_metrics'"
    ).fetchone()
    assert row is not None


def test_compute_metrics_empty(conn):
    assert compute_metrics(conn, "missing_job") is None


def test_compute_metrics_basic(conn):
    for d in [10.0, 20.0, 30.0]:
        _insert(conn, "backup", "success", d)
    m = compute_metrics(conn, "backup")
    assert m["run_count"] == 3
    assert m["success_count"] == 3
    assert m["failure_count"] == 0
    assert m["max_duration"] == 30.0
    assert m["avg_duration"] == 20.0


def test_compute_metrics_mixed_status(conn):
    _insert(conn, "job", "success", 5.0)
    _insert(conn, "job", "failure", 1.0)
    _insert(conn, "job", "success", 9.0)
    m = compute_metrics(conn, "job")
    assert m["success_count"] == 2
    assert m["failure_count"] == 1
    assert m["run_count"] == 3


def test_compute_metrics_p95(conn):
    for i in range(1, 21):
        _insert(conn, "job", "success", float(i))
    m = compute_metrics(conn, "job")
    assert m["p95_duration"] is not None
    assert m["p95_duration"] >= m["p50_duration"]


def test_refresh_metrics_stores_result(conn):
    _insert(conn, "deploy", "success", 42.0)
    result = refresh_metrics(conn, "deploy")
    assert result is not None
    stored = get_metrics(conn, "deploy")
    assert stored is not None
    assert stored["job_name"] == "deploy"
    assert stored["max_duration"] == 42.0


def test_refresh_metrics_upserts(conn):
    _insert(conn, "deploy", "success", 10.0)
    refresh_metrics(conn, "deploy")
    _insert(conn, "deploy", "success", 50.0)
    refresh_metrics(conn, "deploy")
    stored = get_metrics(conn, "deploy")
    assert stored["run_count"] == 2
    assert stored["max_duration"] == 50.0


def test_get_metrics_missing(conn):
    assert get_metrics(conn, "ghost") is None


def test_get_all_metrics_empty(conn):
    assert get_all_metrics(conn) == []


def test_get_all_metrics_multiple(conn):
    for job in ["a", "b", "c"]:
        _insert(conn, job, "success", 1.0)
        refresh_metrics(conn, job)
    rows = get_all_metrics(conn)
    assert len(rows) == 3
    assert rows[0]["job_name"] == "a"
