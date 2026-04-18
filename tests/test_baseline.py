"""Tests for cronwatcher.baseline."""

import sqlite3
import pytest
from cronwatcher.storage import init_db
from cronwatcher.baseline import init_baseline, update_baseline, get_baseline, is_slow


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_baseline(c)
    return c


def _insert_run(conn, job, status, duration):
    conn.execute(
        "INSERT INTO runs (job_name, status, duration, started_at, finished_at) VALUES (?, ?, ?, datetime('now'), datetime('now'))",
        (job, status, duration),
    )
    conn.commit()


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "baselines" in tables


def test_update_baseline_no_runs(conn):
    result = update_baseline(conn, "myjob")
    assert result is None


def test_update_baseline_ignores_failures(conn):
    _insert_run(conn, "myjob", "failure", 99.0)
    result = update_baseline(conn, "myjob")
    assert result is None


def test_update_baseline_calculates_avg(conn):
    _insert_run(conn, "myjob", "success", 10.0)
    _insert_run(conn, "myjob", "success", 20.0)
    avg = update_baseline(conn, "myjob")
    assert avg == pytest.approx(15.0)


def test_get_baseline_returns_dict(conn):
    _insert_run(conn, "myjob", "success", 30.0)
    update_baseline(conn, "myjob")
    b = get_baseline(conn, "myjob")
    assert b is not None
    assert b["job_name"] == "myjob"
    assert b["avg_duration"] == pytest.approx(30.0)
    assert b["sample_count"] == 1


def test_get_baseline_none_when_missing(conn):
    assert get_baseline(conn, "ghost") is None


def test_update_baseline_overwrites(conn):
    _insert_run(conn, "myjob.0)
    update_baseline(conn, "myjob")
    _insert_run(conn, "myjob", "success", 30.0)
    avg = update_baseline(conn, "myjob")
    assert avg == pytest.approx(20.0)
    b = get_baseline(conn, "myjob")
    assert b["sample_count"] == 2


def test_is_slow_true(conn):
    _insert_run(conn, "myjob", "success", 10.0)
    update_baseline(conn, "myjob")
    assert is_slow(conn, "myjob", 25.0, threshold=2.0) is True


def test_is_slow_false(conn):
    _insert_run(conn, "myjob", "success", 10.0)
    update_baseline(conn, "myjob")
    assert is_slow(conn, "myjob", 15.0, threshold=2.0) is False


def test_is_slow_no_baseline(conn):
    assert is_slow(conn, "ghost", 999.0) is False
