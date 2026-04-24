"""Tests for cronwatcher/retention.py"""

import sqlite3
import pytest
from datetime import datetime, timedelta
from cronwatcher.storage import init_db
from cronwatcher.retention import (
    init_retention, set_retention, get_retention,
    remove_retention, list_retention, apply_retention
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_retention(c)
    return c


def _insert_run(conn, job_name, started_at, status="success", duration=1.0):
    conn.execute(
        "INSERT INTO runs (job_name, started_at, finished_at, status, duration) VALUES (?, ?, ?, ?, ?)",
        (job_name, started_at, started_at, status, duration)
    )
    conn.commit()


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "retention_policies" in tables


def test_set_and_get_retention(conn):
    set_retention(conn, "backup", 30)
    policy = get_retention(conn, "backup")
    assert policy is not None
    assert policy["job_name"] == "backup"
    assert policy["max_days"] == 30
    assert policy["max_runs"] is None


def test_set_retention_normalizes_case(conn):
    set_retention(conn, "BACKUP", 7)
    assert get_retention(conn, "backup") is not None


def test_set_retention_with_max_runs(conn):
    set_retention(conn, "sync", 14, max_runs=100)
    policy = get_retention(conn, "sync")
    assert policy["max_runs"] == 100


def test_set_retention_upserts(conn):
    set_retention(conn, "job", 10)
    set_retention(conn, "job", 20, max_runs=50)
    policy = get_retention(conn, "job")
    assert policy["max_days"] == 20
    assert policy["max_runs"] == 50


def test_get_retention_missing_returns_none(conn):
    assert get_retention(conn, "nonexistent") is None


def test_set_retention_invalid_days_raises(conn):
    with pytest.raises(ValueError):
        set_retention(conn, "job", 0)


def test_set_retention_invalid_max_runs_raises(conn):
    with pytest.raises(ValueError):
        set_retention(conn, "job", 7, max_runs=0)


def test_remove_retention_returns_true(conn):
    set_retention(conn, "job", 30)
    assert remove_retention(conn, "job") is True
    assert get_retention(conn, "job") is None


def test_remove_retention_missing_returns_false(conn):
    assert remove_retention(conn, "ghost") is False


def test_list_retention_empty(conn):
    assert list_retention(conn) == []


def test_list_retention_returns_all(conn):
    set_retention(conn, "a", 7)
    set_retention(conn, "b", 14, max_runs=50)
    policies = list_retention(conn)
    assert len(policies) == 2
    names = [p["job_name"] for p in policies]
    assert "a" in names and "b" in names


def test_apply_retention_removes_old_runs(conn):
    old = (datetime.utcnow() - timedelta(days=40)).isoformat()
    recent = (datetime.utcnow() - timedelta(days=1)).isoformat()
    _insert_run(conn, "backup", old)
    _insert_run(conn, "backup", recent)
    set_retention(conn, "backup", 30)
    deleted = apply_retention(conn, "backup")
    assert deleted >= 1
    remaining = conn.execute("SELECT COUNT(*) FROM runs WHERE job_name='backup'").fetchone()[0]
    assert remaining == 1


def test_apply_retention_no_policy_returns_zero(conn):
    _insert_run(conn, "orphan", datetime.utcnow().isoformat())
    assert apply_retention(conn, "orphan") == 0


def test_apply_retention_respects_max_runs(conn):
    base = datetime.utcnow()
    for i in range(10):
        _insert_run(conn, "job", (base - timedelta(hours=i)).isoformat())
    set_retention(conn, "job", 365, max_runs=3)
    apply_retention(conn, "job")
    remaining = conn.execute("SELECT COUNT(*) FROM runs WHERE job_name='job'").fetchone()[0]
    assert remaining == 3
