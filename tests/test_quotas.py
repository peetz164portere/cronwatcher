"""Tests for cronwatcher/quotas.py"""

import sqlite3
import pytest
from datetime import datetime, timedelta
from cronwatcher.quotas import (
    init_quotas, set_quota, get_quota, remove_quota,
    list_quotas, count_recent_runs, is_quota_exceeded
)
from cronwatcher.storage import init_db


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_quotas(c)
    return c


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "quotas" in tables


def test_set_and_get_quota(conn):
    set_quota(conn, "backup", 5, 3600)
    q = get_quota(conn, "backup")
    assert q["max_runs"] == 5
    assert q["window_seconds"] == 3600


def test_set_quota_normalizes_case(conn):
    set_quota(conn, "BackupJob", 3, 600)
    q = get_quota(conn, "backupjob")
    assert q is not None


def test_get_quota_missing_returns_none(conn):
    assert get_quota(conn, "nonexistent") is None


def test_set_quota_upserts(conn):
    set_quota(conn, "myjob", 2, 60)
    set_quota(conn, "myjob", 10, 120)
    q = get_quota(conn, "myjob")
    assert q["max_runs"] == 10
    assert q["window_seconds"] == 120


def test_remove_quota(conn):
    set_quota(conn, "myjob", 5, 300)
    remove_quota(conn, "myjob")
    assert get_quota(conn, "myjob") is None


def test_list_quotas_empty(conn):
    assert list_quotas(conn) == []


def test_list_quotas_returns_all(conn):
    set_quota(conn, "job_a", 1, 60)
    set_quota(conn, "job_b", 2, 120)
    result = list_quotas(conn)
    assert len(result) == 2


def _insert_run(conn, job_name, started_at):
    conn.execute(
        "INSERT INTO runs (job_name, started_at, status) VALUES (?, ?, 'success')",
        (job_name, started_at)
    )
    conn.commit()


def test_count_recent_runs(conn):
    now = datetime.utcnow()
    _insert_run(conn, "myjob", (now - timedelta(seconds=30)).isoformat())
    _insert_run(conn, "myjob", (now - timedelta(seconds=7200)).isoformat())
    count = count_recent_runs(conn, "myjob", 3600)
    assert count == 1


def test_is_quota_exceeded_false(conn):
    set_quota(conn, "myjob", 5, 3600)
    assert not is_quota_exceeded(conn, "myjob")


def test_is_quota_exceeded_true(conn):
    set_quota(conn, "myjob", 2, 3600)
    now = datetime.utcnow()
    _insert_run(conn, "myjob", (now - timedelta(seconds=10)).isoformat())
    _insert_run(conn, "myjob", (now - timedelta(seconds=20)).isoformat())
    assert is_quota_exceeded(conn, "myjob")


def test_is_quota_exceeded_no_quota(conn):
    assert not is_quota_exceeded(conn, "unknown_job")
