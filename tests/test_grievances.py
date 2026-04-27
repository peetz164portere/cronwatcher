"""Tests for cronwatcher/grievances.py"""

import sqlite3
import pytest
from cronwatcher.grievances import (
    init_grievances, record_failure, resolve_grievance,
    get_grievance, list_grievances
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_grievances(c)
    return c


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [t[0] for t in tables]
    assert "grievances" in names


def test_record_failure_returns_id(conn):
    gid = record_failure(conn, "backup-job")
    assert isinstance(gid, int)
    assert gid > 0


def test_record_failure_increments_count(conn):
    record_failure(conn, "backup-job")
    record_failure(conn, "backup-job")
    record_failure(conn, "backup-job")
    g = get_grievance(conn, "backup-job")
    assert g["failure_count"] == 3


def test_record_failure_normalizes_case(conn):
    record_failure(conn, "BackupJob")
    record_failure(conn, "backupjob")
    g = get_grievance(conn, "backupjob")
    assert g["failure_count"] == 2


def test_get_grievance_none_when_empty(conn):
    assert get_grievance(conn, "nonexistent") is None


def test_get_grievance_returns_dict(conn):
    record_failure(conn, "myjob")
    g = get_grievance(conn, "myjob")
    assert g["job_name"] == "myjob"
    assert g["failure_count"] == 1
    assert "first_seen" in g
    assert "last_seen" in g


def test_resolve_grievance_returns_true(conn):
    record_failure(conn, "myjob")
    result = resolve_grievance(conn, "myjob")
    assert result is True


def test_resolve_grievance_clears_active(conn):
    record_failure(conn, "myjob")
    resolve_grievance(conn, "myjob")
    assert get_grievance(conn, "myjob") is None


def test_resolve_nonexistent_returns_false(conn):
    result = resolve_grievance(conn, "ghost")
    assert result is False


def test_list_grievances_active_only(conn):
    record_failure(conn, "job-a")
    record_failure(conn, "job-b")
    resolve_grievance(conn, "job-a")
    items = list_grievances(conn)
    assert len(items) == 1
    assert items[0]["job_name"] == "job-b"


def test_list_grievances_include_resolved(conn):
    record_failure(conn, "job-a")
    record_failure(conn, "job-b")
    resolve_grievance(conn, "job-a")
    items = list_grievances(conn, include_resolved=True)
    assert len(items) == 2


def test_list_grievances_sorted_by_failure_count(conn):
    record_failure(conn, "job-a")
    for _ in range(5):
        record_failure(conn, "job-b")
    items = list_grievances(conn)
    assert items[0]["job_name"] == "job-b"
    assert items[0]["failure_count"] == 5


def test_new_grievance_after_resolve(conn):
    record_failure(conn, "myjob")
    resolve_grievance(conn, "myjob")
    record_failure(conn, "myjob")
    g = get_grievance(conn, "myjob")
    assert g["failure_count"] == 1
