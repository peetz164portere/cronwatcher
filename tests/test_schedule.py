"""Tests for cronwatcher.schedule."""

from datetime import datetime, timezone, timedelta
import pytest
from cronwatcher.storage import get_connection, init_db, record_start, record_finish
from cronwatcher.schedule import get_last_success, is_overdue, check_schedule


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "test.db"
    c = get_connection(str(db))
    init_db(c)
    yield c
    c.close()


def _add_run(conn, job_name, status):
    run_id = record_start(conn, job_name, "echo hi")
    record_finish(conn, run_id, 0 if status == "success" else 1, "output")
    return run_id


def test_get_last_success_none_when_empty(conn):
    assert get_last_success(conn, "myjob") is None


def test_get_last_success_ignores_failures(conn):
    _add_run(conn, "myjob", "failure")
    assert get_last_success(conn, "myjob") is None


def test_get_last_success_returns_datetime(conn):
    _add_run(conn, "myjob", "success")
    result = get_last_success(conn, "myjob")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_is_overdue_true_when_none():
    assert is_overdue(None, 3600) is True


def test_is_overdue_false_when_recent():
    recent = datetime.now(timezone.utc) - timedelta(seconds=60)
    assert is_overdue(recent, 3600) is False


def test_is_overdue_true_when_old():
    old = datetime.now(timezone.utc) - timedelta(seconds=7200)
    assert is_overdue(old, 3600) is True


def test_check_schedule_overdue_no_runs(conn):
    result = check_schedule(conn, "myjob", 3600)
    assert result["overdue"] is True
    assert result["last_success"] is None
    assert result["job_name"] == "myjob"


def test_check_schedule_not_overdue(conn):
    _add_run(conn, "myjob", "success")
    result = check_schedule(conn, "myjob", 3600)
    assert result["overdue"] is False
    assert result["last_success"] is not None
