import sqlite3
import pytest
from cronwatcher.timeouts import (
    init_timeouts,
    set_timeout,
    get_timeout,
    remove_timeout,
    list_timeouts,
    is_timed_out,
    DEFAULT_TIMEOUT,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_timeouts(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='timeouts'"
    ).fetchall()
    assert len(tables) == 1


def test_set_and_get_timeout(conn):
    set_timeout(conn, "backup", 600)
    result = get_timeout(conn, "backup")
    assert result is not None
    assert result["job_name"] == "backup"
    assert result["timeout_seconds"] == 600
    assert result["action"] == "alert"


def test_set_timeout_normalizes_case(conn):
    set_timeout(conn, "BackupJob", 300)
    result = get_timeout(conn, "backupjob")
    assert result is not None
    assert result["job_name"] == "backupjob"


def test_get_timeout_missing_returns_none(conn):
    assert get_timeout(conn, "nonexistent") is None


def test_set_timeout_invalid_action_raises(conn):
    with pytest.raises(ValueError, match="action must be one of"):
        set_timeout(conn, "myjob", 60, action="explode")


def test_set_timeout_invalid_seconds_raises(conn):
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        set_timeout(conn, "myjob", 0)


def test_set_timeout_updates_existing(conn):
    set_timeout(conn, "myjob", 100, action="alert")
    set_timeout(conn, "myjob", 200, action="kill")
    result = get_timeout(conn, "myjob")
    assert result["timeout_seconds"] == 200
    assert result["action"] == "kill"


def test_remove_timeout_returns_true(conn):
    set_timeout(conn, "myjob", 60)
    assert remove_timeout(conn, "myjob") is True
    assert get_timeout(conn, "myjob") is None


def test_remove_timeout_missing_returns_false(conn):
    assert remove_timeout(conn, "ghost") is False


def test_list_timeouts_empty(conn):
    assert list_timeouts(conn) == []


def test_list_timeouts_returns_all(conn):
    set_timeout(conn, "job_a", 60)
    set_timeout(conn, "job_b", 120, action="kill")
    results = list_timeouts(conn)
    assert len(results) == 2
    names = [r["job_name"] for r in results]
    assert "job_a" in names
    assert "job_b" in names


def test_is_timed_out_with_custom_threshold(conn):
    set_timeout(conn, "fastjob", 30)
    assert is_timed_out(conn, "fastjob", 31) is True
    assert is_timed_out(conn, "fastjob", 29) is False


def test_is_timed_out_uses_default_when_no_record(conn):
    assert is_timed_out(conn, "unknownjob", DEFAULT_TIMEOUT + 1) is True
    assert is_timed_out(conn, "unknownjob", DEFAULT_TIMEOUT - 1) is False
