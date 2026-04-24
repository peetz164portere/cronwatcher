"""Tests for cronwatcher/streaks.py"""

import sqlite3
import pytest
from cronwatcher.streaks import init_streaks, update_streak, get_streak, list_streaks, reset_streak


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_streaks(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "streaks" in tables


def test_get_streak_none_when_empty(conn):
    assert get_streak(conn, "backup") is None


def test_update_streak_first_success(conn):
    result = update_streak(conn, "backup", "success")
    assert result["current_streak"] == 1
    assert result["streak_type"] == "success"
    assert result["longest_success"] == 1
    assert result["longest_failure"] == 0


def test_update_streak_first_failure(conn):
    result = update_streak(conn, "backup", "failure")
    assert result["current_streak"] == 1
    assert result["streak_type"] == "failure"
    assert result["longest_failure"] == 1
    assert result["longest_success"] == 0


def test_update_streak_consecutive_successes(conn):
    update_streak(conn, "backup", "success")
    update_streak(conn, "backup", "success")
    result = update_streak(conn, "backup", "success")
    assert result["current_streak"] == 3
    assert result["longest_success"] == 3


def test_update_streak_resets_on_type_change(conn):
    update_streak(conn, "backup", "success")
    update_streak(conn, "backup", "success")
    result = update_streak(conn, "backup", "failure")
    assert result["current_streak"] == 1
    assert result["streak_type"] == "failure"
    assert result["longest_success"] == 2


def test_update_streak_longest_preserved(conn):
    update_streak(conn, "backup", "success")
    update_streak(conn, "backup", "success")
    update_streak(conn, "backup", "success")
    update_streak(conn, "backup", "failure")
    result = update_streak(conn, "backup", "success")
    assert result["current_streak"] == 1
    assert result["longest_success"] == 3


def test_update_streak_normalizes_case(conn):
    update_streak(conn, "Backup", "success")
    result = get_streak(conn, "backup")
    assert result is not None
    assert result["job_name"] == "backup"


def test_get_streak_returns_dict(conn):
    update_streak(conn, "cleanup", "success")
    result = get_streak(conn, "cleanup")
    assert isinstance(result, dict)
    assert "current_streak" in result
    assert "streak_type" in result
    assert "updated_at" in result


def test_list_streaks_empty(conn):
    assert list_streaks(conn) == []


def test_list_streaks_returns_all(conn):
    update_streak(conn, "job_a", "success")
    update_streak(conn, "job_b", "failure")
    results = list_streaks(conn)
    assert len(results) == 2
    names = [r["job_name"] for r in results]
    assert "job_a" in names
    assert "job_b" in names


def test_reset_streak_returns_true(conn):
    update_streak(conn, "cleanup", "success")
    assert reset_streak(conn, "cleanup") is True
    assert get_streak(conn, "cleanup") is None


def test_reset_streak_missing_returns_false(conn):
    assert reset_streak(conn, "nonexistent") is False
