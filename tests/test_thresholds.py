"""Tests for cronwatcher/thresholds.py"""

import sqlite3
import pytest
from cronwatcher.thresholds import (
    init_thresholds, set_threshold, get_threshold,
    remove_threshold, record_streak, get_streak, is_threshold_exceeded,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_thresholds(c)
    yield c
    c.close()


def test_init_creates_tables(conn):
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "thresholds" in tables
    assert "failure_streaks" in tables


def test_set_and_get_threshold(conn):
    set_threshold(conn, "backup", 5)
    assert get_threshold(conn, "backup") == 5


def test_set_threshold_normalizes_case(conn):
    set_threshold(conn, "BackupJob", 3)
    assert get_threshold(conn, "backupjob") == 3


def test_get_threshold_missing_returns_none(conn):
    assert get_threshold(conn, "nonexistent") is None


def test_set_threshold_invalid_raises(conn):
    with pytest.raises(ValueError):
        set_threshold(conn, "job", 0)


def test_set_threshold_overwrites(conn):
    set_threshold(conn, "myjob", 2)
    set_threshold(conn, "myjob", 7)
    assert get_threshold(conn, "myjob") == 7


def test_remove_threshold(conn):
    set_threshold(conn, "job1", 3)
    remove_threshold(conn, "job1")
    assert get_threshold(conn, "job1") is None


def test_record_streak_increments_on_failure(conn):
    assert record_streak(conn, "job", failed=True) == 1
    assert record_streak(conn, "job", failed=True) == 2
    assert record_streak(conn, "job", failed=True) == 3


def test_record_streak_resets_on_success(conn):
    record_streak(conn, "job", failed=True)
    record_streak(conn, "job", failed=True)
    result = record_streak(conn, "job", failed=False)
    assert result == 0
    assert get_streak(conn, "job") == 0


def test_get_streak_zero_when_no_history(conn):
    assert get_streak(conn, "unknown") == 0


def test_is_threshold_exceeded_false_when_no_threshold(conn):
    record_streak(conn, "job", failed=True)
    record_streak(conn, "job", failed=True)
    assert is_threshold_exceeded(conn, "job") is False


def test_is_threshold_exceeded_false_below_limit(conn):
    set_threshold(conn, "job", 3)
    record_streak(conn, "job", failed=True)
    record_streak(conn, "job", failed=True)
    assert is_threshold_exceeded(conn, "job") is False


def test_is_threshold_exceeded_true_at_limit(conn):
    set_threshold(conn, "job", 3)
    for _ in range(3):
        record_streak(conn, "job", failed=True)
    assert is_threshold_exceeded(conn, "job") is True


def test_is_threshold_exceeded_resets_after_success(conn):
    set_threshold(conn, "job", 2)
    record_streak(conn, "job", failed=True)
    record_streak(conn, "job", failed=True)
    assert is_threshold_exceeded(conn, "job") is True
    record_streak(conn, "job", failed=False)
    assert is_threshold_exceeded(conn, "job") is False
