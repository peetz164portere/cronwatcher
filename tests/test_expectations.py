"""Tests for cronwatcher/expectations.py"""
import sqlite3
import pytest
from cronwatcher.expectations import (
    init_expectations, set_expectation, get_expectation,
    remove_expectation, list_expectations, check_expectation,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_expectations(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    assert ("expectations",) in tables


def test_get_expectation_missing_returns_none(conn):
    assert get_expectation(conn, "backup") is None


def test_set_and_get_expectation(conn):
    set_expectation(conn, "backup", min_duration=10.0, max_duration=120.0)
    exp = get_expectation(conn, "backup")
    assert exp is not None
    assert exp["min_duration"] == 10.0
    assert exp["max_duration"] == 120.0
    assert exp["max_interval_seconds"] is None


def test_set_expectation_normalizes_case(conn):
    set_expectation(conn, "BACKUP", max_duration=60.0)
    exp = get_expectation(conn, "backup")
    assert exp is not None
    assert exp["job_name"] == "backup"


def test_set_expectation_upserts(conn):
    set_expectation(conn, "sync", max_duration=30.0)
    set_expectation(conn, "sync", max_duration=60.0, max_interval_seconds=3600)
    exp = get_expectation(conn, "sync")
    assert exp["max_duration"] == 60.0
    assert exp["max_interval_seconds"] == 3600


def test_remove_expectation_returns_true(conn):
    set_expectation(conn, "cleanup", max_duration=45.0)
    assert remove_expectation(conn, "cleanup") is True
    assert get_expectation(conn, "cleanup") is None


def test_remove_expectation_missing_returns_false(conn):
    assert remove_expectation(conn, "ghost") is False


def test_list_expectations_empty(conn):
    assert list_expectations(conn) == []


def test_list_expectations_returns_all(conn):
    set_expectation(conn, "jobA", max_duration=10.0)
    set_expectation(conn, "jobB", min_duration=5.0)
    rows = list_expectations(conn)
    assert len(rows) == 2
    names = [r["job_name"] for r in rows]
    assert "joba" in names
    assert "jobb" in names


def test_check_expectation_no_violations(conn):
    set_expectation(conn, "report", min_duration=5.0, max_duration=60.0)
    assert check_expectation(conn, "report", 30.0) == []


def test_check_expectation_too_fast(conn):
    set_expectation(conn, "report", min_duration=10.0)
    violations = check_expectation(conn, "report", 2.0)
    assert len(violations) == 1
    assert "below minimum" in violations[0]


def test_check_expectation_too_slow(conn):
    set_expectation(conn, "report", max_duration=30.0)
    violations = check_expectation(conn, "report", 90.5)
    assert len(violations) == 1
    assert "exceeds maximum" in violations[0]


def test_check_expectation_both_violations(conn):
    set_expectation(conn, "weird", min_duration=50.0, max_duration=10.0)
    violations = check_expectation(conn, "weird", 25.0)
    assert len(violations) == 2


def test_check_expectation_no_config_returns_empty(conn):
    assert check_expectation(conn, "unknown", 99.9) == []
