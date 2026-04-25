"""Tests for cronwatcher.capacity module."""

import sqlite3
import pytest
from cronwatcher.capacity import (
    init_capacity,
    set_capacity,
    get_capacity,
    remove_capacity,
    list_capacity,
    is_at_capacity,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    # minimal runs table to support count_running
    c.execute(
        """
        CREATE TABLE runs (
            id INTEGER PRIMARY KEY,
            job_name TEXT,
            status TEXT
        )
        """
    )
    init_capacity(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    ]
    assert "capacity_limits" in tables


def test_get_capacity_default_is_one(conn):
    assert get_capacity(conn, "backup") == 1


def test_set_and_get_capacity(conn):
    set_capacity(conn, "backup", 3)
    assert get_capacity(conn, "backup") == 3


def test_set_capacity_normalizes_case(conn):
    set_capacity(conn, "BACKUP", 2)
    assert get_capacity(conn, "backup") == 2


def test_set_capacity_invalid_raises(conn):
    with pytest.raises(ValueError):
        set_capacity(conn, "backup", 0)


def test_set_capacity_overwrite(conn):
    set_capacity(conn, "sync", 2)
    set_capacity(conn, "sync", 5)
    assert get_capacity(conn, "sync") == 5


def test_remove_capacity_returns_true(conn):
    set_capacity(conn, "cleanup", 2)
    assert remove_capacity(conn, "cleanup") is True
    assert get_capacity(conn, "cleanup") == 1  # back to default


def test_remove_capacity_missing_returns_false(conn):
    assert remove_capacity(conn, "nonexistent") is False


def test_list_capacity_empty(conn):
    assert list_capacity(conn) == []


def test_list_capacity_returns_all(conn):
    set_capacity(conn, "alpha", 2)
    set_capacity(conn, "beta", 4)
    result = list_capacity(conn)
    names = [r["job_name"] for r in result]
    assert "alpha" in names
    assert "beta" in names
    assert len(result) == 2


def test_is_at_capacity_false_when_no_runs(conn):
    set_capacity(conn, "report", 2)
    assert is_at_capacity(conn, "report") is False


def test_is_at_capacity_true_when_limit_reached(conn):
    set_capacity(conn, "report", 1)
    conn.execute("INSERT INTO runs (job_name, status) VALUES ('report', 'running')")
    conn.commit()
    assert is_at_capacity(conn, "report") is True


def test_is_at_capacity_false_below_limit(conn):
    set_capacity(conn, "report", 3)
    conn.execute("INSERT INTO runs (job_name, status) VALUES ('report', 'running')")
    conn.commit()
    assert is_at_capacity(conn, "report") is False
