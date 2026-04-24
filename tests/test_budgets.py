"""Tests for cronwatcher/budgets.py"""

import sqlite3
import pytest
from cronwatcher import budgets as bmod


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    bmod.init_budgets(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='budgets'"
    ).fetchall()
    assert len(tables) == 1


def test_set_and_get_budget(conn):
    bmod.set_budget(conn, "backup", 120.0, "warn")
    b = bmod.get_budget(conn, "backup")
    assert b is not None
    assert b["job_name"] == "backup"
    assert b["max_seconds"] == 120.0
    assert b["action"] == "warn"


def test_set_budget_normalizes_case(conn):
    bmod.set_budget(conn, "BackupJob", 60.0)
    b = bmod.get_budget(conn, "backupjob")
    assert b is not None
    assert b["job_name"] == "backupjob"


def test_get_budget_missing_returns_none(conn):
    assert bmod.get_budget(conn, "nonexistent") is None


def test_set_budget_invalid_action_raises(conn):
    with pytest.raises(ValueError, match="Invalid action"):
        bmod.set_budget(conn, "job", 30.0, action="explode")


def test_set_budget_zero_seconds_raises(conn):
    with pytest.raises(ValueError, match="max_seconds must be positive"):
        bmod.set_budget(conn, "job", 0)


def test_set_budget_negative_raises(conn):
    with pytest.raises(ValueError):
        bmod.set_budget(conn, "job", -10.0)


def test_set_budget_upserts(conn):
    bmod.set_budget(conn, "job", 60.0, "warn")
    bmod.set_budget(conn, "job", 90.0, "alert")
    b = bmod.get_budget(conn, "job")
    assert b["max_seconds"] == 90.0
    assert b["action"] == "alert"


def test_remove_budget_returns_true(conn):
    bmod.set_budget(conn, "job", 60.0)
    assert bmod.remove_budget(conn, "job") is True
    assert bmod.get_budget(conn, "job") is None


def test_remove_budget_missing_returns_false(conn):
    assert bmod.remove_budget(conn, "ghost") is False


def test_list_budgets_empty(conn):
    assert bmod.list_budgets(conn) == []


def test_list_budgets_returns_all(conn):
    bmod.set_budget(conn, "alpha", 30.0)
    bmod.set_budget(conn, "beta", 90.0, "alert")
    rows = bmod.list_budgets(conn)
    assert len(rows) == 2
    names = [r["job_name"] for r in rows]
    assert "alpha" in names
    assert "beta" in names


def test_is_over_budget_true(conn):
    bmod.set_budget(conn, "slow_job", 10.0, "warn")
    result = bmod.is_over_budget(conn, "slow_job", 15.0)
    assert result is not None
    assert result["action"] == "warn"


def test_is_over_budget_false(conn):
    bmod.set_budget(conn, "fast_job", 60.0)
    assert bmod.is_over_budget(conn, "fast_job", 30.0) is None


def test_is_over_budget_no_budget_returns_none(conn):
    assert bmod.is_over_budget(conn, "unknown", 999.0) is None
