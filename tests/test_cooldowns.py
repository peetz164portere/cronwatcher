"""Tests for cronwatcher/cooldowns.py"""

import sqlite3
from datetime import datetime, timedelta

import pytest

from cronwatcher.cooldowns import (
    init_cooldowns,
    set_cooldown,
    get_cooldown,
    remove_cooldown,
    list_cooldowns,
    is_in_cooldown,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_cooldowns(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='cooldowns'"
    ).fetchone()
    assert tables is not None


def test_set_and_get_cooldown(conn):
    set_cooldown(conn, "backup", 3600)
    assert get_cooldown(conn, "backup") == 3600


def test_set_cooldown_normalizes_case(conn):
    set_cooldown(conn, "BackupJob", 300)
    assert get_cooldown(conn, "backupjob") == 300


def test_get_cooldown_missing_returns_none(conn):
    assert get_cooldown(conn, "nonexistent") is None


def test_set_cooldown_overwrites(conn):
    set_cooldown(conn, "myjob", 60)
    set_cooldown(conn, "myjob", 120)
    assert get_cooldown(conn, "myjob") == 120


def test_remove_cooldown_returns_true(conn):
    set_cooldown(conn, "myjob", 60)
    assert remove_cooldown(conn, "myjob") is True
    assert get_cooldown(conn, "myjob") is None


def test_remove_cooldown_missing_returns_false(conn):
    assert remove_cooldown(conn, "ghost") is False


def test_list_cooldowns_empty(conn):
    assert list_cooldowns(conn) == []


def test_list_cooldowns_returns_all(conn):
    set_cooldown(conn, "alpha", 60)
    set_cooldown(conn, "beta", 120)
    result = list_cooldowns(conn)
    names = [r["job_name"] for r in result]
    assert "alpha" in names
    assert "beta" in names
    assert len(result) == 2


def test_is_in_cooldown_true(conn):
    set_cooldown(conn, "myjob", 3600)
    last_finish = datetime.utcnow() - timedelta(seconds=30)
    assert is_in_cooldown(conn, "myjob", last_finish) is True


def test_is_in_cooldown_false_after_expiry(conn):
    set_cooldown(conn, "myjob", 60)
    last_finish = datetime.utcnow() - timedelta(seconds=120)
    assert is_in_cooldown(conn, "myjob", last_finish) is False


def test_is_in_cooldown_no_cooldown_set(conn):
    last_finish = datetime.utcnow()
    assert is_in_cooldown(conn, "unknown", last_finish) is False


def test_is_in_cooldown_no_last_finish(conn):
    set_cooldown(conn, "myjob", 3600)
    assert is_in_cooldown(conn, "myjob", None) is False
