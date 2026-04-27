"""Tests for cronwatcher.curfews."""

import sqlite3
from datetime import datetime

import pytest

from cronwatcher.curfews import (
    add_curfew,
    get_curfews,
    init_curfews,
    is_in_curfew,
    remove_curfew,
)


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_curfews(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='curfews'"
    ).fetchall()
    assert len(tables) == 1


def test_add_curfew_returns_id(conn):
    rid = add_curfew(conn, "backup", "all", "02:00", "04:00", "nightly freeze")
    assert isinstance(rid, int)
    assert rid > 0


def test_add_curfew_normalizes_case(conn):
    add_curfew(conn, "BACKUP", "MON", "01:00", "03:00")
    rows = get_curfews(conn, "backup")
    assert rows[0]["job_name"] == "backup"
    assert rows[0]["day"] == "mon"


def test_add_curfew_invalid_day_raises(conn):
    with pytest.raises(ValueError, match="Invalid day"):
        add_curfew(conn, "backup", "funday", "01:00", "03:00")


def test_add_curfew_invalid_time_raises(conn):
    with pytest.raises(ValueError):
        add_curfew(conn, "backup", "all", "25:99", "03:00")


def test_get_curfews_empty(conn):
    assert get_curfews(conn, "nonexistent") == []


def test_get_curfews_returns_correct_job(conn):
    add_curfew(conn, "job_a", "all", "00:00", "01:00")
    add_curfew(conn, "job_b", "all", "00:00", "01:00")
    rows = get_curfews(conn, "job_a")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "job_a"


def test_remove_curfew_returns_true(conn):
    rid = add_curfew(conn, "backup", "all", "02:00", "04:00")
    assert remove_curfew(conn, rid) is True
    assert get_curfews(conn, "backup") == []


def test_remove_curfew_missing_returns_false(conn):
    assert remove_curfew(conn, 9999) is False


def test_is_in_curfew_true(conn):
    add_curfew(conn, "backup", "all", "02:00", "04:00")
    dt = datetime(2024, 1, 15, 3, 30)  # 03:30 — inside window
    assert is_in_curfew(conn, "backup", dt) is True


def test_is_in_curfew_false_outside_window(conn):
    add_curfew(conn, "backup", "all", "02:00", "04:00")
    dt = datetime(2024, 1, 15, 5, 0)  # 05:00 — outside window
    assert is_in_curfew(conn, "backup", dt) is False


def test_is_in_curfew_respects_day(conn):
    # curfew only on monday
    add_curfew(conn, "backup", "mon", "02:00", "04:00")
    tuesday = datetime(2024, 1, 16, 3, 0)  # Tuesday
    assert is_in_curfew(conn, "backup", tuesday) is False


def test_is_in_curfew_no_curfews(conn):
    dt = datetime(2024, 1, 15, 3, 30)
    assert is_in_curfew(conn, "ghost_job", dt) is False
