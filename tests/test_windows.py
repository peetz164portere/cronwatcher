"""Tests for cronwatcher.windows."""

import sqlite3
from datetime import datetime
import pytest

from cronwatcher.windows import (
    init_windows,
    add_window,
    remove_window,
    list_windows,
    is_in_maintenance,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_windows(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='maintenance_windows'")
    assert cur.fetchone() is not None


def test_add_window_returns_id(conn):
    wid = add_window(conn, "backup", "mon", "02:00", "04:00")
    assert isinstance(wid, int)
    assert wid > 0


def test_add_window_normalizes_case(conn):
    add_window(conn, "Backup", "MON", "02:00", "04:00")
    rows = list_windows(conn, "backup")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "backup"
    assert rows[0]["day_of_week"] == "mon"


def test_add_window_invalid_day_raises(conn):
    with pytest.raises(ValueError, match="Invalid day"):
        add_window(conn, "myjob", "xyz", "01:00", "02:00")


def test_add_window_invalid_time_raises(conn):
    with pytest.raises(ValueError):
        add_window(conn, "myjob", "mon", "25:00", "26:00")


def test_add_window_with_note(conn):
    add_window(conn, "deploy", "fri", "18:00", "20:00", note="weekly deploy")
    rows = list_windows(conn, "deploy")
    assert rows[0]["note"] == "weekly deploy"


def test_list_windows_all(conn):
    add_window(conn, "job_a", "mon", "01:00", "02:00")
    add_window(conn, "job_b", "tue", "03:00", "04:00")
    rows = list_windows(conn)
    assert len(rows) == 2


def test_list_windows_filtered(conn):
    add_window(conn, "job_a", "mon", "01:00", "02:00")
    add_window(conn, "job_b", "tue", "03:00", "04:00")
    rows = list_windows(conn, "job_a")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "job_a"


def test_remove_window_returns_true(conn):
    wid = add_window(conn, "myjob", "wed", "10:00", "11:00")
    assert remove_window(conn, wid) is True
    assert list_windows(conn) == []


def test_remove_nonexistent_returns_false(conn):
    assert remove_window(conn, 9999) is False


def test_is_in_maintenance_true(conn):
    # Monday = weekday 0 => DAYS[0] = 'mon'
    add_window(conn, "myjob", "mon", "02:00", "04:00")
    dt = datetime(2024, 1, 1, 3, 0)  # Monday 03:00
    assert is_in_maintenance(conn, "myjob", dt) is True


def test_is_in_maintenance_false_outside_time(conn):
    add_window(conn, "myjob", "mon", "02:00", "04:00")
    dt = datetime(2024, 1, 1, 5, 0)  # Monday 05:00, outside window
    assert is_in_maintenance(conn, "myjob", dt) is False


def test_is_in_maintenance_false_wrong_day(conn):
    add_window(conn, "myjob", "mon", "02:00", "04:00")
    dt = datetime(2024, 1, 2, 3, 0)  # Tuesday
    assert is_in_maintenance(conn, "myjob", dt) is False


def test_is_in_maintenance_boundary_end_exclusive(conn):
    add_window(conn, "myjob", "mon", "02:00", "04:00")
    dt = datetime(2024, 1, 1, 4, 0)  # exactly at end time, should be excluded
    assert is_in_maintenance(conn, "myjob", dt) is False
