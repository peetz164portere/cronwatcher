import sqlite3
from datetime import datetime, timedelta
import pytest
from cronwatcher.silences import (
    init_silences,
    add_silence,
    is_silenced,
    list_silences,
    remove_silence,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_silences(c)
    yield c
    c.close()


def _window(offset_hours=0, duration_hours=2):
    now = datetime.utcnow() + timedelta(hours=offset_hours)
    return now, now + timedelta(hours=duration_hours)


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [r[0] for r in tables]
    assert "silences" in names


def test_add_silence_returns_id(conn):
    start, end = _window()
    sid = add_silence(conn, "backup", start, end, reason="maintenance")
    assert isinstance(sid, int) and sid > 0


def test_is_silenced_within_window(conn):
    start, end = _window(-1, 4)
    add_silence(conn, "backup", start, end)
    assert is_silenced(conn, "backup") is True


def test_is_silenced_outside_window(conn):
    start, end = _window(5, 2)
    add_silence(conn, "backup", start, end)
    assert is_silenced(conn, "backup") is False


def test_is_silenced_normalizes_case(conn):
    start, end = _window(-1, 4)
    add_silence(conn, "MyJob", start, end)
    assert is_silenced(conn, "myjob") is True


def test_is_silenced_different_job(conn):
    start, end = _window(-1, 4)
    add_silence(conn, "backup", start, end)
    assert is_silenced(conn, "cleanup") is False


def test_list_silences_all(conn):
    s, e = _window()
    add_silence(conn, "jobA", s, e)
    add_silence(conn, "jobB", s, e)
    rows = list_silences(conn)
    assert len(rows) == 2


def test_list_silences_filtered(conn):
    s, e = _window()
    add_silence(conn, "jobA", s, e)
    add_silence(conn, "jobB", s, e)
    rows = list_silences(conn, job_name="jobA")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "joba"


def test_remove_silence(conn):
    s, e = _window(-1, 4)
    sid = add_silence(conn, "backup", s, e)
    assert is_silenced(conn, "backup") is True
    removed = remove_silence(conn, sid)
    assert removed is True
    assert is_silenced(conn, "backup") is False


def test_remove_nonexistent_silence(conn):
    assert remove_silence(conn, 9999) is False
