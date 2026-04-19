import sqlite3
from datetime import datetime, timedelta
import pytest
from cronwatcher.reminders import (
    init_reminders, set_reminder, get_reminder,
    list_reminders, remove_reminder, check_reminders
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute("""
        CREATE TABLE runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT,
            status TEXT,
            started_at TEXT
        )
    """)
    init_reminders(c)
    return c


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='reminders'"
    ).fetchone()
    assert row is not None


def test_set_reminder_returns_id(conn):
    rid = set_reminder(conn, "backup", 24.0)
    assert isinstance(rid, int)


def test_get_reminder_returns_dict(conn):
    set_reminder(conn, "Backup", 12.0)
    r = get_reminder(conn, "backup")
    assert r is not None
    assert r["job_name"] == "backup"
    assert r["interval_hours"] == 12.0


def test_get_reminder_missing_returns_none(conn):
    assert get_reminder(conn, "nonexistent") is None


def test_set_reminder_upserts(conn):
    set_reminder(conn, "job", 6.0)
    set_reminder(conn, "job", 12.0)
    r = get_reminder(conn, "job")
    assert r["interval_hours"] == 12.0
    assert len(list_reminders(conn)) == 1


def test_list_reminders_empty(conn):
    assert list_reminders(conn) == []


def test_list_reminders_multiple(conn):
    set_reminder(conn, "a", 1.0)
    set_reminder(conn, "b", 2.0)
    names = [r["job_name"] for r in list_reminders(conn)]
    assert names == ["a", "b"]


def test_remove_reminder(conn):
    set_reminder(conn, "job", 1.0)
    assert remove_reminder(conn, "job") is True
    assert get_reminder(conn, "job") is None


def test_remove_missing_returns_false(conn):
    assert remove_reminder(conn, "ghost") is False


def test_check_reminders_overdue_when_no_runs(conn):
    set_reminder(conn, "daily", 24.0)
    overdue = check_reminders(conn)
    assert len(overdue) == 1
    assert overdue[0]["job_name"] == "daily"


def test_check_reminders_not_overdue_with_recent_run(conn):
    set_reminder(conn, "daily", 24.0)
    recent = datetime.utcnow().isoformat()
    conn.execute("INSERT INTO runs (job_name, status, started_at) VALUES (?,?,?)",
                 ("daily", "success", recent))
    conn.commit()
    assert check_reminders(conn) == []
