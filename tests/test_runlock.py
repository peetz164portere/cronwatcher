import time
import pytest
import sqlite3
from cronwatcher.runlock import (
    init_runlock, acquire_lock, release_lock, get_lock, clear_stale_locks
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_runlock(c)
    return c


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='run_locks'"
    ).fetchall()
    assert len(tables) == 1


def test_acquire_lock_succeeds(conn):
    assert acquire_lock(conn, "backup", pid=1234) is True


def test_acquire_lock_blocked_when_held(conn):
    acquire_lock(conn, "backup", pid=1234)
    assert acquire_lock(conn, "backup", pid=5678) is False


def test_acquire_lock_normalizes_case(conn):
    acquire_lock(conn, "Backup", pid=1)
    assert acquire_lock(conn, "backup", pid=2) is False


def test_release_lock_removes_entry(conn):
    acquire_lock(conn, "backup", pid=1)
    assert release_lock(conn, "backup") is True
    assert get_lock(conn, "backup") is None


def test_release_lock_returns_false_when_not_locked(conn):
    assert release_lock(conn, "nonexistent") is False


def test_get_lock_returns_info(conn):
    acquire_lock(conn, "myjob", pid=42)
    lock = get_lock(conn, "myjob")
    assert lock is not None
    assert lock["pid"] == 42
    assert lock["job_name"] == "myjob"
    assert lock["locked_at"] == pytest.approx(time.time(), abs=2)


def test_get_lock_none_when_empty(conn):
    assert get_lock(conn, "ghost") is None


def test_clear_stale_locks_removes_old(conn):
    conn.execute(
        "INSERT INTO run_locks (job_name, pid, locked_at) VALUES (?, ?, ?)",
        ("oldjob", 99, time.time() - 7200),
    )
    conn.commit()
    removed = clear_stale_locks(conn, max_age_seconds=3600)
    assert removed == 1
    assert get_lock(conn, "oldjob") is None


def test_clear_stale_locks_keeps_recent(conn):
    acquire_lock(conn, "newjob", pid=7)
    removed = clear_stale_locks(conn, max_age_seconds=3600)
    assert removed == 0
    assert get_lock(conn, "newjob") is not None


def test_reacquire_after_release(conn):
    acquire_lock(conn, "job", pid=1)
    release_lock(conn, "job")
    assert acquire_lock(conn, "job", pid=2) is True
