import sqlite3
import pytest
from cronwatcher.pinned import init_pinned, pin_run, unpin_run, is_pinned, list_pinned, clear_pinned


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_pinned(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pinned_runs'").fetchone()
    assert row is not None


def test_pin_run_returns_id(conn):
    rid = pin_run(conn, 42, "backup")
    assert isinstance(rid, int) and rid > 0


def test_pin_run_duplicate_ignored(conn):
    pin_run(conn, 42, "backup")
    rid2 = pin_run(conn, 42, "backup", note="again")
    # INSERT OR IGNORE returns 0 rowid on conflict
    rows = list_pinned(conn)
    assert len(rows) == 1


def test_is_pinned_true(conn):
    pin_run(conn, 10, "myjob")
    assert is_pinned(conn, 10) is True


def test_is_pinned_false(conn):
    assert is_pinned(conn, 99) is False


def test_unpin_run_returns_true(conn):
    pin_run(conn, 5, "job-a")
    result = unpin_run(conn, 5)
    assert result is True
    assert is_pinned(conn, 5) is False


def test_unpin_run_missing_returns_false(conn):
    assert unpin_run(conn, 999) is False


def test_list_pinned_all(conn):
    pin_run(conn, 1, "job-a", note="first")
    pin_run(conn, 2, "job-b")
    rows = list_pinned(conn)
    assert len(rows) == 2
    assert rows[0]["run_id"] in (1, 2)


def test_list_pinned_by_job(conn):
    pin_run(conn, 1, "job-a")
    pin_run(conn, 2, "job-b")
    rows = list_pinned(conn, job_name="job-a")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "job-a"


def test_list_pinned_normalizes_case(conn):
    pin_run(conn, 7, "MyJob")
    rows = list_pinned(conn, job_name="myjob")
    assert len(rows) == 1


def test_clear_pinned_all(conn):
    pin_run(conn, 1, "job-a")
    pin_run(conn, 2, "job-b")
    count = clear_pinned(conn)
    assert count == 2
    assert list_pinned(conn) == []


def test_clear_pinned_by_job(conn):
    pin_run(conn, 1, "job-a")
    pin_run(conn, 2, "job-b")
    count = clear_pinned(conn, job_name="job-a")
    assert count == 1
    assert len(list_pinned(conn)) == 1
