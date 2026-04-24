"""Tests for cronwatcher/search.py and cli_search.py."""
import pytest
from datetime import datetime, timedelta
from cronwatcher.storage import get_connection, init_db
from cronwatcher.search import search_history, count_by_status
from click.testing import CliRunner
from cronwatcher.cli_search import search_cmd


@pytest.fixture
def conn(tmp_path):
    db = str(tmp_path / "test.db")
    c = get_connection(db)
    init_db(c)
    return c


def _insert(conn, job, exit_code, offset_hours=0):
    """Insert a test run record into the database.

    Args:
        conn: SQLite connection.
        job: Job name string.
        exit_code: Exit code (None means still running).
        offset_hours: How many hours in the past to set started_at.
    """
    started = (datetime.utcnow() - timedelta(hours=offset_hours)).isoformat()
    finished = datetime.utcnow().isoformat() if exit_code is not None else None
    conn.execute(
        "INSERT INTO runs (job_name, started_at, finished_at, exit_code) VALUES (?,?,?,?)",
        (job, started, finished, exit_code)
    )
    conn.commit()


def test_search_by_job_name(conn):
    _insert(conn, "backup", 0)
    _insert(conn, "cleanup", 1)
    rows = search_history(conn, job_name="back")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "backup"


def test_search_by_status_success(conn):
    _insert(conn, "job1", 0)
    _insert(conn, "job2", 1)
    rows = search_history(conn, status="success")
    assert all(r["exit_code"] == 0 for r in rows)


def test_search_by_status_failure(conn):
    _insert(conn, "job1", 0)
    _insert(conn, "job2", 2)
    rows = search_history(conn, status="failure")
    assert all(r["exit_code"] != 0 for r in rows)


def test_search_by_status_running(conn):
    _insert(conn, "job1", None)  # running
    _insert(conn, "job2", 0)
    rows = search_history(conn, status="running")
    assert len(rows) == 1
    assert rows[0]["finished_at"] is None


def test_search_since_filter(conn):
    _insert(conn, "old", 0, offset_hours=48)
    _insert(conn, "new", 0, offset_hours=0)
    since = datetime.utcnow() - timedelta(hours=1)
    rows = search_history(conn, since=since)
    assert all(r["job_name"] == "new" for r in rows)


def test_search_no_filters_returns_all(conn):
    """Calling search_history with no filters should return all rows."""
    _insert(conn, "alpha", 0)
    _insert(conn, "beta", 1)
    _insert(conn, "gamma", None)
    rows = search_history(conn)
    assert len(rows) == 3


def test_count_by_status(conn):
    _insert(conn, "job1", 0)
    _insert(conn, "job1", 1)
    _insert(conn, "job1", None)
    counts = count_by_status(conn, job_name="job1")
    assert counts["total"] == 3
    assert counts["success"] == 1
    assert counts["failure"] == 1
    assert counts["running"] == 1


def test_count_all_jobs(conn):
    _insert(conn, "a", 0)
    _insert(conn, "b", 1)
    counts = count_by_status(conn)
    assert counts["total"] == 2


def test_cli_find_no_results(tmp_path):
    runner = CliRunner()
    db = str(tmp_path / "t.db")
    result = runner.invoke(search_cmd, ["find", "--db", db, "--job", "ghost"])
    assert result.exit_code == 0
    assert "No matching" in result.output


def test_cli_stats(tmp_path):
    runner = CliRunner()
    db = str(tmp_path / "t.db")
    c = get_connection(db)
    init_db(c)
    _insert(c, "myjob", 0)
    result = runner.invoke(search_cmd, ["stats", "--db", db, "--job", "myjob"])
    assert result.exit_code == 0
    assert "Success: 1" in result.output
