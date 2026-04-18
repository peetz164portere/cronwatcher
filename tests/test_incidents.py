import sqlite3
import pytest
from cronwatcher.incidents import init_incidents, open_incident, close_incident, get_open_incident, list_incidents


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_incidents(c)
    return c


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "incidents" in tables


def test_open_incident_returns_id(conn):
    iid = open_incident(conn, "backup")
    assert isinstance(iid, int)
    assert iid > 0


def test_open_incident_idempotent(conn):
    id1 = open_incident(conn, "backup")
    id2 = open_incident(conn, "backup")
    assert id1 == id2


def test_get_open_incident(conn):
    open_incident(conn, "sync", run_id=42, note="disk full")
    inc = get_open_incident(conn, "sync")
    assert inc is not None
    assert inc["job_name"] == "sync"
    assert inc["run_id"] == 42
    assert inc["note"] == "disk full"
    assert inc["status"] == "open"


def test_close_incident(conn):
    open_incident(conn, "deploy")
    result = close_incident(conn, "deploy")
    assert result is True
    assert get_open_incident(conn, "deploy") is None


def test_close_nonexistent_returns_false(conn):
    result = close_incident(conn, "ghost")
    assert result is False


def test_list_incidents_all(conn):
    open_incident(conn, "job_a")
    open_incident(conn, "job_b")
    close_incident(conn, "job_a")
    rows = list_incidents(conn)
    assert len(rows) == 2


def test_list_incidents_filter_status(conn):
    open_incident(conn, "job_a")
    open_incident(conn, "job_b")
    close_incident(conn, "job_a")
    open_rows = list_incidents(conn, status="open")
    assert len(open_rows) == 1
    assert open_rows[0]["job_name"] == "job_b"


def test_list_incidents_filter_job(conn):
    open_incident(conn, "job_a")
    open_incident(conn, "job_b")
    rows = list_incidents(conn, job_name="job_a")
    assert len(rows) == 1
