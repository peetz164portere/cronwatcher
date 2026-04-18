"""Tests for cronwatcher.audit."""
import sqlite3
import pytest
from cronwatcher.audit import init_audit, record_action, get_audit_log, clear_audit_log


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_audit(c)
    return c


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_log'"
    ).fetchone()
    assert tables is not None


def test_record_action_returns_id(conn):
    rid = record_action(conn, "run", target="backup", detail="exit=0")
    assert isinstance(rid, int)
    assert rid >= 1


def test_get_audit_log_empty(conn):
    rows = get_audit_log(conn)
    assert rows == []


def test_get_audit_log_returns_entries(conn):
    record_action(conn, "run", target="job_a")
    record_action(conn, "prune", target=None, detail="older than 30d")
    rows = get_audit_log(conn)
    assert len(rows) == 2
    assert rows[0]["action"] == "prune"  # most recent first


def test_get_audit_log_filter_by_action(conn):
    record_action(conn, "run", target="job_a")
    record_action(conn, "run", target="job_b")
    record_action(conn, "prune")
    rows = get_audit_log(conn, action="run")
    assert len(rows) == 2
    assert all(r["action"] == "run" for r in rows)


def test_get_audit_log_respects_limit(conn):
    for i in range(10):
        record_action(conn, "run", target=f"job_{i}")
    rows = get_audit_log(conn, limit=3)
    assert len(rows) == 3


def test_clear_audit_log(conn):
    record_action(conn, "run")
    record_action(conn, "run")
    n = clear_audit_log(conn)
    assert n == 2
    assert get_audit_log(conn) == []


def test_record_action_stores_fields(conn):
    record_action(conn, "webhook", target="my_job", detail="status=200")
    rows = get_audit_log(conn)
    r = rows[0]
    assert r["action"] == "webhook"
    assert r["target"] == "my_job"
    assert r["detail"] == "status=200"
    assert r["created_at"] is not None
