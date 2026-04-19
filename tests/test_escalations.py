import sqlite3
import pytest
from cronwatcher.escalations import (
    init_escalations, set_escalation, get_escalations,
    remove_escalation, get_next_level
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_escalations(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [t[0] for t in tables]
    assert "escalations" in names


def test_set_escalation_returns_id(conn):
    eid = set_escalation(conn, "backup", 1, "http://hook1", 15)
    assert isinstance(eid, int)
    assert eid > 0


def test_get_escalations_empty(conn):
    result = get_escalations(conn, "nojob")
    assert result == []


def test_get_escalations_returns_correct_job(conn):
    set_escalation(conn, "backup", 1, "http://hook1", 15)
    set_escalation(conn, "backup", 2, "http://hook2", 60)
    set_escalation(conn, "other", 1, "http://hook3", 30)
    results = get_escalations(conn, "backup")
    assert len(results) == 2
    assert results[0]["level"] == 1
    assert results[1]["level"] == 2


def test_get_escalations_normalizes_case(conn):
    set_escalation(conn, "MyJob", 1, "http://hook", 10)
    results = get_escalations(conn, "myjob")
    assert len(results) == 1


def test_remove_escalation_returns_true(conn):
    eid = set_escalation(conn, "backup", 1, "http://hook", 30)
    assert remove_escalation(conn, eid) is True
    assert get_escalations(conn, "backup") == []


def test_remove_escalation_missing_returns_false(conn):
    assert remove_escalation(conn, 9999) is False


def test_get_next_level_none_when_no_higher(conn):
    set_escalation(conn, "backup", 1, "http://hook", 15)
    result = get_next_level(conn, "backup", 1)
    assert result is None


def test_get_next_level_returns_next(conn):
    set_escalation(conn, "backup", 1, "http://hook1", 15)
    set_escalation(conn, "backup", 2, "http://hook2", 60)
    result = get_next_level(conn, "backup", 1)
    assert result is not None
    assert result["level"] == 2
    assert result["webhook_url"] == "http://hook2"


def test_escalation_dict_has_expected_keys(conn):
    set_escalation(conn, "backup", 1, "http://hook", 30)
    results = get_escalations(conn, "backup")
    keys = results[0].keys()
    for k in ("id", "job_name", "level", "webhook_url", "threshold_minutes", "created_at"):
        assert k in keys
