import sqlite3
import pytest
from cronwatcher.triggers import (
    init_triggers,
    add_trigger,
    get_triggers,
    remove_trigger,
    set_enabled,
    list_all_triggers,
    VALID_CONDITIONS,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_triggers(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='triggers'"
    ).fetchone()
    assert row is not None


def test_add_trigger_returns_id(conn):
    tid = add_trigger(conn, "backup", "on_failure", "webhook")
    assert isinstance(tid, int)
    assert tid > 0


def test_add_trigger_normalizes_case(conn):
    tid1 = add_trigger(conn, "Backup", "ON_FAILURE", "webhook")
    tid2 = add_trigger(conn, "backup", "on_failure", "webhook")
    assert tid1 == tid2


def test_add_trigger_invalid_condition_raises(conn):
    with pytest.raises(ValueError, match="Invalid condition"):
        add_trigger(conn, "backup", "on_banana", "webhook")


def test_add_trigger_duplicate_ignored(conn):
    id1 = add_trigger(conn, "myjob", "on_success", "slack")
    id2 = add_trigger(conn, "myjob", "on_success", "slack")
    assert id1 == id2


def test_add_trigger_with_params(conn):
    tid = add_trigger(conn, "myjob", "on_slow", "webhook", {"url": "http://example.com"})
    triggers = get_triggers(conn, "myjob", "on_slow")
    assert len(triggers) == 1
    assert triggers[0]["params"]["url"] == "http://example.com"
    assert triggers[0]["id"] == tid


def test_get_triggers_empty(conn):
    result = get_triggers(conn, "nonexistent")
    assert result == []


def test_get_triggers_filters_by_condition(conn):
    add_trigger(conn, "job1", "on_failure", "webhook")
    add_trigger(conn, "job1", "on_success", "slack")
    result = get_triggers(conn, "job1", "on_failure")
    assert len(result) == 1
    assert result[0]["condition"] == "on_failure"


def test_get_triggers_returns_all_conditions_when_no_filter(conn):
    add_trigger(conn, "job1", "on_failure", "webhook")
    add_trigger(conn, "job1", "on_success", "slack")
    result = get_triggers(conn, "job1")
    assert len(result) == 2


def test_remove_trigger_returns_true(conn):
    tid = add_trigger(conn, "job1", "always", "log")
    assert remove_trigger(conn, tid) is True
    assert get_triggers(conn, "job1") == []


def test_remove_trigger_missing_returns_false(conn):
    assert remove_trigger(conn, 9999) is False


def test_set_enabled_disables_trigger(conn):
    tid = add_trigger(conn, "job1", "on_overdue", "pagerduty")
    assert set_enabled(conn, tid, False) is True
    result = get_triggers(conn, "job1", "on_overdue")
    assert result == []  # disabled triggers not returned


def test_set_enabled_missing_returns_false(conn):
    assert set_enabled(conn, 9999, True) is False


def test_list_all_triggers(conn):
    add_trigger(conn, "alpha", "on_failure", "webhook")
    add_trigger(conn, "beta", "on_success", "slack")
    all_t = list_all_triggers(conn)
    assert len(all_t) == 2
    names = {t["job_name"] for t in all_t}
    assert names == {"alpha", "beta"}


def test_valid_conditions_set():
    assert "on_failure" in VALID_CONDITIONS
    assert "on_success" in VALID_CONDITIONS
    assert "always" in VALID_CONDITIONS
