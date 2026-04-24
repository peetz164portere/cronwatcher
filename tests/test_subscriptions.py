import pytest
import sqlite3
from cronwatcher.subscriptions import (
    init_subscriptions,
    add_subscription,
    get_subscriptions,
    remove_subscription,
    list_all_subscriptions,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_subscriptions(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    assert ("subscriptions",) in tables


def test_add_subscription_returns_id(conn):
    sid = add_subscription(conn, "backup", "failure", "https://hooks.example.com/1")
    assert isinstance(sid, int)
    assert sid > 0


def test_add_subscription_normalizes_case(conn):
    add_subscription(conn, "Backup", "Failure", "https://hooks.example.com/1")
    rows = get_subscriptions(conn, "backup", "failure")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "backup"
    assert rows[0]["event"] == "failure"


def test_add_subscription_invalid_event_raises(conn):
    with pytest.raises(ValueError, match="Invalid event"):
        add_subscription(conn, "backup", "explode", "https://hooks.example.com/1")


def test_add_subscription_duplicate_ignored(conn):
    add_subscription(conn, "backup", "failure", "https://hooks.example.com/1")
    add_subscription(conn, "backup", "failure", "https://hooks.example.com/1")
    rows = get_subscriptions(conn, "backup", "failure")
    assert len(rows) == 1


def test_get_subscriptions_empty(conn):
    rows = get_subscriptions(conn, "nojob", "success")
    assert rows == []


def test_get_subscriptions_returns_correct_job(conn):
    add_subscription(conn, "backup", "failure", "https://a.example.com")
    add_subscription(conn, "sync", "failure", "https://b.example.com")
    rows = get_subscriptions(conn, "backup", "failure")
    assert len(rows) == 1
    assert rows[0]["url"] == "https://a.example.com"


def test_add_subscription_stores_headers(conn):
    headers = {"Authorization": "Bearer token123"}
    add_subscription(conn, "backup", "success", "https://hooks.example.com/1", headers)
    rows = get_subscriptions(conn, "backup", "success")
    assert rows[0]["headers"] == headers


def test_remove_subscription_returns_true(conn):
    sid = add_subscription(conn, "backup", "failure", "https://hooks.example.com/1")
    result = remove_subscription(conn, sid)
    assert result is True
    assert get_subscriptions(conn, "backup", "failure") == []


def test_remove_subscription_missing_returns_false(conn):
    result = remove_subscription(conn, 9999)
    assert result is False


def test_list_all_subscriptions(conn):
    add_subscription(conn, "backup", "failure", "https://a.example.com")
    add_subscription(conn, "sync", "success", "https://b.example.com")
    rows = list_all_subscriptions(conn)
    assert len(rows) == 2
