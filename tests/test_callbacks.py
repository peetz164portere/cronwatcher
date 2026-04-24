import sqlite3
import pytest
from cronwatcher.callbacks import (
    init_callbacks,
    add_callback,
    get_callbacks,
    remove_callback,
    list_all_callbacks,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_callbacks(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='callbacks'"
    ).fetchall()
    assert len(tables) == 1


def test_add_callback_returns_id(conn):
    rid = add_callback(conn, "backup", "failure", "https://example.com/hook")
    assert isinstance(rid, int)
    assert rid > 0


def test_add_callback_normalizes_case(conn):
    add_callback(conn, "Backup", "Failure", "https://example.com/hook")
    rows = get_callbacks(conn, "backup", "failure")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "backup"
    assert rows[0]["event"] == "failure"


def test_add_callback_invalid_event_raises(conn):
    with pytest.raises(ValueError, match="event must be one of"):
        add_callback(conn, "job", "unknown", "https://example.com")


def test_add_callback_duplicate_ignored(conn):
    add_callback(conn, "job", "success", "https://example.com")
    add_callback(conn, "job", "success", "https://example.com")
    rows = get_callbacks(conn, "job", "success")
    assert len(rows) == 1


def test_get_callbacks_empty(conn):
    assert get_callbacks(conn, "nojob", "failure") == []


def test_get_callbacks_returns_correct_event(conn):
    add_callback(conn, "job", "success", "https://ok.com")
    add_callback(conn, "job", "failure", "https://fail.com")
    rows = get_callbacks(conn, "job", "success")
    assert len(rows) == 1
    assert rows[0]["url"] == "https://ok.com"


def test_get_callbacks_includes_any(conn):
    add_callback(conn, "job", "any", "https://always.com")
    add_callback(conn, "job", "failure", "https://fail.com")
    rows = get_callbacks(conn, "job", "failure")
    urls = {r["url"] for r in rows}
    assert "https://always.com" in urls
    assert "https://fail.com" in urls


def test_get_callbacks_stores_headers(conn):
    add_callback(conn, "job", "any", "https://h.com", headers={"X-Token": "abc"})
    rows = get_callbacks(conn, "job", "any")
    assert rows[0]["headers"] == {"X-Token": "abc"}


def test_remove_callback_returns_true(conn):
    cid = add_callback(conn, "job", "success", "https://x.com")
    assert remove_callback(conn, cid) is True
    assert get_callbacks(conn, "job", "success") == []


def test_remove_callback_missing_returns_false(conn):
    assert remove_callback(conn, 9999) is False


def test_list_all_callbacks(conn):
    add_callback(conn, "alpha", "failure", "https://a.com")
    add_callback(conn, "beta", "success", "https://b.com")
    all_cb = list_all_callbacks(conn)
    assert len(all_cb) == 2
    names = {r["job_name"] for r in all_cb}
    assert names == {"alpha", "beta"}
