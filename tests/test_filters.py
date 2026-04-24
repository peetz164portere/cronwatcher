"""Tests for cronwatcher.filters."""

import sqlite3
import pytest
from cronwatcher.filters import (
    init_filters,
    save_filter,
    get_filter,
    remove_filter,
    list_filters,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_filters(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='saved_filters'"
    ).fetchone()
    assert row is not None


def test_save_filter_returns_id(conn):
    fid = save_filter(conn, "my-filter", {"job": "backup", "status": "failure"})
    assert isinstance(fid, int)
    assert fid > 0


def test_get_filter_returns_params(conn):
    save_filter(conn, "nightly", {"job": "nightly-sync", "limit": 10})
    params = get_filter(conn, "nightly")
    assert params == {"job": "nightly-sync", "limit": 10}


def test_get_filter_missing_returns_none(conn):
    assert get_filter(conn, "nonexistent") is None


def test_save_filter_normalizes_name(conn):
    save_filter(conn, "MyFilter", {"status": "success"})
    params = get_filter(conn, "myfilter")
    assert params is not None


def test_save_filter_upserts(conn):
    save_filter(conn, "alpha", {"status": "success"})
    save_filter(conn, "alpha", {"status": "failure", "limit": 5})
    params = get_filter(conn, "alpha")
    assert params["status"] == "failure"
    assert params["limit"] == 5
    rows = list_filters(conn)
    assert len(rows) == 1


def test_remove_filter_returns_true(conn):
    save_filter(conn, "temp", {"job": "x"})
    assert remove_filter(conn, "temp") is True
    assert get_filter(conn, "temp") is None


def test_remove_filter_missing_returns_false(conn):
    assert remove_filter(conn, "ghost") is False


def test_list_filters_empty(conn):
    assert list_filters(conn) == []


def test_list_filters_returns_all(conn):
    save_filter(conn, "a", {"status": "success"})
    save_filter(conn, "b", {"job": "deploy"})
    rows = list_filters(conn)
    assert len(rows) == 2
    names = [r["name"] for r in rows]
    assert "a" in names
    assert "b" in names


def test_list_filters_sorted_by_name(conn):
    save_filter(conn, "zzz", {"limit": 1})
    save_filter(conn, "aaa", {"limit": 2})
    rows = list_filters(conn)
    assert rows[0]["name"] == "aaa"
    assert rows[1]["name"] == "zzz"
