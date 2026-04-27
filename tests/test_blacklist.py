"""Tests for cronwatcher.blacklist."""

import sqlite3
import pytest

from cronwatcher.blacklist import (
    init_blacklist,
    add_to_blacklist,
    remove_from_blacklist,
    is_blacklisted,
    list_blacklist,
    clear_blacklist,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_blacklist(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='blacklist'"
    ).fetchone()
    assert row is not None


def test_add_returns_id(conn):
    rid = add_to_blacklist(conn, "backup-job")
    assert isinstance(rid, int)
    assert rid > 0


def test_add_normalizes_case(conn):
    add_to_blacklist(conn, "MyJob")
    assert is_blacklisted(conn, "myjob")
    assert is_blacklisted(conn, "MYJOB")


def test_add_duplicate_ignored(conn):
    id1 = add_to_blacklist(conn, "dup-job")
    id2 = add_to_blacklist(conn, "dup-job")
    # Second insert is ignored; lastrowid may be 0 or same
    entries = list_blacklist(conn)
    assert len(entries) == 1


def test_is_blacklisted_false_when_empty(conn):
    assert not is_blacklisted(conn, "unknown-job")


def test_is_blacklisted_true_after_add(conn):
    add_to_blacklist(conn, "bad-job")
    assert is_blacklisted(conn, "bad-job")


def test_remove_existing_returns_true(conn):
    add_to_blacklist(conn, "temp-job")
    result = remove_from_blacklist(conn, "temp-job")
    assert result is True
    assert not is_blacklisted(conn, "temp-job")


def test_remove_missing_returns_false(conn):
    result = remove_from_blacklist(conn, "ghost-job")
    assert result is False


def test_list_blacklist_empty(conn):
    assert list_blacklist(conn) == []


def test_list_blacklist_returns_entries(conn):
    add_to_blacklist(conn, "job-a", reason="too noisy")
    add_to_blacklist(conn, "job-b")
    entries = list_blacklist(conn)
    assert len(entries) == 2
    names = {e["job_name"] for e in entries}
    assert names == {"job-a", "job-b"}
    reasons = {e["job_name"]: e["reason"] for e in entries}
    assert reasons["job-a"] == "too noisy"
    assert reasons["job-b"] is None


def test_clear_blacklist(conn):
    add_to_blacklist(conn, "x")
    add_to_blacklist(conn, "y")
    removed = clear_blacklist(conn)
    assert removed == 2
    assert list_blacklist(conn) == []
