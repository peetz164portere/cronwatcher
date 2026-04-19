import sqlite3
import pytest
from cronwatcher.aliases import (
    init_aliases,
    set_alias,
    get_alias,
    remove_alias,
    list_aliases,
    resolve,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_aliases(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='aliases'"
    ).fetchone()
    assert row is not None


def test_set_and_get_alias(conn):
    set_alias(conn, "backup", "nightly-backup")
    assert get_alias(conn, "backup") == "nightly-backup"


def test_set_alias_normalizes_case(conn):
    set_alias(conn, "BACKUP", "nightly-backup")
    assert get_alias(conn, "backup") == "nightly-backup"


def test_get_alias_missing_returns_none(conn):
    assert get_alias(conn, "nonexistent") is None


def test_set_alias_replaces_existing(conn):
    set_alias(conn, "bk", "old-job")
    set_alias(conn, "bk", "new-job")
    assert get_alias(conn, "bk") == "new-job"


def test_remove_alias_returns_true(conn):
    set_alias(conn, "bk", "some-job")
    assert remove_alias(conn, "bk") is True
    assert get_alias(conn, "bk") is None


def test_remove_alias_missing_returns_false(conn):
    assert remove_alias(conn, "ghost") is False


def test_list_aliases_empty(conn):
    assert list_aliases(conn) == []


def test_list_aliases_returns_all(conn):
    set_alias(conn, "a", "job-a")
    set_alias(conn, "b", "job-b")
    results = list_aliases(conn)
    assert len(results) == 2
    assert results[0]["alias"] == "a"
    assert results[1]["job_name"] == "job-b"


def test_resolve_known_alias(conn):
    set_alias(conn, "sync", "data-sync-job")
    assert resolve(conn, "sync") == "data-sync-job"


def test_resolve_unknown_returns_original(conn):
    assert resolve(conn, "data-sync-job") == "data-sync-job"
