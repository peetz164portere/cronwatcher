import sqlite3
import pytest
from cronwatcher.ownership import (
    init_ownership,
    set_owner,
    get_owner,
    remove_owner,
    list_owners,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_ownership(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    assert ("ownership",) in tables


def test_set_and_get_owner(conn):
    set_owner(conn, "backup-job", "alice", email="alice@example.com", team="ops")
    result = get_owner(conn, "backup-job")
    assert result is not None
    assert result["owner"] == "alice"
    assert result["email"] == "alice@example.com"
    assert result["team"] == "ops"


def test_get_owner_missing_returns_none(conn):
    assert get_owner(conn, "nonexistent") is None


def test_set_owner_normalizes_case(conn):
    set_owner(conn, "MyJob", "bob")
    assert get_owner(conn, "myjob") is not None
    assert get_owner(conn, "MYJOB") is not None


def test_set_owner_upserts(conn):
    set_owner(conn, "deploy", "alice", team="dev")
    set_owner(conn, "deploy", "bob", team="ops")
    result = get_owner(conn, "deploy")
    assert result["owner"] == "bob"
    assert result["team"] == "ops"


def test_remove_owner_returns_true(conn):
    set_owner(conn, "cleanup", "carol")
    assert remove_owner(conn, "cleanup") is True
    assert get_owner(conn, "cleanup") is None


def test_remove_owner_missing_returns_false(conn):
    assert remove_owner(conn, "ghost-job") is False


def test_list_owners_empty(conn):
    assert list_owners(conn) == []


def test_list_owners_returns_all(conn):
    set_owner(conn, "job-a", "alice")
    set_owner(conn, "job-b", "bob", email="bob@example.com")
    results = list_owners(conn)
    assert len(results) == 2
    names = [r["job_name"] for r in results]
    assert "job-a" in names
    assert "job-b" in names


def test_set_owner_optional_fields_none(conn):
    set_owner(conn, "minimal-job", "dave")
    result = get_owner(conn, "minimal-job")
    assert result["email"] is None
    assert result["team"] is None
