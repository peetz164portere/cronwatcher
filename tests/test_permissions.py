import sqlite3
import pytest
from cronwatcher.permissions import (
    init_permissions, grant_permission, revoke_permission,
    has_permission, get_permissions, list_all_permissions, VALID_ACTIONS
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_permissions(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='permissions'"
    ).fetchone()
    assert row is not None


def test_grant_permission_returns_id(conn):
    pid = grant_permission(conn, "backup", "alice", "run")
    assert isinstance(pid, int)
    assert pid > 0


def test_grant_permission_normalizes_case(conn):
    grant_permission(conn, "Backup", "Alice", "RUN")
    assert has_permission(conn, "backup", "alice", "run")


def test_grant_permission_invalid_action_raises(conn):
    with pytest.raises(ValueError, match="Invalid action"):
        grant_permission(conn, "backup", "alice", "fly")


def test_grant_permission_duplicate_ignored(conn):
    id1 = grant_permission(conn, "backup", "alice", "run")
    id2 = grant_permission(conn, "backup", "alice", "run")
    assert id1 == id2


def test_has_permission_false_when_not_granted(conn):
    assert not has_permission(conn, "backup", "alice", "run")


def test_has_permission_true_after_grant(conn):
    grant_permission(conn, "backup", "alice", "view")
    assert has_permission(conn, "backup", "alice", "view")


def test_revoke_permission_removes_entry(conn):
    grant_permission(conn, "backup", "alice", "run")
    removed = revoke_permission(conn, "backup", "alice", "run")
    assert removed is True
    assert not has_permission(conn, "backup", "alice", "run")


def test_revoke_nonexistent_returns_false(conn):
    result = revoke_permission(conn, "backup", "alice", "run")
    assert result is False


def test_get_permissions_empty(conn):
    assert get_permissions(conn, "backup") == []


def test_get_permissions_returns_correct_job(conn):
    grant_permission(conn, "backup", "alice", "run")
    grant_permission(conn, "backup", "bob", "view")
    grant_permission(conn, "cleanup", "alice", "admin")
    rows = get_permissions(conn, "backup")
    assert len(rows) == 2
    principals = {r["principal"] for r in rows}
    assert principals == {"alice", "bob"}


def test_get_permissions_has_expected_keys(conn):
    grant_permission(conn, "backup", "alice", "edit")
    rows = get_permissions(conn, "backup")
    assert set(rows[0].keys()) == {"id", "job_name", "principal", "action", "granted_at"}


def test_list_all_permissions_returns_all(conn):
    grant_permission(conn, "backup", "alice", "run")
    grant_permission(conn, "cleanup", "bob", "view")
    rows = list_all_permissions(conn)
    assert len(rows) == 2


def test_valid_actions_set(conn):
    for action in VALID_ACTIONS:
        pid = grant_permission(conn, "job", "user", action)
        assert pid > 0
