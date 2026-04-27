import sqlite3
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cronwatcher.cli_permissions import permissions_cmd
from cronwatcher.permissions import init_permissions


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    init_permissions(conn)
    return conn


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_permissions._get_conn", return_value=mock_conn)


def test_grant_permission(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["grant", "backup", "alice", "run"])
    assert result.exit_code == 0
    assert "Granted" in result.output
    assert "alice" in result.output


def test_grant_invalid_action(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["grant", "backup", "alice", "fly"])
    assert result.exit_code == 1
    assert "Error" in result.output


def test_revoke_existing(runner, mock_conn):
    from cronwatcher.permissions import grant_permission
    grant_permission(mock_conn, "backup", "alice", "run")
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["revoke", "backup", "alice", "run"])
    assert result.exit_code == 0
    assert "Revoked" in result.output


def test_revoke_nonexistent(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["revoke", "backup", "alice", "run"])
    assert result.exit_code == 1
    assert "No matching" in result.output


def test_check_allowed(runner, mock_conn):
    from cronwatcher.permissions import grant_permission
    grant_permission(mock_conn, "backup", "alice", "view")
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["check", "backup", "alice", "view"])
    assert result.exit_code == 0
    assert "ALLOWED" in result.output


def test_check_denied(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["check", "backup", "alice", "view"])
    assert result.exit_code == 1
    assert "DENIED" in result.output


def test_list_by_job(runner, mock_conn):
    from cronwatcher.permissions import grant_permission
    grant_permission(mock_conn, "backup", "alice", "run")
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["list", "backup"])
    assert result.exit_code == 0
    assert "alice" in result.output


def test_list_all(runner, mock_conn):
    from cronwatcher.permissions import grant_permission
    grant_permission(mock_conn, "backup", "alice", "run")
    grant_permission(mock_conn, "cleanup", "bob", "view")
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["list", "--all"])
    assert result.exit_code == 0
    assert "alice" in result.output
    assert "bob" in result.output


def test_list_no_args_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(permissions_cmd, ["list"])
    assert result.exit_code == 1
