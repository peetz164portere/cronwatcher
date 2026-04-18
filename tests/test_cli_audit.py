"""Tests for cronwatcher.cli_audit."""
import json
import sqlite3
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from cronwatcher.cli_audit import audit_cmd
from cronwatcher.audit import init_audit, record_action


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_audit(conn)
    return conn


def test_list_empty(runner, mock_conn):
    with patch("cronwatcher.cli_audit._get_conn", return_value=mock_conn):
        result = runner.invoke(audit_cmd, ["list"])
    assert result.exit_code == 0
    assert "No audit entries" in result.output


def test_list_shows_entries(runner, mock_conn):
    record_action(mock_conn, "run", target="backup")
    with patch("cronwatcher.cli_audit._get_conn", return_value=mock_conn):
        result = runner.invoke(audit_cmd, ["list"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "backup" in result.output


def test_list_json_output(runner, mock_conn):
    record_action(mock_conn, "prune", detail="30d")
    with patch("cronwatcher.cli_audit._get_conn", return_value=mock_conn):
        result = runner.invoke(audit_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["action"] == "prune"


def test_list_filter_by_action(runner, mock_conn):
    record_action(mock_conn, "run", target="job_a")
    record_action(mock_conn, "prune")
    with patch("cronwatcher.cli_audit._get_conn", return_value=mock_conn):
        result = runner.invoke(audit_cmd, ["list", "--action", "prune", "--json"])
    data = json.loads(result.output)
    assert len(data) == 1
    assert data[0]["action"] == "prune"


def test_clear_with_confirmation(runner, mock_conn):
    record_action(mock_conn, "run")
    with patch("cronwatcher.cli_audit._get_conn", return_value=mock_conn):
        result = runner.invoke(audit_cmd, ["clear"], input="y\n")
    assert result.exit_code == 0
    assert "Cleared 1" in result.output


def test_clear_aborted(runner, mock_conn):
    record_action(mock_conn, "run")
    with patch("cronwatcher.cli_audit._get_conn", return_value=mock_conn):
        result = runner.invoke(audit_cmd, ["clear"], input="n\n")
    assert result.exit_code != 0 or "Aborted" in result.output
