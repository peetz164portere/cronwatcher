"""Tests for cronwatcher/cli_circuits.py"""

import json
import sqlite3
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cronwatcher.cli_circuits import circuits_cmd
from cronwatcher import circuits


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    circuits.init_circuits(c)
    return c


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_circuits._get_conn", return_value=mock_conn)


def test_status_no_data(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(circuits_cmd, ["status", "backup"])
    assert result.exit_code == 0
    assert "closed" in result.output


def test_status_json_no_data(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(circuits_cmd, ["status", "backup", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["state"] == "closed"


def test_status_open_circuit(runner, mock_conn):
    for _ in range(3):
        circuits.record_failure(mock_conn, "backup")
    with _patch_conn(mock_conn):
        result = runner.invoke(circuits_cmd, ["status", "backup"])
    assert result.exit_code == 0
    assert "open" in result.output


def test_reset_circuit(runner, mock_conn):
    circuits.record_failure(mock_conn, "backup")
    with _patch_conn(mock_conn):
        result = runner.invoke(circuits_cmd, ["reset", "backup"])
    assert result.exit_code == 0
    assert "reset" in result.output.lower()
    assert circuits.get_circuit(mock_conn, "backup") is None


def test_list_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(circuits_cmd, ["list"])
    assert result.exit_code == 0
    assert "No circuit" in result.output


def test_list_json(runner, mock_conn):
    circuits.record_failure(mock_conn, "job_a")
    circuits.record_failure(mock_conn, "job_b")
    with _patch_conn(mock_conn):
        result = runner.invoke(circuits_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    names = [r["job_name"] for r in data]
    assert "job_a" in names
    assert "job_b" in names
