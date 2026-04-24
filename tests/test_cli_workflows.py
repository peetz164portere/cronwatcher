"""Tests for cronwatcher/cli_workflows.py"""

import json
import sqlite3
from unittest.mock import patch, MagicMock
import pytest
from click.testing import CliRunner
from cronwatcher.cli_workflows import workflows_cmd
from cronwatcher import workflows as wf_mod


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    c = sqlite3.connect(":memory:")
    wf_mod.init_workflows(c)
    return c


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_workflows._get_conn", return_value=mock_conn)


def test_create_workflow(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["create", "deploy", "--steps", '["build","test"]'])
    assert result.exit_code == 0
    assert "Created workflow 'deploy'" in result.output


def test_create_workflow_invalid_json(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["create", "deploy", "--steps", "not-json"])
    assert result.exit_code != 0
    assert "valid JSON" in result.output


def test_show_workflow(runner, mock_conn):
    wf_mod.create_workflow(mock_conn, "backup", "nightly", ["dump", "upload"])
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["show", "backup"])
    assert result.exit_code == 0
    assert "backup" in result.output
    assert "dump" in result.output


def test_show_workflow_json(runner, mock_conn):
    wf_mod.create_workflow(mock_conn, "backup", steps=["dump"])
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["show", "backup", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["name"] == "backup"


def test_show_missing_workflow_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["show", "ghost"])
    assert result.exit_code == 1


def test_remove_workflow(runner, mock_conn):
    wf_mod.create_workflow(mock_conn, "cleanup")
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["remove", "cleanup"])
    assert result.exit_code == 0
    assert "Removed" in result.output


def test_remove_missing_workflow_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["remove", "ghost"])
    assert result.exit_code == 1


def test_list_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["list"])
    assert result.exit_code == 0
    assert "No workflows" in result.output


def test_list_multiple(runner, mock_conn):
    wf_mod.create_workflow(mock_conn, "alpha")
    wf_mod.create_workflow(mock_conn, "beta")
    with _patch_conn(mock_conn):
        result = runner.invoke(workflows_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 2
