"""Tests for cronwatcher.cli_filters."""

import json
import sqlite3
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cronwatcher.cli_filters import filters_cmd
from cronwatcher import filters as flt


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    c = sqlite3.connect(":memory:")
    flt.init_filters(c)
    return c


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_filters._get_conn", return_value=mock_conn)


def test_save_filter(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["save", "myf", "--job", "backup", "--status", "failure"])
    assert result.exit_code == 0
    assert "saved" in result.output
    params = flt.get_filter(mock_conn, "myf")
    assert params["job"] == "backup"
    assert params["status"] == "failure"


def test_save_filter_no_params_fails(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["save", "empty"])
    assert result.exit_code != 0


def test_show_filter(runner, mock_conn):
    flt.save_filter(mock_conn, "view", {"status": "success", "limit": 5})
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["show", "view"])
    assert result.exit_code == 0
    assert "status" in result.output


def test_show_filter_json(runner, mock_conn):
    flt.save_filter(mock_conn, "jview", {"job": "deploy"})
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["show", "jview", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["job"] == "deploy"


def test_show_missing_filter_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["show", "ghost"])
    assert result.exit_code == 1


def test_remove_filter(runner, mock_conn):
    flt.save_filter(mock_conn, "todelete", {"status": "failure"})
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["remove", "todelete"])
    assert result.exit_code == 0
    assert flt.get_filter(mock_conn, "todelete") is None


def test_remove_missing_filter_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["remove", "nope"])
    assert result.exit_code == 1


def test_list_filters_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["list"])
    assert result.exit_code == 0
    assert "No saved filters" in result.output


def test_list_filters_json(runner, mock_conn):
    flt.save_filter(mock_conn, "f1", {"status": "success"})
    flt.save_filter(mock_conn, "f2", {"job": "sync"})
    with _patch_conn(mock_conn):
        result = runner.invoke(filters_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 2
