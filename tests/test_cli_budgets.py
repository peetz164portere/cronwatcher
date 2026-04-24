"""Tests for cronwatcher/cli_budgets.py"""

import sqlite3
from unittest.mock import patch, MagicMock
import pytest
from click.testing import CliRunner
from cronwatcher.cli_budgets import budgets_cmd
from cronwatcher import budgets as bmod


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    bmod.init_budgets(conn)
    return conn


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_budgets._get_conn", return_value=mock_conn)


def test_set_budget(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["set", "my_job", "120", "--action", "warn"])
    assert result.exit_code == 0
    assert "my_job" in result.output
    assert "120" in result.output


def test_set_budget_invalid_action(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["set", "my_job", "60", "--action", "nuke"])
    assert result.exit_code != 0


def test_set_budget_zero_seconds(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["set", "my_job", "0"])
    assert result.exit_code == 1
    assert "Error" in result.output


def test_show_existing_budget(runner, mock_conn):
    bmod.set_budget(mock_conn, "report", 300.0, "alert")
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["show", "report"])
    assert result.exit_code == 0
    assert "300" in result.output
    assert "alert" in result.output


def test_show_missing_budget_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["show", "ghost"])
    assert result.exit_code == 1


def test_remove_existing_budget(runner, mock_conn):
    bmod.set_budget(mock_conn, "cleanup", 60.0)
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["remove", "cleanup"])
    assert result.exit_code == 0
    assert "removed" in result.output


def test_remove_missing_budget_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["remove", "nobody"])
    assert result.exit_code == 1


def test_list_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["list"])
    assert result.exit_code == 0
    assert "No budgets" in result.output


def test_list_shows_entries(runner, mock_conn):
    bmod.set_budget(mock_conn, "alpha", 30.0, "warn")
    bmod.set_budget(mock_conn, "beta", 90.0, "alert")
    with _patch_conn(mock_conn):
        result = runner.invoke(budgets_cmd, ["list"])
    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" in result.output
