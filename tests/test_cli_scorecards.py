"""Tests for cronwatcher/cli_scorecards.py."""

import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from cronwatcher.cli_scorecards import scorecards_cmd


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    from cronwatcher.storage import init_db
    from cronwatcher.scorecards import init_scorecards
    init_db(conn)
    init_scorecards(conn)
    return conn


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_scorecards._get_conn", return_value=mock_conn)


def test_refresh_outputs_score(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(scorecards_cmd, ["refresh", "backup"])
    assert result.exit_code == 0
    assert "100" in result.output


def test_show_missing_job_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(scorecards_cmd, ["show", "nonexistent"])
    assert result.exit_code == 1


def test_show_existing_job(runner, mock_conn):
    from cronwatcher.scorecards import refresh_scorecard
    refresh_scorecard(mock_conn, "deploy")
    with _patch_conn(mock_conn):
        result = runner.invoke(scorecards_cmd, ["show", "deploy"])
    assert result.exit_code == 0
    assert "deploy" in result.output
    assert "100" in result.output


def test_show_json_output(runner, mock_conn):
    from cronwatcher.scorecards import refresh_scorecard
    refresh_scorecard(mock_conn, "sync")
    with _patch_conn(mock_conn):
        result = runner.invoke(scorecards_cmd, ["show", "sync", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["job_name"] == "sync"
    assert "score" in data


def test_list_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(scorecards_cmd, ["list"])
    assert result.exit_code == 0
    assert "No scorecards" in result.output


def test_list_shows_entries(runner, mock_conn):
    from cronwatcher.scorecards import refresh_scorecard
    refresh_scorecard(mock_conn, "job-a")
    refresh_scorecard(mock_conn, "job-b")
    with _patch_conn(mock_conn):
        result = runner.invoke(scorecards_cmd, ["list"])
    assert result.exit_code == 0
    assert "job-a" in result.output
    assert "job-b" in result.output


def test_list_json_output(runner, mock_conn):
    from cronwatcher.scorecards import refresh_scorecard
    refresh_scorecard(mock_conn, "myjob")
    with _patch_conn(mock_conn):
        result = runner.invoke(scorecards_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["job_name"] == "myjob"
