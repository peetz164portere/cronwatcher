"""Tests for cronwatcher/cli_pauses.py"""

import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from cronwatcher.cli_pauses import pauses_cmd
from cronwatcher import pauses


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    pauses.init_pauses(conn)
    return conn


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_pauses._get_conn", return_value=mock_conn)


def test_pause_job(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["pause", "backup"])
    assert result.exit_code == 0
    assert "Paused" in result.output
    assert pauses.is_paused(mock_conn, "backup")


def test_pause_job_with_reason(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["pause", "backup", "--reason", "deploy"])
    assert result.exit_code == 0
    assert "deploy" in result.output


def test_resume_job(runner, mock_conn):
    pauses.pause_job(mock_conn, "backup")
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["resume", "backup"])
    assert result.exit_code == 0
    assert "Resumed" in result.output


def test_resume_not_paused_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["resume", "ghost"])
    assert result.exit_code == 1


def test_status_paused(runner, mock_conn):
    pauses.pause_job(mock_conn, "myjob", reason="testing")
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["status", "myjob"])
    assert result.exit_code == 0
    assert "PAUSED" in result.output
    assert "testing" in result.output


def test_status_not_paused(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["status", "myjob"])
    assert result.exit_code == 0
    assert "not paused" in result.output


def test_list_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["list"])
    assert result.exit_code == 0
    assert "No jobs" in result.output


def test_list_shows_paused_jobs(runner, mock_conn):
    pauses.pause_job(mock_conn, "job_a")
    pauses.pause_job(mock_conn, "job_b")
    with _patch_conn(mock_conn):
        result = runner.invoke(pauses_cmd, ["list"])
    assert result.exit_code == 0
    assert "job_a" in result.output
    assert "job_b" in result.output
