"""Tests for cronwatcher/cli_quotas.py"""

import sqlite3
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cronwatcher.cli_quotas import quotas_cmd


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    return conn


def _patch_conn(conn):
    return patch("cronwatcher.cli_quotas._get_conn", return_value=conn)


def test_set_quota(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_quotas.set_quota") as mock_set:
            result = runner.invoke(quotas_cmd, ["set", "backup", "--max-runs", "5", "--window", "3600"])
            assert result.exit_code == 0
            mock_set.assert_called_once_with(conn, "backup", 5, 3600)
            assert "Quota set" in result.output


def test_remove_quota(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_quotas.remove_quota") as mock_rm:
            result = runner.invoke(quotas_cmd, ["remove", "backup"])
            assert result.exit_code == 0
            mock_rm.assert_called_once_with(conn, "backup")


def test_show_quota_exists(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_quotas.get_quota", return_value={"job_name": "backup", "max_runs": 5, "window_seconds": 3600}):
            with patch("cronwatcher.cli_quotas.is_quota_exceeded", return_value=False):
                result = runner.invoke(quotas_cmd, ["show", "backup"])
                assert result.exit_code == 0
                assert "backup" in result.output
                assert "ok" in result.output


def test_show_quota_missing_exits_1(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_quotas.get_quota", return_value=None):
            result = runner.invoke(quotas_cmd, ["show", "ghost"])
            assert result.exit_code == 1


def test_show_quota_exceeded(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_quotas.get_quota", return_value={"job_name": "backup", "max_runs": 2, "window_seconds": 60}):
            with patch("cronwatcher.cli_quotas.is_quota_exceeded", return_value=True):
                result = runner.invoke(quotas_cmd, ["show", "backup"])
                assert "EXCEEDED" in result.output


def test_list_quotas_empty(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_quotas.list_quotas", return_value=[]):
            result = runner.invoke(quotas_cmd, ["list"])
            assert result.exit_code == 0
            assert "No quotas" in result.output


def test_list_quotas_shows_entries(runner):
    conn = MagicMock()
    entries = [
        {"job_name": "job_a", "max_runs": 3, "window_seconds": 600},
        {"job_name": "job_b", "max_runs": 10, "window_seconds": 86400},
    ]
    with _patch_conn(conn):
        with patch("cronwatcher.cli_quotas.list_quotas", return_value=entries):
            result = runner.invoke(quotas_cmd, ["list"])
            assert "job_a" in result.output
            assert "job_b" in result.output
