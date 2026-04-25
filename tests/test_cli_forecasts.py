"""Tests for cronwatcher/cli_forecasts.py"""

import json
import sqlite3
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from datetime import datetime
from cronwatcher.cli_forecasts import forecasts_cmd


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    return MagicMock()


def _patch_conn(conn):
    return patch("cronwatcher.cli_forecasts._get_conn", return_value=conn)


def test_refresh_no_data_exits_1(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_forecasts.compute_forecast", return_value=None):
            result = runner.invoke(forecasts_cmd, ["refresh", "ghost-job"])
    assert result.exit_code == 1
    assert "No data" in result.output


def test_refresh_saves_forecast(runner):
    conn = MagicMock()
    fake_forecast = {
        "job_name": "backup",
        "predicted_duration_s": 12.5,
        "predicted_next_run": None,
        "confidence": "high",
        "sample_size": 10,
    }
    with _patch_conn(conn):
        with patch("cronwatcher.cli_forecasts.compute_forecast", return_value=fake_forecast):
            with patch("cronwatcher.cli_forecasts.save_forecast") as mock_save:
                result = runner.invoke(forecasts_cmd, ["refresh", "backup"])
    assert result.exit_code == 0
    assert "12.5" in result.output
    mock_save.assert_called_once()


def test_show_missing_job_exits_1(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_forecasts.get_forecast", return_value=None):
            result = runner.invoke(forecasts_cmd, ["show", "nope"])
    assert result.exit_code == 1


def test_show_existing_job(runner):
    conn = MagicMock()
    fake = {
        "job_name": "nightly",
        "predicted_duration_s": 45.0,
        "predicted_next_run": "2024-01-01T03:00:00",
        "confidence": "medium",
        "updated_at": datetime.utcnow().isoformat(),
    }
    with _patch_conn(conn):
        with patch("cronwatcher.cli_forecasts.get_forecast", return_value=fake):
            result = runner.invoke(forecasts_cmd, ["show", "nightly"])
    assert result.exit_code == 0
    assert "45.0" in result.output
    assert "medium" in result.output


def test_show_json_output(runner):
    conn = MagicMock()
    fake = {
        "job_name": "nightly",
        "predicted_duration_s": 45.0,
        "predicted_next_run": None,
        "confidence": "high",
        "updated_at": "2024-01-01T00:00:00",
    }
    with _patch_conn(conn):
        with patch("cronwatcher.cli_forecasts.get_forecast", return_value=fake):
            result = runner.invoke(forecasts_cmd, ["show", "nightly", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["job_name"] == "nightly"


def test_list_empty(runner):
    conn = MagicMock()
    with _patch_conn(conn):
        with patch("cronwatcher.cli_forecasts.list_forecasts", return_value=[]):
            result = runner.invoke(forecasts_cmd, ["list"])
    assert result.exit_code == 0
    assert "No forecasts" in result.output


def test_list_json_output(runner):
    conn = MagicMock()
    rows = [
        {"job_name": "alpha", "predicted_duration_s": 10.0, "predicted_next_run": None, "confidence": "high", "updated_at": "x"},
    ]
    with _patch_conn(conn):
        with patch("cronwatcher.cli_forecasts.list_forecasts", return_value=rows):
            result = runner.invoke(forecasts_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data[0]["job_name"] == "alpha"
