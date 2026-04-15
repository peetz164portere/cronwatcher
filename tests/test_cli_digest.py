"""Tests for the digest CLI command."""

import json
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cronwatcher.cli_digest import digest_cmd


FAKE_DIGEST = {
    "period_hours": 24,
    "generated_at": "2024-06-01T12:00:00",
    "total_runs": 5,
    "successful_runs": 4,
    "failed_runs": 1,
    "running_runs": 0,
    "failure_rate": 20.0,
    "jobs": {"backup": {"total": 3, "failures": 1}},
}


@pytest.fixture
def runner():
    return CliRunner()


@patch("cronwatcher.cli_digest.build_digest", return_value=FAKE_DIGEST)
@patch("cronwatcher.cli_digest.load_config", return_value={"db_path": ":memory:"})
def test_digest_text_output(mock_cfg, mock_build, runner):
    result = runner.invoke(digest_cmd, ["--hours", "24"])
    assert result.exit_code == 0
    assert "Digest" in result.output
    assert "20.0%" in result.output


@patch("cronwatcher.cli_digest.build_digest", return_value=FAKE_DIGEST)
@patch("cronwatcher.cli_digest.load_config", return_value={"db_path": ":memory:"})
def test_digest_json_output(mock_cfg, mock_build, runner):
    result = runner.invoke(digest_cmd, ["--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["total_runs"] == 5
    assert data["failure_rate"] == 20.0


@patch("cronwatcher.cli_digest.send_webhook", return_value=True)
@patch("cronwatcher.cli_digest.build_digest", return_value=FAKE_DIGEST)
@patch("cronwatcher.cli_digest.load_config", return_value={"db_path": ":memory:", "webhook_url": "http://hook"})
def test_digest_send_webhook_success(mock_cfg, mock_build, mock_send, runner):
    result = runner.invoke(digest_cmd, ["--send"])
    assert result.exit_code == 0
    assert "sent to webhook" in result.output
    mock_send.assert_called_once()
    payload = mock_send.call_args[0][1]
    assert payload["event"] == "digest"


@patch("cronwatcher.cli_digest.send_webhook", return_value=False)
@patch("cronwatcher.cli_digest.build_digest", return_value=FAKE_DIGEST)
@patch("cronwatcher.cli_digest.load_config", return_value={"db_path": ":memory:", "webhook_url": "http://hook"})
def test_digest_send_webhook_failure(mock_cfg, mock_build, mock_send, runner):
    result = runner.invoke(digest_cmd, ["--send"])
    assert result.exit_code == 0
    assert "Failed" in result.output


@patch("cronwatcher.cli_digest.build_digest", return_value=FAKE_DIGEST)
@patch("cronwatcher.cli_digest.load_config", return_value={"db_path": ":memory:"})
def test_digest_send_no_webhook_url(mock_cfg, mock_build, runner):
    result = runner.invoke(digest_cmd, ["--send"])
    assert result.exit_code == 0
    assert "No webhook_url" in result.output


@patch("cronwatcher.cli_digest.build_digest", return_value=FAKE_DIGEST)
@patch("cronwatcher.cli_digest.load_config", return_value={"db_path": ":memory:"})
def test_digest_custom_hours(mock_cfg, mock_build, runner):
    result = runner.invoke(digest_cmd, ["--hours", "48"])
    assert result.exit_code == 0
    mock_build.assert_called_once_with(":memory:", hours=48)


@patch("cronwatcher.cli_digest.build_digest", return_value=FAKE_DIGEST)
@patch("cronwatcher.cli_digest.load_config", return_value={"db_path": ":memory:"})
def test_digest_custom_db(mock_cfg, mock_build, runner):
    result = runner.invoke(digest_cmd, ["--db", "/tmp/test.db"])
    assert result.exit_code == 0
    mock_build.assert_called_once_with("/tmp/test.db", hours=24)
