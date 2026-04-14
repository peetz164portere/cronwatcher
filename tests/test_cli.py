import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cronwatcher.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def mock_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("CRONWATCHER_DB", db_path)
    from cronwatcher import storage
    monkeypatch.setattr(storage, "DB_PATH", db_path)
    storage.init_db()


def test_run_success(runner):
    result = runner.invoke(cli, ["run", "test-job", "echo hello"])
    assert result.exit_code == 0
    assert "Starting job 'test-job'" in result.output
    assert "succeeded" in result.output


def test_run_failure(runner):
    result = runner.invoke(cli, ["run", "fail-job", "exit 1"], catch_exceptions=False)
    assert result.exit_code == 1
    assert "FAILED" in result.output


def test_run_failure_triggers_webhook(runner):
    with patch("cronwatcher.cli.load_config") as mock_cfg, \
         patch("cronwatcher.cli.should_alert", return_value=True) as mock_alert, \
         patch("cronwatcher.cli.notify_failure") as mock_notify:
        mock_cfg.return_value = {"webhook_url": "http://example.com/hook"}
        result = runner.invoke(cli, ["run", "fail-job", "exit 1"])
        assert mock_notify.called
        args = mock_notify.call_args[0]
        assert args[1] == "fail-job"


def test_run_success_no_webhook(runner):
    with patch("cronwatcher.cli.notify_failure") as mock_notify:
        runner.invoke(cli, ["run", "ok-job", "echo ok"])
        mock_notify.assert_not_called()


def test_history_empty(runner):
    result = runner.invoke(cli, ["history"])
    assert result.exit_code == 0
    assert "No history found" in result.output


def test_history_shows_records(runner):
    runner.invoke(cli, ["run", "my-job", "echo hi"])
    result = runner.invoke(cli, ["history"])
    assert result.exit_code == 0
    assert "my-job" in result.output


def test_history_filter_by_job(runner):
    runner.invoke(cli, ["run", "job-a", "echo a"])
    runner.invoke(cli, ["run", "job-b", "echo b"])
    result = runner.invoke(cli, ["history", "job-a"])
    assert "job-a" in result.output
    assert "job-b" not in result.output


def test_history_limit(runner):
    for i in range(5):
        runner.invoke(cli, ["run", "loop-job", "echo x"])
    result = runner.invoke(cli, ["history", "--limit", "3"])
    lines = [l for l in result.output.splitlines() if "loop-job" in l]
    assert len(lines) == 3
