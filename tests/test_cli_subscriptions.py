import json
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from cronwatcher.cli_subscriptions import subscriptions_cmd
from cronwatcher.subscriptions import init_subscriptions, add_subscription
import sqlite3


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    init_subscriptions(conn)
    return conn


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_subscriptions._get_conn", return_value=mock_conn)


def test_add_subscription(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(
            subscriptions_cmd,
            ["add", "backup", "failure", "https://hooks.example.com/1"],
        )
    assert result.exit_code == 0
    assert "added" in result.output


def test_add_subscription_with_headers(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(
            subscriptions_cmd,
            [
                "add", "backup", "success", "https://hooks.example.com/1",
                "--header", "Authorization=Bearer abc",
            ],
        )
    assert result.exit_code == 0
    rows = mock_conn.execute("SELECT headers FROM subscriptions").fetchone()
    assert json.loads(rows[0])["Authorization"] == "Bearer abc"


def test_add_subscription_invalid_event(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(
            subscriptions_cmd,
            ["add", "backup", "badvent", "https://hooks.example.com/1"],
        )
    assert result.exit_code != 0
    assert "Invalid event" in result.output


def test_list_subscriptions_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(subscriptions_cmd, ["list"])
    assert result.exit_code == 0
    assert "No subscriptions" in result.output


def test_list_subscriptions_json(runner, mock_conn):
    add_subscription(mock_conn, "backup", "failure", "https://hooks.example.com/1")
    with _patch_conn(mock_conn):
        result = runner.invoke(subscriptions_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 1
    assert data[0]["job_name"] == "backup"


def test_remove_subscription(runner, mock_conn):
    sid = add_subscription(mock_conn, "backup", "failure", "https://hooks.example.com/1")
    with _patch_conn(mock_conn):
        result = runner.invoke(subscriptions_cmd, ["remove", str(sid)])
    assert result.exit_code == 0
    assert "removed" in result.output


def test_remove_subscription_missing(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(subscriptions_cmd, ["remove", "9999"])
    assert result.exit_code != 0
    assert "No subscription" in result.output
