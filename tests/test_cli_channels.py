"""Tests for cronwatcher.cli_channels."""
import json
import sqlite3
from unittest.mock import patch, MagicMock
import pytest
from click.testing import CliRunner
from cronwatcher.cli_channels import channels_cmd
from cronwatcher.channels import init_channels, add_channel


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    init_channels(conn)
    return conn


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_channels._get_conn", return_value=mock_conn)


def test_add_channel(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(channels_cmd, ["add", "my-slack", "slack", "--config", '{"url": "https://x"}'])
    assert result.exit_code == 0
    assert "added" in result.output


def test_add_channel_invalid_json(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(channels_cmd, ["add", "bad", "slack", "--config", "not-json"])
    assert result.exit_code != 0


def test_remove_existing_channel(runner, mock_conn):
    add_channel(mock_conn, "to-del", "email")
    with _patch_conn(mock_conn):
        result = runner.invoke(channels_cmd, ["remove", "to-del"])
    assert result.exit_code == 0
    assert "removed" in result.output


def test_remove_missing_channel_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(channels_cmd, ["remove", "ghost"])
    assert result.exit_code == 1


def test_list_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(channels_cmd, ["list"])
    assert result.exit_code == 0
    assert "No channels" in result.output


def test_list_json_output(runner, mock_conn):
    add_channel(mock_conn, "ch1", "webhook")
    with _patch_conn(mock_conn):
        result = runner.invoke(channels_cmd, ["list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["name"] == "ch1"


def test_enable_disable(runner, mock_conn):
    add_channel(mock_conn, "ch2", "slack")
    with _patch_conn(mock_conn):
        runner.invoke(channels_cmd, ["disable", "ch2"])
        runner.invoke(channels_cmd, ["enable", "ch2"])
    from cronwatcher.channels import get_channel
    assert get_channel(mock_conn, "ch2")["enabled"] is True
