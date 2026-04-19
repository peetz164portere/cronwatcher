import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from cronwatcher.cli_bookmarks import bookmarks_cmd
from cronwatcher.bookmarks import init_bookmarks


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    init_bookmarks(conn)
    return conn


def _patch_conn(mock_conn):
    return patch("cronwatcher.cli_bookmarks._get_conn", return_value=mock_conn)


def test_add_bookmark(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(bookmarks_cmd, ["add", "myjob", "10", "--label", "deploy"])
    assert result.exit_code == 0
    assert "Bookmarked" in result.output


def test_add_bookmark_duplicate(runner, mock_conn):
    with _patch_conn(mock_conn):
        runner.invoke(bookmarks_cmd, ["add", "myjob", "10"])
        result = runner.invoke(bookmarks_cmd, ["add", "myjob", "10"])
    assert result.exit_code == 0
    assert "already exists" in result.output


def test_remove_existing(runner, mock_conn):
    from cronwatcher.bookmarks import add_bookmark
    add_bookmark(mock_conn, "myjob", 10)
    with _patch_conn(mock_conn):
        result = runner.invoke(bookmarks_cmd, ["remove", "myjob", "10"])
    assert result.exit_code == 0
    assert "Removed" in result.output


def test_remove_missing_exits_1(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(bookmarks_cmd, ["remove", "myjob", "999"])
    assert result.exit_code == 1


def test_list_empty(runner, mock_conn):
    with _patch_conn(mock_conn):
        result = runner.invoke(bookmarks_cmd, ["list"])
    assert result.exit_code == 0
    assert "No bookmarks" in result.output


def test_list_json_output(runner, mock_conn):
    from cronwatcher.bookmarks import add_bookmark
    add_bookmark(mock_conn, "myjob", 3, label="test")
    with _patch_conn(mock_conn):
        result = runner.invoke(bookmarks_cmd, ["list", "--json"])
    assert result.exit_code == 0
    import json
    data = json.loads(result.output)
    assert len(data) == 1
    assert data[0]["label"] == "test"
