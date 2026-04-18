import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cronwatcher.cli_dependencies import deps_cmd


@pytest.fixture
def runner():
    return CliRunner()


def _mock_conn():
    import sqlite3
    from cronwatcher.storage import init_db
    from cronwatcher.dependencies import init_dependencies
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_dependencies(c)
    return c


def test_add_dependency(runner):
    conn = _mock_conn()
    with patch("cronwatcher.cli_dependencies._get_conn", return_value=conn):
        result = runner.invoke(deps_cmd, ["add", "report", "etl"])
    assert result.exit_code == 0
    assert "report depends on etl" in result.output


def test_add_self_dependency_fails(runner):
    conn = _mock_conn()
    with patch("cronwatcher.cli_dependencies._get_conn", return_value=conn):
        result = runner.invoke(deps_cmd, ["add", "job", "job"])
    assert result.exit_code == 1


def test_remove_existing(runner):
    conn = _mock_conn()
    from cronwatcher.dependencies import add_dependency
    add_dependency(conn, "a", "b")
    with patch("cronwatcher.cli_dependencies._get_conn", return_value=conn):
        result = runner.invoke(deps_cmd, ["remove", "a", "b"])
    assert result.exit_code == 0
    assert "no longer depends on" in result.output


def test_remove_missing_fails(runner):
    conn = _mock_conn()
    with patch("cronwatcher.cli_dependencies._get_conn", return_value=conn):
        result = runner.invoke(deps_cmd, ["remove", "x", "y"])
    assert result.exit_code == 1


def test_list_cmd(runner):
    conn = _mock_conn()
    from cronwatcher.dependencies import add_dependency
    add_dependency(conn, "child", "parent")
    with patch("cronwatcher.cli_dependencies._get_conn", return_value=conn):
        result = runner.invoke(deps_cmd, ["list", "child"])
    assert "parent" in result.output


def test_check_ready(runner):
    conn = _mock_conn()
    with patch("cronwatcher.cli_dependencies._get_conn", return_value=conn):
        result = runner.invoke(deps_cmd, ["check", "standalone"])
    assert result.exit_code == 0
    assert "ready" in result.output


def test_check_blocked(runner):
    conn = _mock_conn()
    from cronwatcher.dependencies import add_dependency
    add_dependency(conn, "report", "etl")
    with patch("cronwatcher.cli_dependencies._get_conn", return_value=conn):
        result = runner.invoke(deps_cmd, ["check", "report"])
    assert result.exit_code == 1
    assert "blocked" in result.output
