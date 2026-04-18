import sqlite3
import pytest
from cronwatcher.env import init_env_log, capture_env, save_env, get_env


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_env_log(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    names = [t[0] for t in tables]
    assert "run_env" in names


def test_capture_env_has_required_keys():
    data = capture_env()
    assert "hostname" in data
    assert "user" in data
    assert "env_vars" in data
    assert isinstance(data["env_vars"], dict)


def test_capture_env_extra_keys(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "abc123")
    data = capture_env(extra_keys=["MY_TOKEN", "MISSING_KEY"])
    assert data["env_vars"]["MY_TOKEN"] == "abc123"
    assert "MISSING_KEY" not in data["env_vars"]


def test_save_and_get_env(conn):
    env_data = {"hostname": "myhost", "user": "alice", "env_vars": {"FOO": "bar"}}
    save_env(conn, run_id=42, env_data=env_data)
    result = get_env(conn, run_id=42)
    assert result["hostname"] == "myhost"
    assert result["user"] == "alice"
    assert result["env_vars"] == {"FOO": "bar"}


def test_get_env_returns_none_for_missing(conn):
    assert get_env(conn, run_id=999) is None


def test_save_env_empty_vars(conn):
    env_data = {"hostname": "h", "user": "u", "env_vars": {}}
    save_env(conn, run_id=1, env_data=env_data)
    result = get_env(conn, run_id=1)
    assert result["env_vars"] == {}


def test_multiple_runs_isolated(conn):
    save_env(conn, 1, {"hostname": "h1", "user": "u1", "env_vars": {}})
    save_env(conn, 2, {"hostname": "h2", "user": "u2", "env_vars": {}})
    assert get_env(conn, 1)["hostname"] == "h1"
    assert get_env(conn, 2)["hostname"] == "h2"
