import sqlite3
import pytest
from cronwatcher.profiles import init_profiles, set_profile, get_profile, remove_profile, list_profiles


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_profiles(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [t[0] for t in tables]
    assert "job_profiles" in names


def test_set_and_get_profile(conn):
    set_profile(conn, "backup", "critical")
    result = get_profile(conn, "backup")
    assert result is not None
    assert result["profile"] == "critical"
    assert result["options"] == {}


def test_set_profile_normalizes_case(conn):
    set_profile(conn, "Backup", "Critical")
    result = get_profile(conn, "BACKUP")
    assert result["job_name"] == "backup"
    assert result["profile"] == "critical"


def test_set_profile_with_options(conn):
    set_profile(conn, "sync", "standard", {"retries": 3, "timeout": 60})
    result = get_profile(conn, "sync")
    assert result["options"] == {"retries": 3, "timeout": 60}


def test_set_profile_overwrites_existing(conn):
    set_profile(conn, "sync", "standard")
    set_profile(conn, "sync", "critical", {"retries": 5})
    result = get_profile(conn, "sync")
    assert result["profile"] == "critical"
    assert result["options"]["retries"] == 5


def test_get_profile_missing_returns_none(conn):
    assert get_profile(conn, "nonexistent") is None


def test_remove_profile_returns_true(conn):
    set_profile(conn, "cleanup", "low")
    removed = remove_profile(conn, "cleanup")
    assert removed is True
    assert get_profile(conn, "cleanup") is None


def test_remove_profile_missing_returns_false(conn):
    assert remove_profile(conn, "ghost") is False


def test_list_profiles_empty(conn):
    assert list_profiles(conn) == []


def test_list_profiles_returns_all(conn):
    set_profile(conn, "job_a", "critical")
    set_profile(conn, "job_b", "low")
    results = list_profiles(conn)
    assert len(results) == 2
    names = [r["job_name"] for r in results]
    assert "job_a" in names
    assert "job_b" in names
