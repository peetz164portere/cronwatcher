import pytest
import sqlite3
from cronwatcher.storage import init_db
from cronwatcher.dependencies import (
    init_dependencies, add_dependency, remove_dependency,
    get_dependencies, get_dependents, check_ready,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_dependencies(c)
    return c


def test_init_creates_table(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='job_dependencies'")
    assert cur.fetchone() is not None


def test_add_and_get_dependency(conn):
    add_dependency(conn, "backup", "db-dump")
    deps = get_dependencies(conn, "backup")
    assert deps == ["db-dump"]


def test_add_dependency_normalizes_case(conn):
    add_dependency(conn, "Backup", "DB-Dump")
    deps = get_dependencies(conn, "backup")
    assert "db-dump" in deps


def test_add_duplicate_ignored(conn):
    add_dependency(conn, "a", "b")
    add_dependency(conn, "a", "b")  # should not raise
    assert len(get_dependencies(conn, "a")) == 1


def test_self_dependency_raises(conn):
    with pytest.raises(ValueError):
        add_dependency(conn, "job", "job")


def test_remove_dependency(conn):
    add_dependency(conn, "a", "b")
    removed = remove_dependency(conn, "a", "b")
    assert removed is True
    assert get_dependencies(conn, "a") == []


def test_remove_nonexistent_returns_false(conn):
    assert remove_dependency(conn, "x", "y") is False


def test_get_dependents(conn):
    add_dependency(conn, "child", "parent")
    add_dependency(conn, "other-child", "parent")
    dependents = get_dependents(conn, "parent")
    assert "child" in dependents
    assert "other-child" in dependents


def test_check_ready_no_deps(conn):
    result = check_ready(conn, "standalone")
    assert result["ready"] is True
    assert result["blocking"] == []


def test_check_ready_blocked(conn):
    add_dependency(conn, "report", "etl")
    result = check_ready(conn, "report")
    assert result["ready"] is False
    assert "etl" in result["blocking"]


def test_check_ready_after_success(conn):
    add_dependency(conn, "report", "etl")
    conn.execute(
        "INSERT INTO runs (job_name, status, started_at, finished_at, duration) VALUES (?, 'success', datetime('now'), datetime('now'), 1.0)",
        ("etl",),
    )
    conn.commit()
    result = check_ready(conn, "report")
    assert result["ready"] is True
