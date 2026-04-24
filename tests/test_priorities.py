import sqlite3
import pytest
from cronwatcher.priorities import (
    init_priorities,
    set_priority,
    get_priority,
    remove_priority,
    list_priorities,
    is_high_priority,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_priorities(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='priorities'"
    ).fetchone()
    assert row is not None


def test_get_priority_default_normal(conn):
    assert get_priority(conn, "backup") == "normal"


def test_set_and_get_priority(conn):
    set_priority(conn, "backup", "high")
    assert get_priority(conn, "backup") == "high"


def test_set_priority_normalizes_case(conn):
    set_priority(conn, "BACKUP", "Critical")
    assert get_priority(conn, "backup") == "critical"


def test_set_priority_invalid_level_raises(conn):
    with pytest.raises(ValueError, match="Invalid priority level"):
        set_priority(conn, "backup", "urgent")


def test_set_priority_overwrites_existing(conn):
    set_priority(conn, "backup", "low")
    set_priority(conn, "backup", "critical")
    assert get_priority(conn, "backup") == "critical"


def test_remove_priority_returns_true(conn):
    set_priority(conn, "backup", "high")
    result = remove_priority(conn, "backup")
    assert result is True
    assert get_priority(conn, "backup") == "normal"


def test_remove_priority_missing_returns_false(conn):
    result = remove_priority(conn, "nonexistent")
    assert result is False


def test_list_priorities_empty(conn):
    assert list_priorities(conn) == []


def test_list_priorities_returns_all(conn):
    set_priority(conn, "alpha", "low")
    set_priority(conn, "beta", "critical")
    rows = list_priorities(conn)
    assert len(rows) == 2
    names = [r["job_name"] for r in rows]
    assert "alpha" in names
    assert "beta" in names


def test_list_priorities_has_expected_keys(conn):
    set_priority(conn, "myjob", "high")
    row = list_priorities(conn)[0]
    assert "job_name" in row
    assert "level" in row
    assert "updated_at" in row


def test_is_high_priority_true_for_high(conn):
    set_priority(conn, "myjob", "high")
    assert is_high_priority(conn, "myjob") is True


def test_is_high_priority_true_for_critical(conn):
    set_priority(conn, "myjob", "critical")
    assert is_high_priority(conn, "myjob") is True


def test_is_high_priority_false_for_low(conn):
    set_priority(conn, "myjob", "low")
    assert is_high_priority(conn, "myjob") is False


def test_is_high_priority_false_for_default(conn):
    assert is_high_priority(conn, "unset_job") is False
