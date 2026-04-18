import sqlite3
import pytest
from cronwatcher.labels import (
    init_labels, set_label, get_label, get_labels,
    remove_label, get_jobs_by_label
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_labels(c)
    return c


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "labels" in tables


def test_set_and_get_label(conn):
    set_label(conn, "backup", "env", "prod")
    assert get_label(conn, "backup", "env") == "prod"


def test_set_label_normalizes_key_case(conn):
    set_label(conn, "backup", "ENV", "prod")
    assert get_label(conn, "backup", "env") == "prod"


def test_get_label_missing_returns_none(conn):
    assert get_label(conn, "backup", "missing") is None


def test_set_label_overwrites_existing(conn):
    set_label(conn, "backup", "env", "prod")
    set_label(conn, "backup", "env", "staging")
    assert get_label(conn, "backup", "env") == "staging"


def test_get_labels_returns_all(conn):
    set_label(conn, "backup", "env", "prod")
    set_label(conn, "backup", "team", "ops")
    labels = get_labels(conn, "backup")
    assert labels == {"env": "prod", "team": "ops"}


def test_get_labels_empty(conn):
    assert get_labels(conn, "nonexistent") == {}


def test_remove_label_returns_true(conn):
    set_label(conn, "backup", "env", "prod")
    assert remove_label(conn, "backup", "env") is True
    assert get_label(conn, "backup", "env") is None


def test_remove_label_missing_returns_false(conn):
    assert remove_label(conn, "backup", "nope") is False


def test_get_jobs_by_label(conn):
    set_label(conn, "backup", "env", "prod")
    set_label(conn, "sync", "env", "prod")
    set_label(conn, "cleanup", "env", "staging")
    jobs = get_jobs_by_label(conn, "env", "prod")
    assert sorted(jobs) == ["backup", "sync"]


def test_get_jobs_by_label_no_match(conn):
    assert get_jobs_by_label(conn, "env", "dev") == []
