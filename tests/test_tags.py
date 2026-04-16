import pytest
import sqlite3
from cronwatcher.tags import init_tags, add_tag, remove_tag, get_tags, get_jobs_by_tag, clear_tags


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_tags(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='job_tags'")
    assert cur.fetchone() is not None


def test_add_and_get_tags(conn):
    add_tag(conn, "backup", "critical")
    add_tag(conn, "backup", "nightly")
    tags = get_tags(conn, "backup")
    assert tags == ["critical", "nightly"]


def test_add_tag_duplicate_ignored(conn):
    add_tag(conn, "backup", "critical")
    add_tag(conn, "backup", "critical")
    assert get_tags(conn, "backup") == ["critical"]


def test_add_tag_normalizes_case(conn):
    add_tag(conn, "backup", "Critical")
    assert get_tags(conn, "backup") == ["critical"]


def test_remove_tag(conn):
    add_tag(conn, "backup", "critical")
    add_tag(conn, "backup", "nightly")
    remove_tag(conn, "backup", "critical")
    assert get_tags(conn, "backup") == ["nightly"]


def test_remove_nonexistent_tag_ok(conn):
    remove_tag(conn, "backup", "ghost")  # should not raise


def test_get_jobs_by_tag(conn):
    add_tag(conn, "backup", "critical")
    add_tag(conn, "deploy", "critical")
    add_tag(conn, "cleanup", "nightly")
    jobs = get_jobs_by_tag(conn, "critical")
    assert jobs == ["backup", "deploy"]


def test_get_tags_empty(conn):
    assert get_tags(conn, "nonexistent") == []


def test_clear_tags(conn):
    add_tag(conn, "backup", "critical")
    add_tag(conn, "backup", "nightly")
    clear_tags(conn, "backup")
    assert get_tags(conn, "backup") == []


def test_clear_tags_does_not_affect_other_jobs(conn):
    add_tag(conn, "backup", "critical")
    add_tag(conn, "deploy", "critical")
    clear_tags(conn, "backup")
    assert get_jobs_by_tag(conn, "critical") == ["deploy"]
