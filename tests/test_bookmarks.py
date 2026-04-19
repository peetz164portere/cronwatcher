import sqlite3
import pytest
from cronwatcher.bookmarks import (
    init_bookmarks, add_bookmark, remove_bookmark,
    get_bookmarks, list_all_bookmarks, is_bookmarked,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_bookmarks(c)
    return c


def test_init_creates_table(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bookmarks'")
    assert cur.fetchone() is not None


def test_add_bookmark_returns_id(conn):
    row_id = add_bookmark(conn, "myjob", 42)
    assert isinstance(row_id, int)
    assert row_id > 0


def test_add_bookmark_duplicate_ignored(conn):
    add_bookmark(conn, "myjob", 42)
    row_id2 = add_bookmark(conn, "myjob", 42)
    assert row_id2 == 0 or row_id2 is None or isinstance(row_id2, int)
    entries = get_bookmarks(conn, "myjob")
    assert len(entries) == 1


def test_add_bookmark_normalizes_case(conn):
    add_bookmark(conn, "MyJob", 1)
    entries = get_bookmarks(conn, "myjob")
    assert len(entries) == 1
    assert entries[0]["job_name"] == "myjob"


def test_get_bookmarks_empty(conn):
    assert get_bookmarks(conn, "nojob") == []


def test_get_bookmarks_returns_correct_job(conn):
    add_bookmark(conn, "job_a", 1, label="important")
    add_bookmark(conn, "job_b", 2)
    entries = get_bookmarks(conn, "job_a")
    assert len(entries) == 1
    assert entries[0]["run_id"] == 1
    assert entries[0]["label"] == "important"


def test_is_bookmarked_true(conn):
    add_bookmark(conn, "job", 5)
    assert is_bookmarked(conn, "job", 5) is True


def test_is_bookmarked_false(conn):
    assert is_bookmarked(conn, "job", 99) is False


def test_remove_bookmark_returns_true(conn):
    add_bookmark(conn, "job", 7)
    result = remove_bookmark(conn, "job", 7)
    assert result is True
    assert is_bookmarked(conn, "job", 7) is False


def test_remove_bookmark_missing_returns_false(conn):
    result = remove_bookmark(conn, "job", 999)
    assert result is False


def test_list_all_bookmarks(conn):
    add_bookmark(conn, "job_a", 1)
    add_bookmark(conn, "job_b", 2, label="x")
    all_entries = list_all_bookmarks(conn)
    assert len(all_entries) == 2
