import sqlite3
import pytest
from cronwatcher.notes import init_notes, set_note, get_note, remove_note, list_notes


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_notes(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    assert any("job_notes" in t for t in tables)


def test_set_note_returns_id(conn):
    rid = set_note(conn, "backup", "runs nightly")
    assert isinstance(rid, int) and rid > 0


def test_get_note_returns_dict(conn):
    set_note(conn, "backup", "runs nightly")
    result = get_note(conn, "backup")
    assert result is not None
    assert result["note"] == "runs nightly"
    assert "updated_at" in result


def test_get_note_missing_returns_none(conn):
    assert get_note(conn, "nonexistent") is None


def test_set_note_normalizes_case(conn):
    set_note(conn, "MyJob", "hello")
    result = get_note(conn, "myjob")
    assert result is not None
    assert result["job_name"] == "myjob"


def test_set_note_overwrites(conn):
    set_note(conn, "backup", "old note")
    set_note(conn, "backup", "new note")
    result = get_note(conn, "backup")
    assert result["note"] == "new note"


def test_remove_note_returns_true(conn):
    set_note(conn, "backup", "some note")
    assert remove_note(conn, "backup") is True
    assert get_note(conn, "backup") is None


def test_remove_note_missing_returns_false(conn):
    assert remove_note(conn, "ghost") is False


def test_list_notes_empty(conn):
    assert list_notes(conn) == []


def test_list_notes_returns_all(conn):
    set_note(conn, "jobA", "note A")
    set_note(conn, "jobB", "note B")
    results = list_notes(conn)
    assert len(results) == 2
    names = [r["job_name"] for r in results]
    assert "joba" in names and "jobb" in names
