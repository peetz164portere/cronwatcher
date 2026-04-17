"""Tests for cronwatcher/annotations.py"""
import sqlite3
import pytest
from cronwatcher.annotations import (
    init_annotations,
    add_annotation,
    get_annotations,
    delete_annotation,
    get_all_annotations,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_annotations(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='annotations'")
    assert cur.fetchone() is not None


def test_add_annotation_returns_id(conn):
    aid = add_annotation(conn, run_id=1, note="looks good")
    assert isinstance(aid, int)
    assert aid > 0


def test_get_annotations_empty(conn):
    result = get_annotations(conn, run_id=99)
    assert result == []


def test_get_annotations_returns_correct_run(conn):
    add_annotation(conn, run_id=1, note="note for run 1")
    add_annotation(conn, run_id=2, note="note for run 2")
    results = get_annotations(conn, run_id=1)
    assert len(results) == 1
    assert results[0]["note"] == "note for run 1"
    assert results[0]["run_id"] == 1


def test_get_annotations_multiple(conn):
    add_annotation(conn, run_id=5, note="first")
    add_annotation(conn, run_id=5, note="second")
    results = get_annotations(conn, run_id=5)
    assert len(results) == 2
    notes = [r["note"] for r in results]
    assert "first" in notes
    assert "second" in notes


def test_delete_annotation_removes_entry(conn):
    aid = add_annotation(conn, run_id=3, note="to delete")
    deleted = delete_annotation(conn, aid)
    assert deleted is True
    assert get_annotations(conn, run_id=3) == []


def test_delete_annotation_nonexistent(conn):
    result = delete_annotation(conn, annotation_id=9999)
    assert result is False


def test_get_all_annotations(conn):
    add_annotation(conn, run_id=1, note="a")
    add_annotation(conn, run_id=2, note="b")
    all_ann = get_all_annotations(conn)
    assert len(all_ann) == 2
