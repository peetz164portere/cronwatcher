import pytest
import sqlite3
from cronwatcher.checkpoints import init_checkpoints, set_checkpoint, get_checkpoint, list_checkpoints, remove_checkpoint


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_checkpoints(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [t[0] for t in tables]
    assert "checkpoints" in names


def test_set_checkpoint_returns_id(conn):
    row_id = set_checkpoint(conn, "backup", "started")
    assert isinstance(row_id, int)
    assert row_id > 0


def test_get_checkpoint_returns_dict(conn):
    set_checkpoint(conn, "backup", "started", note="phase 1")
    cp = get_checkpoint(conn, "backup", "started")
    assert cp is not None
    assert cp["job_name"] == "backup"
    assert cp["label"] == "started"
    assert cp["note"] == "phase 1"
    assert "recorded_at" in cp


def test_get_checkpoint_missing_returns_none(conn):
    result = get_checkpoint(conn, "ghost", "nope")
    assert result is None


def test_set_checkpoint_normalizes_case(conn):
    set_checkpoint(conn, "Backup", "Started")
    cp = get_checkpoint(conn, "backup", "started")
    assert cp is not None


def test_set_checkpoint_upserts(conn):
    set_checkpoint(conn, "backup", "done", note="first")
    set_checkpoint(conn, "backup", "done", note="updated")
    cp = get_checkpoint(conn, "backup", "done")
    assert cp["note"] == "updated"
    rows = conn.execute("SELECT COUNT(*) FROM checkpoints WHERE job_name='backup' AND label='done'").fetchone()[0]
    assert rows == 1


def test_list_checkpoints_empty(conn):
    result = list_checkpoints(conn, "nojob")
    assert result == []


def test_list_checkpoints_returns_all(conn):
    set_checkpoint(conn, "myjob", "step1")
    set_checkpoint(conn, "myjob", "step2")
    result = list_checkpoints(conn, "myjob")
    assert len(result) == 2
    labels = [r["label"] for r in result]
    assert "step1" in labels
    assert "step2" in labels


def test_remove_checkpoint_returns_true(conn):
    set_checkpoint(conn, "job", "mark")
    removed = remove_checkpoint(conn, "job", "mark")
    assert removed is True
    assert get_checkpoint(conn, "job", "mark") is None


def test_remove_checkpoint_missing_returns_false(conn):
    removed = remove_checkpoint(conn, "ghost", "nowhere")
    assert removed is False
