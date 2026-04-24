"""Tests for cronwatcher/workflows.py"""

import sqlite3
import pytest
from cronwatcher import workflows as wf_mod


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    wf_mod.init_workflows(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='workflows'"
    ).fetchall()
    assert len(tables) == 1


def test_create_workflow_returns_id(conn):
    row_id = wf_mod.create_workflow(conn, "deploy", "deploy pipeline", ["build", "test", "release"])
    assert isinstance(row_id, int)
    assert row_id > 0


def test_create_workflow_normalizes_name(conn):
    wf_mod.create_workflow(conn, "DEPLOY")
    wf = wf_mod.get_workflow(conn, "deploy")
    assert wf is not None
    assert wf["name"] == "deploy"


def test_create_workflow_duplicate_ignored(conn):
    id1 = wf_mod.create_workflow(conn, "deploy")
    id2 = wf_mod.create_workflow(conn, "deploy")
    assert id2 == 0 or id2 is None or id1 != id2  # second insert is ignored
    # only one row should exist
    rows = conn.execute("SELECT count(*) FROM workflows WHERE name='deploy'").fetchone()
    assert rows[0] == 1


def test_get_workflow_missing_returns_none(conn):
    assert wf_mod.get_workflow(conn, "nonexistent") is None


def test_get_workflow_returns_correct_data(conn):
    wf_mod.create_workflow(conn, "backup", "nightly backup", ["dump", "compress", "upload"])
    wf = wf_mod.get_workflow(conn, "backup")
    assert wf["name"] == "backup"
    assert wf["description"] == "nightly backup"
    assert wf["steps"] == ["dump", "compress", "upload"]
    assert "created_at" in wf


def test_update_steps(conn):
    wf_mod.create_workflow(conn, "deploy", steps=["build"])
    result = wf_mod.update_steps(conn, "deploy", ["build", "test", "release"])
    assert result is True
    wf = wf_mod.get_workflow(conn, "deploy")
    assert wf["steps"] == ["build", "test", "release"]


def test_update_steps_missing_workflow(conn):
    result = wf_mod.update_steps(conn, "ghost", ["a", "b"])
    assert result is False


def test_remove_workflow(conn):
    wf_mod.create_workflow(conn, "cleanup")
    assert wf_mod.remove_workflow(conn, "cleanup") is True
    assert wf_mod.get_workflow(conn, "cleanup") is None


def test_remove_workflow_missing(conn):
    assert wf_mod.remove_workflow(conn, "ghost") is False


def test_list_workflows_empty(conn):
    assert wf_mod.list_workflows(conn) == []


def test_list_workflows_multiple(conn):
    wf_mod.create_workflow(conn, "beta")
    wf_mod.create_workflow(conn, "alpha")
    items = wf_mod.list_workflows(conn)
    assert len(items) == 2
    assert items[0]["name"] == "alpha"  # sorted
    assert items[1]["name"] == "beta"


def test_get_next_step_returns_first_incomplete(conn):
    wf_mod.create_workflow(conn, "pipeline", steps=["build", "test", "deploy"])
    next_step = wf_mod.get_next_step(conn, "pipeline", completed=["build"])
    assert next_step == "test"


def test_get_next_step_all_done_returns_none(conn):
    wf_mod.create_workflow(conn, "pipeline", steps=["build", "test"])
    next_step = wf_mod.get_next_step(conn, "pipeline", completed=["build", "test"])
    assert next_step is None


def test_get_next_step_missing_workflow_returns_none(conn):
    assert wf_mod.get_next_step(conn, "ghost", []) is None
