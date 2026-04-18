"""Tests for cronwatcher.watchdog."""

import pytest
from datetime import datetime, timedelta
from cronwatcher.storage import get_connection, init_db
from cronwatcher.watchdog import get_hung_runs, mark_hung_as_failed, resolve_hung_runs


@pytest.fixture
def tmp_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    return db_path


def _insert_run(db_path, job_name, started_offset_minutes, status="running"):
    conn = get_connection(db_path)
    started = (datetime.utcnow() - timedelta(minutes=started_offset_minutes)).isoformat()
    cur = conn.execute(
        "INSERT INTO runs (job_name, started_at, status) VALUES (?, ?, ?)",
        (job_name, started, status),
    )
    conn.commit()
    run_id = cur.lastrowid
    conn.close()
    return run_id


def test_get_hung_runs_empty(tmp_db):
    assert get_hung_runs(tmp_db) == []


def test_get_hung_runs_detects_old_running(tmp_db):
    _insert_run(tmp_db, "backup", started_offset_minutes=90)
    hung = get_hung_runs(tmp_db, timeout_minutes=60)
    assert len(hung) == 1
    assert hung[0]["job_name"] == "backup"


def test_get_hung_runs_ignores_recent(tmp_db):
    _insert_run(tmp_db, "quick", started_offset_minutes=10)
    hung = get_hung_runs(tmp_db, timeout_minutes=60)
    assert hung == []


def test_get_hung_runs_ignores_finished(tmp_db):
    _insert_run(tmp_db, "done", started_offset_minutes=90, status="success")
    hung = get_hung_runs(tmp_db, timeout_minutes=60)
    assert hung == []


def test_mark_hung_as_failed(tmp_db):
    run_id = _insert_run(tmp_db, "myjob", started_offset_minutes=90)
    mark_hung_as_failed(tmp_db, run_id)
    conn = get_connection(tmp_db)
    row = conn.execute("SELECT status, output FROM runs WHERE id = ?", (run_id,)).fetchone()
    conn.close()
    assert row[0] == "failure"
    assert row[1] == "hung"


def test_resolve_hung_runs_dry_run_does_not_modify(tmp_db):
    _insert_run(tmp_db, "slow", started_offset_minutes=120)
    hung = resolve_hung_runs(tmp_db, timeout_minutes=60, dry_run=True)
    assert len(hung) == 1
    conn = get_connection(tmp_db)
    row = conn.execute("SELECT status FROM runs").fetchone()
    conn.close()
    assert row[0] == "running"


def test_resolve_hung_runs_marks_failed(tmp_db):
    _insert_run(tmp_db, "slow", started_offset_minutes=120)
    hung = resolve_hung_runs(tmp_db, timeout_minutes=60, dry_run=False)
    assert len(hung) == 1
    conn = get_connection(tmp_db)
    row = conn.execute("SELECT status FROM runs").fetchone()
    conn.close()
    assert row[0] == "failure"
