import json
import os
import pytest
from unittest.mock import patch
from cronwatcher.snapshots import build_snapshot, save_snapshot, load_snapshot
from cronwatcher.storage import get_connection, init_db, record_start, record_finish


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    conn = get_connection(db)
    init_db(conn)
    conn.close()
    return db


def _add_run(db_path, job_name, status, exit_code=0):
    conn = get_connection(db_path)
    run_id = record_start(conn, job_name, "echo hi")
    record_finish(conn, run_id, exit_code, status)
    conn.close()
    return run_id


def test_build_snapshot_empty(tmp_db):
    snap = build_snapshot(tmp_db)
    assert snap["total_jobs"] == 0
    assert snap["jobs"] == []
    assert "generated_at" in snap


def test_build_snapshot_single_job(tmp_db):
    _add_run(tmp_db, "backup", "success", 0)
    snap = build_snapshot(tmp_db)
    assert snap["total_jobs"] == 1
    assert snap["jobs"][0]["job_name"] == "backup"
    assert snap["jobs"][0]["last_status"] == "success"


def test_build_snapshot_deduplicates_jobs(tmp_db):
    _add_run(tmp_db, "backup", "success", 0)
    _add_run(tmp_db, "backup", "failure", 1)
    snap = build_snapshot(tmp_db)
    assert snap["total_jobs"] == 1


def test_build_snapshot_multiple_jobs(tmp_db):
    _add_run(tmp_db, "backup", "success", 0)
    _add_run(tmp_db, "cleanup", "failure", 1)
    snap = build_snapshot(tmp_db)
    assert snap["total_jobs"] == 2
    names = {j["job_name"] for j in snap["jobs"]}
    assert names == {"backup", "cleanup"}


def test_save_and_load_snapshot(tmp_path):
    snap = {"generated_at": "2024-01-01T00:00:00+00:00", "total_jobs": 1, "jobs": []}
    out = str(tmp_path / "snap.json")
    save_snapshot(snap, out)
    assert os.path.exists(out)
    loaded = load_snapshot(out)
    assert loaded["total_jobs"] == 1


def test_load_snapshot_missing_file(tmp_path):
    result = load_snapshot(str(tmp_path / "nonexistent.json"))
    assert result == {}


def test_save_snapshot_creates_nested_dir(tmp_path):
    out = str(tmp_path / "subdir" / "snap.json")
    save_snapshot({"jobs": []}, out)
    assert os.path.exists(out)
