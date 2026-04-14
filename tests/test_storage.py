"""Tests for cronwatcher.storage module."""

import os
import tempfile
import pytest

from cronwatcher.storage import init_db, record_start, record_finish, fetch_history


@pytest.fixture
def tmp_db(tmp_path):
    db_file = str(tmp_path / "test.db")
    init_db(db_file)
    return db_file


def test_init_db_creates_file(tmp_path):
    db_file = str(tmp_path / "new.db")
    init_db(db_file)
    assert os.path.exists(db_file)


def test_record_start_returns_id(tmp_db):
    run_id = record_start("backup_job", db_path=tmp_db)
    assert isinstance(run_id, int)
    assert run_id > 0


def test_record_finish_success(tmp_db):
    run_id = record_start("backup_job", db_path=tmp_db)
    record_finish(run_id, exit_code=0, output="done", db_path=tmp_db)

    history = fetch_history("backup_job", db_path=tmp_db)
    assert len(history) == 1
    assert history[0]["status"] == "success"
    assert history[0]["exit_code"] == 0
    assert history[0]["output"] == "done"


def test_record_finish_failure(tmp_db):
    run_id = record_start("cleanup_job", db_path=tmp_db)
    record_finish(run_id, exit_code=1, output="error!", db_path=tmp_db)

    history = fetch_history("cleanup_job", db_path=tmp_db)
    assert history[0]["status"] == "failure"
    assert history[0]["exit_code"] == 1


def test_fetch_history_all_jobs(tmp_db):
    record_start("job_a", db_path=tmp_db)
    record_start("job_b", db_path=tmp_db)

    all_history = fetch_history(db_path=tmp_db)
    assert len(all_history) == 2


def test_fetch_history_filtered(tmp_db):
    record_start("job_a", db_path=tmp_db)
    record_start("job_b", db_path=tmp_db)

    filtered = fetch_history("job_a", db_path=tmp_db)
    assert len(filtered) == 1
    assert filtered[0]["job_name"] == "job_a"


def test_fetch_history_limit(tmp_db):
    for _ in range(5):
        record_start("batch_job", db_path=tmp_db)

    limited = fetch_history("batch_job", limit=3, db_path=tmp_db)
    assert len(limited) == 3
