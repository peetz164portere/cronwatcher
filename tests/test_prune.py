"""Tests for cronwatcher.prune and cli_prune."""

import pytest
from datetime import datetime, timedelta
from click.testing import CliRunner
from cronwatcher.storage import get_connection, init_db
from cronwatcher.alerts import init_alert_log
from cronwatcher.prune import prune_history, prune_alert_log
from cronwatcher.cli_prune import prune_cmd


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    conn = get_connection(db)
    init_db(conn)
    init_alert_log(conn)
    conn.close()
    return db


def _insert_run(db_path, job_name, started_at, status="success"):
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO runs (job_name, started_at, finished_at, status, exit_code) VALUES (?, ?, ?, ?, ?)",
        (job_name, started_at, started_at, status, 0),
    )
    conn.commit()
    conn.close()


def _insert_alert(db_path, job_name, alerted_at):
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO alert_log (job_name, alerted_at) VALUES (?, ?)",
        (job_name, alerted_at),
    )
    conn.commit()
    conn.close()


def test_prune_history_removes_old(tmp_db):
    old = (datetime.utcnow() - timedelta(days=40)).isoformat()
    recent = (datetime.utcnow() - timedelta(days=5)).isoformat()
    _insert_run(tmp_db, "job1", old)
    _insert_run(tmp_db, "job1", recent)
    deleted = prune_history(tmp_db, older_than_days=30)
    assert deleted == 1


def test_prune_history_by_job(tmp_db):
    old = (datetime.utcnow() - timedelta(days=40)).isoformat()
    _insert_run(tmp_db, "job1", old)
    _insert_run(tmp_db, "job2", old)
    deleted = prune_history(tmp_db, older_than_days=30, job_name="job1")
    assert deleted == 1
    conn = get_connection(tmp_db)
    rows = conn.execute("SELECT job_name FROM runs").fetchall()
    conn.close()
    assert rows[0][0] == "job2"


def test_prune_history_nothing_old(tmp_db):
    recent = (datetime.utcnow() - timedelta(days=1)).isoformat()
    _insert_run(tmp_db, "job1", recent)
    deleted = prune_history(tmp_db, older_than_days=30)
    assert deleted == 0


def test_prune_alert_log(tmp_db):
    old = (datetime.utcnow() - timedelta(days=60)).isoformat()
    _insert_alert(tmp_db, "job1", old)
    deleted = prune_alert_log(tmp_db, older_than_days=30)
    assert deleted == 1


def test_cli_prune_dry_run(tmp_db):
    runner = CliRunner()
    result = runner.invoke(prune_cmd, ["--db", tmp_db, "--days", "30", "--dry-run"])
    assert result.exit_code == 0
    assert "dry-run" in result.output


def test_cli_prune_with_alerts_flag(tmp_db):
    old = (datetime.utcnow() - timedelta(days=40)).isoformat()
    _insert_run(tmp_db, "job1", old)
    _insert_alert(tmp_db, "job1", old)
    runner = CliRunner()
    result = runner.invoke(prune_cmd, ["--db", tmp_db, "--days", "30", "--alerts"])
    assert result.exit_code == 0
    assert "run record" in result.output
    assert "alert log" in result.output
