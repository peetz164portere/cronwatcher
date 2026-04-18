"""Tests for cronwatcher/cli_metrics.py"""
import json
import sqlite3
import pytest
from click.testing import CliRunner
from datetime import datetime, timezone
from cronwatcher.storage import init_db
from cronwatcher.metrics import init_metrics, refresh_metrics
from cronwatcher.cli_metrics import metrics_cmd


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    conn = sqlite3.connect(db)
    init_db(conn)
    init_metrics(conn)
    now = datetime.now(timezone.utc).isoformat()
    for d in [10.0, 20.0, 30.0]:
        conn.execute(
            "INSERT INTO runs (job_name, status, duration, started_at, finished_at) VALUES (?,?,?,?,?)",
            ("backup", "success", d, now, now)
        )
    conn.commit()
    refresh_metrics(conn, "backup")
    conn.close()
    return db


def test_show_existing_job(runner, tmp_db):
    result = runner.invoke(metrics_cmd, ["show", "backup", "--db", tmp_db])
    assert result.exit_code == 0
    assert "backup" in result.output
    assert "Runs:" in result.output


def test_show_missing_job_exits_1(runner, tmp_db):
    result = runner.invoke(metrics_cmd, ["show", "ghost", "--db", tmp_db])
    assert result.exit_code == 1
    assert "No metrics" in result.output


def test_show_json_output(runner, tmp_db):
    result = runner.invoke(metrics_cmd, ["show", "backup", "--db", tmp_db, "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["job_name"] == "backup"
    assert data["max_duration"] == 30.0


def test_list_empty(runner, tmp_path):
    db = str(tmp_path / "empty.db")
    result = runner.invoke(metrics_cmd, ["list", "--db", db])
    assert result.exit_code == 0
    assert "No metrics" in result.output


def test_list_shows_jobs(runner, tmp_db):
    result = runner.invoke(metrics_cmd, ["list", "--db", tmp_db])
    assert result.exit_code == 0
    assert "backup" in result.output


def test_list_json(runner, tmp_db):
    result = runner.invoke(metrics_cmd, ["list", "--db", tmp_db, "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["job_name"] == "backup"


def test_refresh_all(runner, tmp_db):
    result = runner.invoke(metrics_cmd, ["refresh", "--db", tmp_db])
    assert result.exit_code == 0
    assert "Refreshed" in result.output


def test_refresh_specific_job(runner, tmp_db):
    result = runner.invoke(metrics_cmd, ["refresh", "--db", tmp_db, "--job", "backup"])
    assert result.exit_code == 0
    assert "1 job" in result.output
