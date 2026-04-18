"""Tests for cronwatcher/cli_compare.py"""

import json
import pytest
from click.testing import CliRunner
from cronwatcher.cli_compare import compare_cmd


@pytest.fixture
def runner():
    return CliRunner()


def _write_snap(path, jobs, ts="2024-01-01T00:00:00"):
    with open(path, "w") as f:
        json.dump({"created_at": ts, "jobs": jobs}, f)


def test_compare_no_changes_exits_0(runner, tmp_path):
    jobs = [{"job_name": "backup", "last_status": "success", "total_runs": 3, "failure_count": 0}]
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    _write_snap(old, jobs)
    _write_snap(new, jobs)
    result = runner.invoke(compare_cmd, [str(old), str(new)])
    assert result.exit_code == 0
    assert "No changes detected" in result.output


def test_compare_with_changes_exits_1(runner, tmp_path):
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    _write_snap(old, [{"job_name": "sync", "last_status": "success", "total_runs": 1, "failure_count": 0}])
    _write_snap(new, [{"job_name": "sync", "last_status": "failure", "total_runs": 2, "failure_count": 1}])
    result = runner.invoke(compare_cmd, [str(old), str(new)])
    assert result.exit_code == 1
    assert "sync" in result.output


def test_compare_json_output(runner, tmp_path):
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    _write_snap(old, [])
    _write_snap(new, [{"job_name": "newjob", "last_status": "success", "total_runs": 1, "failure_count": 0}])
    result = runner.invoke(compare_cmd, [str(old), str(new), "--json"])
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert "added" in data
    assert "newjob" in data["added"]


def test_compare_added_and_removed(runner, tmp_path):
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    _write_snap(old, [{"job_name": "gone", "last_status": "success", "total_runs": 1, "failure_count": 0}])
    _write_snap(new, [{"job_name": "fresh", "last_status": "success", "total_runs": 1, "failure_count": 0}])
    result = runner.invoke(compare_cmd, [str(old), str(new)])
    assert result.exit_code == 1
    assert "gone" in result.output
    assert "fresh" in result.output
