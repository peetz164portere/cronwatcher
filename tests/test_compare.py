"""Tests for cronwatcher/compare.py"""

import pytest
from cronwatcher.compare import compare_snapshots, format_compare_text


def _snap(jobs, ts="2024-01-01T00:00:00"):
    return {"created_at": ts, "jobs": jobs}


def test_compare_no_changes():
    jobs = [{"job_name": "backup", "last_status": "success", "total_runs": 5, "failure_count": 0}]
    diff = compare_snapshots(_snap(jobs), _snap(jobs))
    assert diff["summary"] == {"added": 0, "removed": 0, "changed": 0}
    assert diff["added"] == []
    assert diff["removed"] == []
    assert diff["changed"] == []


def test_compare_added_job():
    old = _snap([])
    new = _snap([{"job_name": "sync", "last_status": "success", "total_runs": 1, "failure_count": 0}])
    diff = compare_snapshots(old, new)
    assert "sync" in diff["added"]
    assert diff["summary"]["added"] == 1


def test_compare_removed_job():
    old = _snap([{"job_name": "old_job", "last_status": "success", "total_runs": 3, "failure_count": 0}])
    new = _snap([])
    diff = compare_snapshots(old, new)
    assert "old_job" in diff["removed"]
    assert diff["summary"]["removed"] == 1


def test_compare_changed_status():
    old_job = {"job_name": "deploy", "last_status": "success", "total_runs": 10, "failure_count": 1}
    new_job = {"job_name": "deploy", "last_status": "failure", "total_runs": 11, "failure_count": 2}
    diff = compare_snapshots(_snap([old_job]), _snap([new_job]))
    assert diff["summary"]["changed"] == 1
    entry = diff["changed"][0]
    assert entry["job_name"] == "deploy"
    assert "last_status" in entry["changes"]
    assert entry["changes"]["last_status"] == {"old": "success", "new": "failure"}


def test_compare_multiple_changes():
    old = _snap([
        {"job_name": "a", "last_status": "success", "total_runs": 1, "failure_count": 0},
        {"job_name": "b", "last_status": "success", "total_runs": 2, "failure_count": 0},
    ])
    new = _snap([
        {"job_name": "a", "last_status": "failure", "total_runs": 2, "failure_count": 1},
        {"job_name": "c", "last_status": "success", "total_runs": 1, "failure_count": 0},
    ])
    diff = compare_snapshots(old, new)
    assert diff["summary"]["added"] == 1
    assert diff["summary"]["removed"] == 1
    assert diff["summary"]["changed"] == 1


def test_format_compare_text_no_changes():
    diff = compare_snapshots(_snap([]), _snap([]))
    text = format_compare_text(diff)
    assert "No changes detected" in text


def test_format_compare_text_shows_changes():
    old_job = {"job_name": "etl", "last_status": "success", "total_runs": 5, "failure_count": 0}
    new_job = {"job_name": "etl", "last_status": "failure", "total_runs": 6, "failure_count": 1}
    diff = compare_snapshots(_snap([old_job]), _snap([new_job]))
    text = format_compare_text(diff)
    assert "etl" in text
    assert "last_status" in text
    assert "success" in text
    assert "failure" in text
