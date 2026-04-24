"""Tests for cronwatcher/snapshots_diff.py"""

from __future__ import annotations

import pytest

from cronwatcher.snapshots_diff import (
    DIFF_ADDED,
    DIFF_CHANGED,
    DIFF_REMOVED,
    DIFF_UNCHANGED,
    diff_snapshots,
    format_diff_text,
    has_changes,
    summary_counts,
)


def _snap(jobs: dict) -> dict:
    return {"jobs": jobs}


def test_diff_no_changes():
    old = _snap({"backup": "success", "cleanup": "success"})
    new = _snap({"backup": "success", "cleanup": "success"})
    diff = diff_snapshots(old, new)
    assert all(e["change"] == DIFF_UNCHANGED for e in diff)
    assert len(diff) == 2


def test_diff_added_job():
    old = _snap({"backup": "success"})
    new = _snap({"backup": "success", "cleanup": "failure"})
    diff = diff_snapshots(old, new)
    added = [e for e in diff if e["change"] == DIFF_ADDED]
    assert len(added) == 1
    assert added[0]["job"] == "cleanup"
    assert added[0]["new"] == "failure"
    assert added[0]["old"] is None


def test_diff_removed_job():
    old = _snap({"backup": "success", "cleanup": "success"})
    new = _snap({"backup": "success"})
    diff = diff_snapshots(old, new)
    removed = [e for e in diff if e["change"] == DIFF_REMOVED]
    assert len(removed) == 1
    assert removed[0]["job"] == "cleanup"
    assert removed[0]["new"] is None


def test_diff_changed_status():
    old = _snap({"backup": "success"})
    new = _snap({"backup": "failure"})
    diff = diff_snapshots(old, new)
    assert diff[0]["change"] == DIFF_CHANGED
    assert diff[0]["old"] == "success"
    assert diff[0]["new"] == "failure"


def test_diff_empty_snapshots():
    diff = diff_snapshots(_snap({}), _snap({}))
    assert diff == []


def test_has_changes_false():
    old = new = _snap({"job_a": "success"})
    assert has_changes(diff_snapshots(old, new)) is False


def test_has_changes_true():
    old = _snap({"job_a": "success"})
    new = _snap({"job_a": "failure"})
    assert has_changes(diff_snapshots(old, new)) is True


def test_format_diff_text_no_changes():
    diff = diff_snapshots(_snap({"x": "success"}), _snap({"x": "success"}))
    text = format_diff_text(diff)
    assert text == "No changes."


def test_format_diff_text_show_unchanged():
    diff = diff_snapshots(_snap({"x": "success"}), _snap({"x": "success"}))
    text = format_diff_text(diff, show_unchanged=True)
    assert "x" in text
    assert "[ ]" in text


def test_format_diff_text_added_job():
    diff = diff_snapshots(_snap({}), _snap({"newjob": "success"}))
    text = format_diff_text(diff)
    assert "[+]" in text
    assert "newjob" in text


def test_format_diff_text_removed_job():
    diff = diff_snapshots(_snap({"gone": "success"}), _snap({}))
    text = format_diff_text(diff)
    assert "[-]" in text
    assert "gone" in text


def test_summary_counts():
    old = _snap({"a": "success", "b": "success", "c": "success"})
    new = _snap({"a": "failure", "b": "success", "d": "success"})
    counts = summary_counts(diff_snapshots(old, new))
    assert counts[DIFF_CHANGED] == 1
    assert counts[DIFF_UNCHANGED] == 1
    assert counts[DIFF_ADDED] == 1
    assert counts[DIFF_REMOVED] == 1
