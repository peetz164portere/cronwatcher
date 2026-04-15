"""Tests for the digest report module."""

import pytest
from unittest.mock import patch
from datetime import datetime, timedelta
from cronwatcher.digest import build_digest, format_digest_text, _parse_dt


FAKE_NOW = datetime(2024, 6, 1, 12, 0, 0)


def _ts(delta_hours: float) -> str:
    dt = FAKE_NOW - timedelta(hours=delta_hours)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


FAKE_ROWS = [
    {"job_name": "backup", "started_at": _ts(1), "finished_at": _ts(0.9), "exit_code": 0, "duration": 360},
    {"job_name": "backup", "started_at": _ts(2), "finished_at": _ts(1.9), "exit_code": 1, "duration": 360},
    {"job_name": "cleanup", "started_at": _ts(3), "finished_at": _ts(2.8), "exit_code": 0, "duration": 720},
    {"job_name": "cleanup", "started_at": _ts(30), "finished_at": _ts(29), "exit_code": 0, "duration": 3600},
    {"job_name": "sync", "started_at": _ts(0.5), "finished_at": None, "exit_code": None, "duration": None},
]


@patch("cronwatcher.digest.datetime")
@patch("cronwatcher.digest.fetch_history")
def test_build_digest_totals(mock_fetch, mock_dt):
    mock_fetch.return_value = FAKE_ROWS
    mock_dt.utcnow.return_value = FAKE_NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    digest = build_digest(":memory:", hours=24)

    assert digest["total_runs"] == 4  # row at 30h is outside window
    assert digest["failed_runs"] == 1
    assert digest["successful_runs"] == 2
    assert digest["running_runs"] == 1


@patch("cronwatcher.digest.datetime")
@patch("cronwatcher.digest.fetch_history")
def test_build_digest_failure_rate(mock_fetch, mock_dt):
    mock_fetch.return_value = FAKE_ROWS
    mock_dt.utcnow.return_value = FAKE_NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    digest = build_digest(":memory:", hours=24)
    assert digest["failure_rate"] == 25.0


@patch("cronwatcher.digest.datetime")
@patch("cronwatcher.digest.fetch_history")
def test_build_digest_per_job(mock_fetch, mock_dt):
    mock_fetch.return_value = FAKE_ROWS
    mock_dt.utcnow.return_value = FAKE_NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    digest = build_digest(":memory:", hours=24)
    assert "backup" in digest["jobs"]
    assert digest["jobs"]["backup"]["failures"] == 1
    assert digest["jobs"]["cleanup"]["total"] == 1  # only 1 within 24h


@patch("cronwatcher.digest.datetime")
@patch("cronwatcher.digest.fetch_history")
def test_build_digest_empty(mock_fetch, mock_dt):
    mock_fetch.return_value = []
    mock_dt.utcnow.return_value = FAKE_NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    digest = build_digest(":memory:", hours=24)
    assert digest["total_runs"] == 0
    assert digest["failure_rate"] == 0.0


def test_format_digest_text_contains_summary():
    digest = {
        "period_hours": 24,
        "generated_at": "2024-06-01T12:00:00",
        "total_runs": 10,
        "successful_runs": 8,
        "failed_runs": 2,
        "running_runs": 0,
        "failure_rate": 20.0,
        "jobs": {"backup": {"total": 5, "failures": 1}},
    }
    text = format_digest_text(digest)
    assert "Digest" in text
    assert "10" in text
    assert "20.0%" in text
    assert "backup" in text


def test_format_digest_text_no_jobs():
    digest = {
        "period_hours": 6,
        "generated_at": "2024-06-01T12:00:00",
        "total_runs": 0,
        "successful_runs": 0,
        "failed_runs": 0,
        "running_runs": 0,
        "failure_rate": 0.0,
        "jobs": {},
    }
    text = format_digest_text(digest)
    assert "no runs recorded" in text


def test_parse_dt_formats():
    assert _parse_dt("2024-06-01T10:30:00") == datetime(2024, 6, 1, 10, 30, 0)
    assert _parse_dt("2024-06-01 10:30:00") == datetime(2024, 6, 1, 10, 30, 0)
    assert _parse_dt("2024-06-01T10:30:00.123456") == datetime(2024, 6, 1, 10, 30, 0, 123456)


def test_parse_dt_invalid():
    with pytest.raises(ValueError):
        _parse_dt("not-a-date")
