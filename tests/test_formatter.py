"""Tests for cronwatcher.formatter."""

import pytest
from cronwatcher.formatter import (
    format_duration,
    format_timestamp,
    format_row,
    format_history_table,
)


def test_format_duration_seconds():
    assert format_duration(5.678) == "5.7s"


def test_format_duration_minutes():
    assert format_duration(90.0) == "1m 30s"


def test_format_duration_hours():
    assert format_duration(3661.0) == "1h 1m"


def test_format_duration_none():
    assert format_duration(None) == "—"


def test_format_duration_zero():
    assert format_duration(0.0) == "0.0s"


def test_format_duration_exactly_one_minute():
    assert format_duration(60.0) == "1m 0s"


def test_format_timestamp_valid():
    result = format_timestamp("2024-05-10T14:32:00")
    assert result == "2024-05-10 14:32:00"


def test_format_timestamp_none():
    assert format_timestamp(None) == "—"


def test_format_timestamp_invalid():
    assert format_timestamp("not-a-date") == "not-a-date"


def _make_record(**kwargs):
    defaults = {
        "job_name": "backup",
        "status": "success",
        "started_at": "2024-05-10T14:32:00",
        "duration": 12.5,
        "exit_code": 0,
    }
    defaults.update(kwargs)
    return defaults


def test_format_row_contains_job_name():
    row = format_row(_make_record(), use_color=False)
    assert "backup" in row


def test_format_row_success_symbol():
    row = format_row(_make_record(status="success"), use_color=False)
    assert "✓" in row


def test_format_row_failure_symbol():
    row = format_row(_make_record(status="failure"), use_color=False)
    assert "✗" in row


def test_format_row_running_symbol():
    row = format_row(_make_record(status="running"), use_color=False)
    assert "⟳" in row


def test_format_row_no_exit_code():
    row = format_row(_make_record(exit_code=None), use_color=False)
    assert "exit=—" in row


def test_format_row_contains_duration():
    row = format_row(_make_record(duration=12.5), use_color=False)
    assert "12.5s" in row


def test_format_history_table_empty():
    result = format_history_table([])
    assert result == "No history found."


def test_format_history_table_has_header():
    records = [_make_record()]
    result = format_history_table(records, use_color=False)
    assert "JOB" in result
    assert "STARTED" in result
    assert "STATUS" in result


def test_format_history_table_multiple_rows():
    records = [_make_record(job_name=f"job{i}") for i in range(3)]
    result = format_history_table(records, use_color=False)
    assert "job0" in result
    assert "job1" in result
    assert "job2" in result
