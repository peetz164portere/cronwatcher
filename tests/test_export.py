"""Tests for cronwatcher/export.py"""
import json
import csv
import io
import pytest
from unittest.mock import MagicMock
from cronwatcher.export import rows_to_dicts, export_csv, export_json, export_history


def _make_rows(data):
    """Create list of dict-like objects mimicking sqlite3.Row."""
    rows = []
    for d in data:
        m = MagicMock()
        m.keys.return_value = list(d.keys())
        m.__iter__ = lambda self: iter(d.items())
        # make dict(row) work
        rows.append(d)  # plain dicts work fine for our implementation
    return rows


SAMPLE = [
    {"id": 1, "job_name": "backup", "status": "success", "duration": 12.5},
    {"id": 2, "job_name": "backup", "status": "failure", "duration": None},
]


def test_rows_to_dicts_plain_dicts():
    result = rows_to_dicts(SAMPLE)
    assert result == SAMPLE


def test_export_json_valid():
    result = export_json(SAMPLE)
    parsed = json.loads(result)
    assert len(parsed) == 2
    assert parsed[0]["job_name"] == "backup"


def test_export_json_empty():
    result = export_json([])
    assert json.loads(result) == []


def test_export_csv_has_header():
    result = export_csv(SAMPLE)
    lines = result.strip().splitlines()
    assert lines[0] == "id,job_name,status,duration"
    assert len(lines) == 3  # header + 2 rows


def test_export_csv_empty():
    result = export_csv([])
    assert result == ""


def test_export_history_json():
    result = export_history(SAMPLE, fmt="json")
    parsed = json.loads(result)
    assert isinstance(parsed, list)


def test_export_history_csv():
    result = export_history(SAMPLE, fmt="csv")
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert len(rows) == 2
    assert rows[1]["status"] == "failure"


def test_export_history_invalid_format():
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_history(SAMPLE, fmt="xml")


def test_export_json_indent():
    result = export_json(SAMPLE, indent=4)
    # 4-space indent means lines start with 4 spaces
    assert "    " in result
