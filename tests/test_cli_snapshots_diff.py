"""Tests for cronwatcher/cli_snapshots_diff.py"""

from __future__ import annotations

import json
import os
import tempfile

import pytest
from click.testing import CliRunner

from cronwatcher.cli_snapshots_diff import snapshot_diff_cmd


@pytest.fixture()
def runner():
    return CliRunner()


def _write_snap(tmp_dir: str, name: str, jobs: dict) -> str:
    path = os.path.join(tmp_dir, name)
    with open(path, "w") as f:
        json.dump({"jobs": jobs}, f)
    return path


def test_compare_no_changes_exits_0(runner):
    with runner.isolated_filesystem():
        old = _write_snap(".", "old.json", {"backup": "success"})
        new = _write_snap(".", "new.json", {"backup": "success"})
        result = runner.invoke(snapshot_diff_cmd, ["compare", old, new])
        assert result.exit_code == 0
        assert "No changes" in result.output


def test_compare_with_changes_exits_1(runner):
    with runner.isolated_filesystem():
        old = _write_snap(".", "old.json", {"backup": "success"})
        new = _write_snap(".", "new.json", {"backup": "failure"})
        result = runner.invoke(snapshot_diff_cmd, ["compare", old, new])
        assert result.exit_code == 1
        assert "[~]" in result.output


def test_compare_json_output(runner):
    with runner.isolated_filesystem():
        old = _write_snap(".", "old.json", {"backup": "success"})
        new = _write_snap(".", "new.json", {"backup": "failure"})
        result = runner.invoke(snapshot_diff_cmd, ["compare", old, new, "--json"])
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert data[0]["job"] == "backup"
        assert data[0]["change"] == "changed"


def test_compare_summary_flag(runner):
    with runner.isolated_filesystem():
        old = _write_snap(".", "old.json", {"a": "success", "b": "success"})
        new = _write_snap(".", "new.json", {"a": "failure", "c": "success"})
        result = runner.invoke(snapshot_diff_cmd, ["compare", old, new, "--summary"])
        assert "added=1" in result.output
        assert "removed=1" in result.output
        assert "changed=1" in result.output


def test_compare_summary_json(runner):
    with runner.isolated_filesystem():
        old = _write_snap(".", "old.json", {"a": "success"})
        new = _write_snap(".", "new.json", {"a": "success", "b": "failure"})
        result = runner.invoke(snapshot_diff_cmd, ["compare", old, new, "--json", "--summary"])
        data = json.loads(result.output)
        assert data["added"] == 1
        assert data["unchanged"] == 1
