"""Tests for cronwatcher/pauses.py"""

import sqlite3
import pytest
from cronwatcher import pauses


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    pauses.init_pauses(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='pauses'"
    ).fetchall()
    assert len(rows) == 1


def test_pause_job_returns_id(conn):
    row_id = pauses.pause_job(conn, "backup")
    assert isinstance(row_id, int)
    assert row_id > 0


def test_pause_job_normalizes_case(conn):
    pauses.pause_job(conn, "BackupJob")
    assert pauses.is_paused(conn, "backupjob")


def test_is_paused_false_when_empty(conn):
    assert pauses.is_paused(conn, "nonexistent") is False


def test_is_paused_true_after_pause(conn):
    pauses.pause_job(conn, "myjob")
    assert pauses.is_paused(conn, "myjob") is True


def test_resume_job_returns_true(conn):
    pauses.pause_job(conn, "myjob")
    result = pauses.resume_job(conn, "myjob")
    assert result is True
    assert pauses.is_paused(conn, "myjob") is False


def test_resume_job_returns_false_when_not_paused(conn):
    result = pauses.resume_job(conn, "ghost")
    assert result is False


def test_pause_idempotent_updates_reason(conn):
    pauses.pause_job(conn, "myjob", reason="maintenance")
    pauses.pause_job(conn, "myjob", reason="updated reason")
    info = pauses.get_pause_info(conn, "myjob")
    assert info["reason"] == "updated reason"


def test_get_pause_info_none_when_not_paused(conn):
    assert pauses.get_pause_info(conn, "unknown") is None


def test_get_pause_info_returns_dict(conn):
    pauses.pause_job(conn, "myjob", reason="deploy")
    info = pauses.get_pause_info(conn, "myjob")
    assert info["job_name"] == "myjob"
    assert info["reason"] == "deploy"
    assert "paused_at" in info


def test_list_paused_empty(conn):
    assert pauses.list_paused(conn) == []


def test_list_paused_returns_all(conn):
    pauses.pause_job(conn, "job_a")
    pauses.pause_job(conn, "job_b", reason="testing")
    rows = pauses.list_paused(conn)
    names = [r["job_name"] for r in rows]
    assert "job_a" in names
    assert "job_b" in names
