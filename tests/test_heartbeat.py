"""Tests for cronwatcher.heartbeat."""

import sqlite3
from unittest.mock import patch, MagicMock
import pytest
from cronwatcher.heartbeat import (
    init_heartbeat_log,
    record_heartbeat,
    get_heartbeat_history,
    send_heartbeat,
    maybe_heartbeat,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_heartbeat_log(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='heartbeat_log'")
    assert cur.fetchone() is not None


def test_record_heartbeat_returns_id(conn):
    rid = record_heartbeat(conn, "backup", "https://hc-ping.com/abc", True)
    assert isinstance(rid, int) and rid > 0


def test_get_heartbeat_history_empty(conn):
    assert get_heartbeat_history(conn, "nojob") == []


def test_get_heartbeat_history_returns_entries(conn):
    record_heartbeat(conn, "job1", "https://example.com/ping", True)
    record_heartbeat(conn, "job1", "https://example.com/ping", False)
    rows = get_heartbeat_history(conn, "job1")
    assert len(rows) == 2
    assert rows[0]["success"] is False  # most recent first
    assert rows[1]["success"] is True


def test_get_heartbeat_history_isolates_jobs(conn):
    record_heartbeat(conn, "job_a", "https://a.com", True)
    record_heartbeat(conn, "job_b", "https://b.com", True)
    assert len(get_heartbeat_history(conn, "job_a")) == 1


def test_send_heartbeat_success():
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=mock_resp):
        assert send_heartbeat("https://example.com") is True


def test_send_heartbeat_failure():
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("timeout")):
        assert send_heartbeat("https://example.com") is False


def test_maybe_heartbeat_skips_on_failure(conn):
    with patch("cronwatcher.heartbeat.send_heartbeat") as mock_send:
        maybe_heartbeat(conn, "job", "https://example.com", success=False)
        mock_send.assert_not_called()


def test_maybe_heartbeat_skips_when_no_url(conn):
    with patch("cronwatcher.heartbeat.send_heartbeat") as mock_send:
        maybe_heartbeat(conn, "job", None, success=True)
        mock_send.assert_not_called()


def test_maybe_heartbeat_sends_on_success(conn):
    with patch("cronwatcher.heartbeat.send_heartbeat", return_value=True):
        maybe_heartbeat(conn, "job", "https://example.com/ping", success=True)
    rows = get_heartbeat_history(conn, "job")
    assert len(rows) == 1
    assert rows[0]["success"] is True
