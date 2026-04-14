import json
from unittest.mock import patch, MagicMock
import pytest

from cronwatcher.webhook import build_payload, send_webhook, notify_failure
from cronwatcher.config import should_alert, load_config, save_config


# ---------------------------------------------------------------------------
# build_payload
# ---------------------------------------------------------------------------

def test_build_payload_keys():
    payload = build_payload("backup", 42, 1, "2024-01-01T00:00:00", "2024-01-01T00:01:00", 60.0)
    assert payload["event"] == "cron_failure"
    assert payload["job_name"] == "backup"
    assert payload["run_id"] == 42
    assert payload["exit_code"] == 1
    assert payload["duration_seconds"] == 60.0
    assert "timestamp" in payload


def test_build_payload_duration_rounded():
    payload = build_payload("sync", 1, 2, "s", "e", 12.3456789)
    assert payload["duration_seconds"] == 12.35


# ---------------------------------------------------------------------------
# send_webhook
# ---------------------------------------------------------------------------

def _make_mock_response(status: int):
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def test_send_webhook_success():
    with patch("urllib.request.urlopen", return_value=_make_mock_response(200)):
        result = send_webhook("http://example.com/hook", {"key": "val"})
    assert result is True


def test_send_webhook_http_error():
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
        url="http://x", code=500, msg="Server Error", hdrs={}, fp=None
    )):
        result = send_webhook("http://example.com/hook", {})
    assert result is False


def test_send_webhook_url_error():
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("no route")):
        result = send_webhook("http://example.com/hook", {})
    assert result is False


# ---------------------------------------------------------------------------
# notify_failure
# ---------------------------------------------------------------------------

def test_notify_failure_no_url():
    result = notify_failure("", "job", 1, 1, "s", "e", 1.0)
    assert result is False


# ---------------------------------------------------------------------------
# config helpers
# ---------------------------------------------------------------------------

def test_should_alert_zero_exit_code():
    assert should_alert(0, []) is False


def test_should_alert_nonzero_no_filter():
    assert should_alert(1, []) is True


def test_should_alert_filtered_match():
    assert should_alert(2, [1, 2, 3]) is True


def test_should_alert_filtered_no_match():
    assert should_alert(5, [1, 2, 3]) is False


def test_load_config_defaults_when_missing(tmp_path):
    cfg = load_config(str(tmp_path / "nonexistent.json"))
    assert "db_path" in cfg
    assert "webhook_url" in cfg


def test_save_and_load_config(tmp_path):
    path = str(tmp_path / "config.json")
    save_config({"webhook_url": "http://hooks.example.com", "webhook_timeout": 5}, path)
    cfg = load_config(path)
    assert cfg["webhook_url"] == "http://hooks.example.com"
    assert cfg["webhook_timeout"] == 5
    assert "db_path" in cfg  # default merged in
