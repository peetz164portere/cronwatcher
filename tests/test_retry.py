"""Tests for cronwatcher.retry module."""

import pytest
from unittest.mock import MagicMock, patch

from cronwatcher.retry import with_retry, get_retry_config


def test_with_retry_succeeds_first_attempt():
    fn = MagicMock(return_value="ok")
    result = with_retry(fn, max_retries=3, base_delay=0)
    assert result == "ok"
    assert fn.call_count == 1


def test_with_retry_succeeds_after_failures():
    fn = MagicMock(side_effect=[ValueError("oops"), ValueError("again"), "done"])
    result = with_retry(fn, max_retries=3, base_delay=0)
    assert result == "done"
    assert fn.call_count == 3


def test_with_retry_raises_after_max_retries():
    fn = MagicMock(side_effect=ConnectionError("down"))
    with pytest.raises(ConnectionError, match="down"):
        with_retry(fn, max_retries=2, base_delay=0)
    # initial attempt + 2 retries = 3 total calls
    assert fn.call_count == 3


def test_with_retry_only_catches_specified_exceptions():
    fn = MagicMock(side_effect=RuntimeError("unexpected"))
    with pytest.raises(RuntimeError):
        with_retry(fn, max_retries=3, base_delay=0, exceptions=(ValueError,))
    assert fn.call_count == 1


def test_with_retry_respects_max_delay():
    delays = []

    def fake_sleep(d):
        delays.append(d)

    fn = MagicMock(side_effect=[OSError(), OSError(), OSError(), "ok"])
    with patch("cronwatcher.retry.time.sleep", side_effect=fake_sleep):
        with_retry(fn, max_retries=3, base_delay=10.0, backoff_factor=10.0, max_delay=15.0)

    assert all(d <= 15.0 for d in delays)


def test_with_retry_sleeps_with_backoff():
    delays = []

    def fake_sleep(d):
        delays.append(d)

    fn = MagicMock(side_effect=[OSError(), OSError(), "ok"])
    with patch("cronwatcher.retry.time.sleep", side_effect=fake_sleep):
        with_retry(fn, max_retries=3, base_delay=1.0, backoff_factor=2.0, max_delay=100.0)

    assert delays == [1.0, 2.0]


def test_with_retry_label_defaults_to_fn_name():
    """Should not raise even when label is None."""
    def my_func():
        return 42

    result = with_retry(my_func, max_retries=1, base_delay=0)
    assert result == 42


def test_get_retry_config_defaults():
    cfg = get_retry_config({})
    assert cfg["max_retries"] == 3
    assert cfg["base_delay"] == 1.0
    assert cfg["backoff_factor"] == 2.0
    assert cfg["max_delay"] == 30.0


def test_get_retry_config_custom():
    cfg = get_retry_config({"retry": {"max_retries": 5, "base_delay": 0.5, "max_delay": 60}})
    assert cfg["max_retries"] == 5
    assert cfg["base_delay"] == 0.5
    assert cfg["max_delay"] == 60.0
