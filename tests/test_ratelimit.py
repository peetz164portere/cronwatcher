"""Tests for cronwatcher.ratelimit."""

import time
import pytest
from cronwatcher import ratelimit


@pytest.fixture(autouse=True)
def clear_state():
    ratelimit.reset()
    yield
    ratelimit.reset()


def test_not_limited_when_no_alerts_sent():
    assert ratelimit.is_rate_limited("myjob", max_alerts=3, window_seconds=60) is False


def test_not_limited_below_threshold():
    ratelimit.record_alert_sent("myjob")
    ratelimit.record_alert_sent("myjob")
    assert ratelimit.is_rate_limited("myjob", max_alerts=3, window_seconds=60) is False


def test_limited_at_threshold():
    for _ in range(3):
        ratelimit.record_alert_sent("myjob")
    assert ratelimit.is_rate_limited("myjob", max_alerts=3, window_seconds=60) is True


def test_jobs_are_isolated():
    for _ in range(5):
        ratelimit.record_alert_sent("job_a")
    assert ratelimit.is_rate_limited("job_b", max_alerts=3, window_seconds=60) is False


def test_old_alerts_expire(monkeypatch):
    base = time.time()
    monkeypatch.setattr(time, "time", lambda: base)
    for _ in range(3):
        ratelimit.record_alert_sent("myjob")
    # advance time past window
    monkeypatch.setattr(time, "time", lambda: base + 61)
    assert ratelimit.is_rate_limited("myjob", max_alerts=3, window_seconds=60) is False


def test_max_alerts_zero_never_limited():
    for _ in range(100):
        ratelimit.record_alert_sent("myjob")
    assert ratelimit.is_rate_limited("myjob", max_alerts=0, window_seconds=60) is False


def test_reset_single_job():
    ratelimit.record_alert_sent("job_a")
    ratelimit.record_alert_sent("job_b")
    ratelimit.reset("job_a")
    assert ratelimit.get_alert_count("job_a", 60) == 0
    assert ratelimit.get_alert_count("job_b", 60) == 1


def test_get_alert_count():
    ratelimit.record_alert_sent("myjob")
    ratelimit.record_alert_sent("myjob")
    assert ratelimit.get_alert_count("myjob", window_seconds=60) == 2
