import time
import pytest
from cronwatcher import throttle


@pytest.fixture(autouse=True)
def clear_state():
    throttle.clear()
    yield
    throttle.clear()


def test_not_throttled_when_empty():
    assert throttle.is_throttled("myjob") is False


def test_throttled_after_set():
    throttle.set_cooldown("myjob")
    assert throttle.is_throttled("myjob") is True


def test_not_throttled_after_cooldown_expires():
    throttle.set_cooldown("myjob", ts=time.time() - 400)
    assert throttle.is_throttled("myjob", cooldown=300) is False


def test_jobs_are_isolated():
    throttle.set_cooldown("job-a")
    assert throttle.is_throttled("job-a") is True
    assert throttle.is_throttled("job-b") is False


def test_get_last_alert_none_when_empty():
    assert throttle.get_last_alert("myjob") is None


def test_get_last_alert_returns_timestamp():
    ts = time.time() - 60
    throttle.set_cooldown("myjob", ts=ts)
    assert abs(throttle.get_last_alert("myjob") - ts) < 0.01


def test_clear_single_job():
    throttle.set_cooldown("job-a")
    throttle.set_cooldown("job-b")
    throttle.clear("job-a")
    assert throttle.is_throttled("job-a") is False
    assert throttle.is_throttled("job-b") is True


def test_clear_all():
    throttle.set_cooldown("job-a")
    throttle.set_cooldown("job-b")
    throttle.clear()
    assert throttle.is_throttled("job-a") is False
    assert throttle.is_throttled("job-b") is False


def test_time_until_unthrottled_none_when_not_set():
    assert throttle.time_until_unthrottled("myjob") is None


def test_time_until_unthrottled_returns_seconds():
    throttle.set_cooldown("myjob", ts=time.time() - 100)
    remaining = throttle.time_until_unthrottled("myjob", cooldown=300)
    assert remaining is not None
    assert 195 <= remaining <= 205


def test_time_until_unthrottled_none_after_expiry():
    throttle.set_cooldown("myjob", ts=time.time() - 400)
    assert throttle.time_until_unthrottled("myjob", cooldown=300) is None
