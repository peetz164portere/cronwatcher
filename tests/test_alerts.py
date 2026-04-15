"""Tests for cronwatcher.alerts module."""

import time
import pytest
from cronwatcher.alerts import (
    init_alert_log,
    record_alert,
    get_last_alert_time,
    should_suppress_alert,
    DEFAULT_COOLDOWN_SECONDS,
)
from cronwatcher.storage import init_db


@pytest.fixture
def tmp_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    init_alert_log(db_path)
    return db_path


def test_init_alert_log_creates_table(tmp_db):
    # Should not raise; calling twice is also safe
    init_alert_log(tmp_db)


def test_get_last_alert_time_none_when_empty(tmp_db):
    result = get_last_alert_time(tmp_db, "backup-job")
    assert result is None


def test_record_alert_stores_entry(tmp_db):
    record_alert(tmp_db, "backup-job", run_id=42)
    last = get_last_alert_time(tmp_db, "backup-job")
    assert last is not None
    assert abs(last - time.time()) < 5


def test_record_alert_multiple_returns_latest(tmp_db):
    record_alert(tmp_db, "backup-job", run_id=1)
    time.sleep(0.05)
    record_alert(tmp_db, "backup-job", run_id=2)
    last = get_last_alert_time(tmp_db, "backup-job")
    assert last is not None
    # The second alert should be the most recent
    assert abs(last - time.time()) < 5


def test_should_suppress_alert_false_when_no_history(tmp_db):
    assert should_suppress_alert(tmp_db, "new-job") is False


def test_should_suppress_alert_true_within_cooldown(tmp_db):
    record_alert(tmp_db, "backup-job", run_id=1)
    assert should_suppress_alert(tmp_db, "backup-job", cooldown=3600) is True


def test_should_suppress_alert_false_after_cooldown(tmp_db, monkeypatch):
    record_alert(tmp_db, "backup-job", run_id=1)
    # Simulate time moving past the cooldown window
    future = time.time() + DEFAULT_COOLDOWN_SECONDS + 10
    monkeypatch.setattr("cronwatcher.alerts.time.time", lambda: future)
    assert should_suppress_alert(tmp_db, "backup-job") is False


def test_alert_isolation_between_jobs(tmp_db):
    record_alert(tmp_db, "job-a", run_id=1)
    assert should_suppress_alert(tmp_db, "job-b") is False
    assert should_suppress_alert(tmp_db, "job-a") is True
