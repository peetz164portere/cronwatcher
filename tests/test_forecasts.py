"""Tests for cronwatcher/forecasts.py"""

import sqlite3
import pytest
from datetime import datetime, timedelta
from cronwatcher.storage import init_db
from cronwatcher.forecasts import (
    init_forecasts,
    compute_forecast,
    save_forecast,
    get_forecast,
    list_forecasts,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_forecasts(c)
    return c


def _insert(conn, job, duration, status="success", offset_hours=0):
    ts = (datetime.utcnow() - timedelta(hours=offset_hours)).isoformat()
    conn.execute(
        "INSERT INTO runs (job_name, status, started_at, finished_at, duration_s) VALUES (?, ?, ?, ?, ?)",
        (job, status, ts, ts, duration),
    )
    conn.commit()


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "forecasts" in tables


def test_compute_forecast_empty(conn):
    result = compute_forecast(conn, "no-such-job")
    assert result is None


def test_compute_forecast_basic(conn):
    for i, d in enumerate([10, 12, 11, 13, 10, 12, 11, 10, 12, 11]):
        _insert(conn, "backup", d, offset_hours=i)
    result = compute_forecast(conn, "backup")
    assert result is not None
    assert result["job_name"] == "backup"
    assert result["predicted_duration_s"] > 0
    assert result["confidence"] == "high"
    assert result["sample_size"] == 10


def test_compute_forecast_low_confidence(conn):
    _insert(conn, "rare", 5.0)
    result = compute_forecast(conn, "rare")
    assert result["confidence"] == "low"


def test_compute_forecast_medium_confidence(conn):
    for d in [5, 6, 7, 8]:
        _insert(conn, "medium-job", float(d))
    result = compute_forecast(conn, "medium-job")
    assert result["confidence"] == "medium"


def test_compute_forecast_ignores_failures(conn):
    _insert(conn, "myjob", 100.0, status="failure")
    result = compute_forecast(conn, "myjob")
    assert result is None


def test_save_and_get_forecast(conn):
    for d in [10, 11, 12]:
        _insert(conn, "nightly", float(d))
    forecast = compute_forecast(conn, "nightly")
    save_forecast(conn, forecast)
    saved = get_forecast(conn, "nightly")
    assert saved is not None
    assert saved["job_name"] == "nightly"
    assert saved["confidence"] == "low"


def test_get_forecast_missing_returns_none(conn):
    assert get_forecast(conn, "ghost") is None


def test_save_forecast_upserts(conn):
    for d in [10, 11, 12]:
        _insert(conn, "daily", float(d))
    f1 = compute_forecast(conn, "daily")
    save_forecast(conn, f1)
    f2 = {**f1, "predicted_duration_s": 99.0, "confidence": "high"}
    save_forecast(conn, f2)
    result = get_forecast(conn, "daily")
    assert result["predicted_duration_s"] == 99.0


def test_list_forecasts_empty(conn):
    assert list_forecasts(conn) == []


def test_list_forecasts_returns_all(conn):
    for job in ["alpha", "beta"]:
        for d in [5, 6, 7]:
            _insert(conn, job, float(d))
        f = compute_forecast(conn, job)
        save_forecast(conn, f)
    rows = list_forecasts(conn)
    names = [r["job_name"] for r in rows]
    assert "alpha" in names
    assert "beta" in names
