"""Tests for cronwatcher/scorecards.py."""

import sqlite3
import pytest

from cronwatcher.storage import init_db
from cronwatcher.scorecards import (
    init_scorecards,
    compute_score,
    refresh_scorecard,
    get_scorecard,
    list_scorecards,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_db(c)
    init_scorecards(c)
    return c


def _insert_run(conn, job, status, duration=5.0):
    conn.execute(
        "INSERT INTO runs (job_name, status, started_at, finished_at, duration) "
        "VALUES (?, ?, datetime('now'), datetime('now'), ?)",
        (job, status, duration),
    )
    conn.commit()


def test_init_creates_table(conn):
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "scorecards" in tables


def test_compute_score_no_runs():
    assert compute_score(0, 0) == 100.0


def test_compute_score_all_success():
    assert compute_score(10, 0) == 100.0


def test_compute_score_all_failure():
    assert compute_score(10, 10) == 0.0


def test_compute_score_mixed():
    assert compute_score(4, 1) == 75.0


def test_refresh_scorecard_no_runs(conn):
    score = refresh_scorecard(conn, "backup")
    assert score == 100.0
    card = get_scorecard(conn, "backup")
    assert card is not None
    assert card["runs"] == 0
    assert card["failures"] == 0


def test_refresh_scorecard_with_runs(conn):
    _insert_run(conn, "backup", "success")
    _insert_run(conn, "backup", "success")
    _insert_run(conn, "backup", "failure")
    score = refresh_scorecard(conn, "backup")
    assert score == pytest.approx(66.67, abs=0.01)


def test_refresh_scorecard_normalizes_case(conn):
    _insert_run(conn, "backup", "success")
    refresh_scorecard(conn, "BACKUP")
    card = get_scorecard(conn, "backup")
    assert card is not None


def test_refresh_scorecard_updates_existing(conn):
    _insert_run(conn, "sync", "success")
    refresh_scorecard(conn, "sync")
    _insert_run(conn, "sync", "failure")
    score = refresh_scorecard(conn, "sync")
    assert score == 50.0
    card = get_scorecard(conn, "sync")
    assert card["runs"] == 2


def test_get_scorecard_missing_returns_none(conn):
    assert get_scorecard(conn, "ghost") is None


def test_list_scorecards_empty(conn):
    assert list_scorecards(conn) == []


def test_list_scorecards_ordered_by_score(conn):
    for job, successes, failures in [("a", 10, 0), ("b", 5, 5), ("c", 8, 2)]:
        for _ in range(successes):
            _insert_run(conn, job, "success")
        for _ in range(failures):
            _insert_run(conn, job, "failure")
        refresh_scorecard(conn, job)
    cards = list_scorecards(conn)
    scores = [c["score"] for c in cards]
    assert scores == sorted(scores)
    assert cards[0]["job_name"] == "b"
