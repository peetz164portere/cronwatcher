"""Tests for cronwatcher/circuits.py"""

import sqlite3
import pytest
from cronwatcher import circuits


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    circuits.init_circuits(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [t["name"] for t in tables]
    assert "circuit_breakers" in names


def test_get_circuit_none_when_empty(conn):
    assert circuits.get_circuit(conn, "backup") is None


def test_record_failure_creates_entry(conn):
    result = circuits.record_failure(conn, "backup")
    assert result["failure_count"] == 1
    assert result["state"] == "closed"


def test_record_failure_normalizes_case(conn):
    circuits.record_failure(conn, "Backup")
    result = circuits.get_circuit(conn, "backup")
    assert result is not None


def test_circuit_opens_at_threshold(conn):
    for _ in range(3):
        result = circuits.record_failure(conn, "myjob")
    assert result["state"] == "open"


def test_circuit_stays_closed_below_threshold(conn):
    for _ in range(2):
        result = circuits.record_failure(conn, "myjob")
    assert result["state"] == "closed"


def test_record_success_resets_state(conn):
    for _ in range(3):
        circuits.record_failure(conn, "myjob")
    result = circuits.record_success(conn, "myjob")
    assert result["state"] == "closed"
    assert result["failure_count"] == 0


def test_is_open_false_when_no_data(conn):
    assert circuits.is_open(conn, "unknown") is False


def test_is_open_true_when_open(conn):
    for _ in range(3):
        circuits.record_failure(conn, "myjob")
    assert circuits.is_open(conn, "myjob") is True


def test_is_open_false_after_recovery(conn):
    for _ in range(3):
        circuits.record_failure(conn, "myjob")
    # force opened_at to old time
    conn.execute(
        "UPDATE circuit_breakers SET opened_at = '2000-01-01T00:00:00' WHERE job_name = 'myjob'"
    )
    conn.commit()
    assert circuits.is_open(conn, "myjob", recovery_seconds=300) is False
    circuit = circuits.get_circuit(conn, "myjob")
    assert circuit["state"] == "half_open"


def test_reset_circuit_removes_entry(conn):
    circuits.record_failure(conn, "myjob")
    circuits.reset_circuit(conn, "myjob")
    assert circuits.get_circuit(conn, "myjob") is None


def test_list_circuits_empty(conn):
    assert circuits.list_circuits(conn) == []


def test_list_circuits_returns_all(conn):
    circuits.record_failure(conn, "job_a")
    circuits.record_failure(conn, "job_b")
    rows = circuits.list_circuits(conn)
    names = [r["job_name"] for r in rows]
    assert "job_a" in names
    assert "job_b" in names


def test_custom_threshold(conn):
    for _ in range(5):
        result = circuits.record_failure(conn, "myjob", threshold=5)
    assert result["state"] == "open"
    result2 = circuits.record_failure(conn, "otherjob", threshold=5)
    assert result2["state"] == "closed"
