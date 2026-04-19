import json
import pytest
import sqlite3
from cronwatcher.storage import init_db, record_start, record_finish
from cronwatcher.badges import get_job_status, build_badge, get_badge, badge_json, STATUS_COLORS


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "test.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    init_db(c)
    return c


@pytest.fixture
def tmp_db(tmp_path):
    db = tmp_path / "test.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    init_db(c)
    c.close()
    return str(db)


def test_get_job_status_unknown_when_empty(conn):
    assert get_job_status(conn, "myjob") == "unknown"


def test_get_job_status_success(conn):
    run_id = record_start(conn, "myjob")
    record_finish(conn, run_id, "success")
    assert get_job_status(conn, "myjob") == "success"


def test_get_job_status_failure(conn):
    run_id = record_start(conn, "myjob")
    record_finish(conn, run_id, "failure", error="boom")
    assert get_job_status(conn, "myjob") == "failure"


def test_get_job_status_returns_latest(conn):
    r1 = record_start(conn, "myjob")
    record_finish(conn, r1, "success")
    r2 = record_start(conn, "myjob")
    record_finish(conn, r2, "failure", error="oops")
    assert get_job_status(conn, "myjob") == "failure"


def test_build_badge_structure():
    badge = build_badge("backup", "success")
    assert badge["schemaVersion"] == 1
    assert badge["label"] == "backup"
    assert badge["message"] == "success"
    assert badge["color"] == "brightgreen"


def test_build_badge_unknown_status():
    badge = build_badge("backup", "weird")
    assert badge["color"] == "lightgrey"
    assert badge["message"] == "weird"


def test_all_known_statuses_have_colors():
    for status in ["success", "failure", "running", "unknown"]:
        badge = build_badge("job", status)
        assert badge["color"] == STATUS_COLORS[status]


def test_get_badge_end_to_end(tmp_db, conn):
    run_id = record_start(conn, "etl")
    record_finish(conn, run_id, "success")
    conn.close()
    badge = get_badge(tmp_db, "etl")
    assert badge["message"] == "success"
    assert badge["label"] == "etl"


def test_badge_json_is_valid_json(tmp_db, conn):
    run_id = record_start(conn, "etl")
    record_finish(conn, run_id, "failure", error="err")
    conn.close()
    result = badge_json(tmp_db, "etl")
    parsed = json.loads(result)
    assert parsed["message"] == "failure"
    assert parsed["color"] == "red"
