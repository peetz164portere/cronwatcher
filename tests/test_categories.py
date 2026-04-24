"""Tests for cronwatcher/categories.py"""

import sqlite3
import pytest

from cronwatcher.categories import (
    init_categories,
    set_category,
    get_category,
    remove_category,
    list_categories,
    get_jobs_in_category,
    DEFAULT_CATEGORY,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_categories(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='categories'"
    ).fetchall()
    assert len(tables) == 1


def test_get_category_default_when_unset(conn):
    assert get_category(conn, "backup") == DEFAULT_CATEGORY


def test_set_and_get_category(conn):
    set_category(conn, "backup", "maintenance")
    assert get_category(conn, "backup") == "maintenance"


def test_set_category_normalizes_case(conn):
    set_category(conn, "BackupJob", "Maintenance")
    assert get_category(conn, "backupjob") == "maintenance"


def test_set_category_overwrites_existing(conn):
    set_category(conn, "sync", "data")
    set_category(conn, "sync", "network")
    assert get_category(conn, "sync") == "network"


def test_set_category_with_description(conn):
    set_category(conn, "report", "analytics", description="Monthly reports")
    rows = list_categories(conn)
    assert rows[0]["description"] == "Monthly reports"


def test_remove_category_returns_true(conn):
    set_category(conn, "cleanup", "maintenance")
    result = remove_category(conn, "cleanup")
    assert result is True
    assert get_category(conn, "cleanup") == DEFAULT_CATEGORY


def test_remove_category_missing_returns_false(conn):
    result = remove_category(conn, "nonexistent")
    assert result is False


def test_list_categories_empty(conn):
    assert list_categories(conn) == []


def test_list_categories_returns_all(conn):
    set_category(conn, "alpha", "group-a")
    set_category(conn, "beta", "group-b")
    rows = list_categories(conn)
    assert len(rows) == 2
    names = {r["job_name"] for r in rows}
    assert names == {"alpha", "beta"}


def test_list_categories_sorted_by_category_then_job(conn):
    set_category(conn, "z-job", "aaa")
    set_category(conn, "a-job", "aaa")
    set_category(conn, "m-job", "bbb")
    rows = list_categories(conn)
    assert rows[0]["job_name"] == "a-job"
    assert rows[1]["job_name"] == "z-job"
    assert rows[2]["job_name"] == "m-job"


def test_get_jobs_in_category_empty(conn):
    assert get_jobs_in_category(conn, "missing") == []


def test_get_jobs_in_category_returns_correct_jobs(conn):
    set_category(conn, "job1", "infra")
    set_category(conn, "job2", "infra")
    set_category(conn, "job3", "data")
    result = get_jobs_in_category(conn, "infra")
    assert set(result) == {"job1", "job2"}


def test_get_jobs_in_category_normalizes_case(conn):
    set_category(conn, "myjob", "infra")
    result = get_jobs_in_category(conn, "INFRA")
    assert "myjob" in result
