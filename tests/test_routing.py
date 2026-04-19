import pytest
import sqlite3
from cronwatcher.routing import (
    init_routing, set_route, get_route, remove_route, list_routes, resolve_webhook
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_routing(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [t[0] for t in tables]
    assert "job_routes" in names


def test_set_and_get_route(conn):
    set_route(conn, "backup", "https://hooks.example.com/backup")
    assert get_route(conn, "backup") == "https://hooks.example.com/backup"


def test_set_route_normalizes_case(conn):
    set_route(conn, "MyJob", "https://hooks.example.com/myjob")
    assert get_route(conn, "myjob") == "https://hooks.example.com/myjob"
    assert get_route(conn, "MYJOB") == "https://hooks.example.com/myjob"


def test_get_route_missing_returns_none(conn):
    assert get_route(conn, "nonexistent") is None


def test_set_route_overwrites_existing(conn):
    set_route(conn, "backup", "https://old.example.com")
    set_route(conn, "backup", "https://new.example.com")
    assert get_route(conn, "backup") == "https://new.example.com"


def test_remove_route_returns_true(conn):
    set_route(conn, "backup", "https://hooks.example.com/backup")
    assert remove_route(conn, "backup") is True
    assert get_route(conn, "backup") is None


def test_remove_route_missing_returns_false(conn):
    assert remove_route(conn, "nonexistent") is False


def test_list_routes_empty(conn):
    assert list_routes(conn) == []


def test_list_routes_returns_all(conn):
    set_route(conn, "job_a", "https://a.example.com")
    set_route(conn, "job_b", "https://b.example.com")
    routes = list_routes(conn)
    assert len(routes) == 2
    names = [r["job_name"] for r in routes]
    assert "job_a" in names
    assert "job_b" in names


def test_resolve_webhook_uses_job_route(conn):
    set_route(conn, "backup", "https://specific.example.com")
    result = resolve_webhook(conn, "backup", "https://default.example.com")
    assert result == "https://specific.example.com"


def test_resolve_webhook_falls_back_to_default(conn):
    result = resolve_webhook(conn, "unknown_job", "https://default.example.com")
    assert result == "https://default.example.com"


def test_resolve_webhook_returns_none_when_no_default(conn):
    result = resolve_webhook(conn, "unknown_job", None)
    assert result is None
