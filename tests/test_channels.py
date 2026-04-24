"""Tests for cronwatcher.channels."""
import sqlite3
import pytest
from cronwatcher.channels import (
    init_channels, add_channel, get_channel, remove_channel,
    set_enabled, list_channels,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_channels(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "channels" in tables


def test_add_channel_returns_id(conn):
    row_id = add_channel(conn, "my-slack", "slack", {"url": "https://hooks.slack.com/x"})
    assert isinstance(row_id, int)
    assert row_id > 0


def test_add_channel_normalizes_name(conn):
    add_channel(conn, "MySlack", "slack")
    ch = get_channel(conn, "myslack")
    assert ch is not None
    assert ch["name"] == "myslack"


def test_add_channel_invalid_type_raises(conn):
    with pytest.raises(ValueError, match="Invalid channel type"):
        add_channel(conn, "bad", "sms")


def test_add_channel_duplicate_ignored(conn):
    add_channel(conn, "alerts", "webhook")
    add_channel(conn, "alerts", "email")  # duplicate name, should be ignored
    ch = get_channel(conn, "alerts")
    assert ch["type"] == "webhook"  # first one wins


def test_get_channel_returns_dict(conn):
    add_channel(conn, "pd", "pagerduty", {"api_key": "abc"})
    ch = get_channel(conn, "pd")
    assert ch["type"] == "pagerduty"
    assert ch["config"] == {"api_key": "abc"}
    assert ch["enabled"] is True


def test_get_channel_missing_returns_none(conn):
    assert get_channel(conn, "nonexistent") is None


def test_remove_channel_returns_true(conn):
    add_channel(conn, "tmp", "email")
    assert remove_channel(conn, "tmp") is True
    assert get_channel(conn, "tmp") is None


def test_remove_channel_missing_returns_false(conn):
    assert remove_channel(conn, "ghost") is False


def test_set_enabled_false(conn):
    add_channel(conn, "ch1", "slack")
    set_enabled(conn, "ch1", False)
    ch = get_channel(conn, "ch1")
    assert ch["enabled"] is False


def test_set_enabled_true(conn):
    add_channel(conn, "ch2", "teams")
    set_enabled(conn, "ch2", False)
    set_enabled(conn, "ch2", True)
    ch = get_channel(conn, "ch2")
    assert ch["enabled"] is True


def test_list_channels_empty(conn):
    assert list_channels(conn) == []


def test_list_channels_returns_all(conn):
    add_channel(conn, "alpha", "slack")
    add_channel(conn, "beta", "email")
    channels = list_channels(conn)
    names = [c["name"] for c in channels]
    assert "alpha" in names
    assert "beta" in names


def test_list_channels_sorted_by_name(conn):
    add_channel(conn, "zzz", "slack")
    add_channel(conn, "aaa", "email")
    names = [c["name"] for c in list_channels(conn)]
    assert names == sorted(names)
