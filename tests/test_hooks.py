import sqlite3
import pytest
from cronwatcher.hooks import init_hooks, add_hook, get_hooks, remove_hook, list_all_hooks


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_hooks(c)
    yield c
    c.close()


def test_init_creates_table(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hooks'")
    assert cur.fetchone() is not None


def test_add_hook_returns_id(conn):
    hid = add_hook(conn, "backup", "pre", "echo starting")
    assert isinstance(hid, int)
    assert hid > 0


def test_add_hook_invalid_type_raises(conn):
    with pytest.raises(ValueError):
        add_hook(conn, "backup", "during", "echo nope")


def test_get_hooks_empty(conn):
    result = get_hooks(conn, "backup", "pre")
    assert result == []


def test_get_hooks_returns_correct_entries(conn):
    add_hook(conn, "backup", "pre", "echo before")
    add_hook(conn, "backup", "post", "echo after")
    pre = get_hooks(conn, "backup", "pre")
    assert len(pre) == 1
    assert pre[0]["command"] == "echo before"
    assert pre[0]["hook_type"] == "pre"


def test_get_hooks_normalizes_job_name(conn):
    add_hook(conn, "BACKUP", "pre", "echo hi")
    result = get_hooks(conn, "backup", "pre")
    assert len(result) == 1


def test_remove_hook_returns_true(conn):
    hid = add_hook(conn, "sync", "post", "echo done")
    assert remove_hook(conn, hid) is True
    assert get_hooks(conn, "sync", "post") == []


def test_remove_hook_missing_returns_false(conn):
    assert remove_hook(conn, 9999) is False


def test_list_all_hooks_no_filter(conn):
    add_hook(conn, "job_a", "pre", "echo a")
    add_hook(conn, "job_b", "post", "echo b")
    all_hooks = list_all_hooks(conn)
    assert len(all_hooks) == 2


def test_list_all_hooks_with_filter(conn):
    add_hook(conn, "job_a", "pre", "echo a")
    add_hook(conn, "job_b", "post", "echo b")
    result = list_all_hooks(conn, job_name="job_a")
    assert len(result) == 1
    assert result[0]["job_name"] == "job_a"


def test_multiple_hooks_same_type(conn):
    add_hook(conn, "deploy", "pre", "echo step1")
    add_hook(conn, "deploy", "pre", "echo step2")
    hooks = get_hooks(conn, "deploy", "pre")
    assert len(hooks) == 2
    assert hooks[0]["command"] == "echo step1"
    assert hooks[1]["command"] == "echo step2"
