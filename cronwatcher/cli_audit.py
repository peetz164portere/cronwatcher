"""CLI commands for the audit log."""
import json
import sqlite3

import click

from cronwatcher.audit import init_audit, get_audit_log, clear_audit_log
from cronwatcher.storage import get_connection


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_audit(conn)
    return conn


@click.group("audit")
def audit_cmd():
    """Audit log commands."""


@audit_cmd.command("list")
@click.option("--action", default=None, help="Filter by action name.")
@click.option("--limit", default=50, show_default=True)
@click.option("--json", "as_json", is_flag=True)
def list_cmd(action, limit, as_json):
    """List recent audit log entries."""
    conn = _get_conn()
    rows = get_audit_log(conn, action=action, limit=limit)
    if as_json:
        click.echo(json.dumps(rows, indent=2))
        return
    if not rows:
        click.echo("No audit entries found.")
        return
    for r in rows:
        click.echo(f"[{r['created_at']}] {r['action']}  target={r['target']}  detail={r['detail']}")


@audit_cmd.command("clear")
@click.confirmation_option(prompt="Clear all audit log entries?")
def clear_cmd():
    """Delete all audit log entries."""
    conn = _get_conn()
    n = clear_audit_log(conn)
    click.echo(f"Cleared {n} audit log entries.")
