"""CLI commands for managing grievances (persistent failure tracking)."""

import json
import click
from cronwatcher.storage import get_connection
from cronwatcher.grievances import init_grievances, record_failure, resolve_grievance, get_grievance, list_grievances


def _get_conn():
    conn = get_connection()
    init_grievances(conn)
    return conn


@click.group("grievances")
def grievances_cmd():
    """Track and manage persistent job failure patterns."""


@grievances_cmd.command("record")
@click.argument("job_name")
def record_cmd(job_name):
    """Record a failure for a job."""
    conn = _get_conn()
    gid = record_failure(conn, job_name)
    g = get_grievance(conn, job_name)
    click.echo(f"Grievance #{gid} updated for '{job_name}' — total failures: {g['failure_count']}")


@grievances_cmd.command("resolve")
@click.argument("job_name")
def resolve_cmd(job_name):
    """Mark a job's grievance as resolved."""
    conn = _get_conn()
    ok = resolve_grievance(conn, job_name)
    if ok:
        click.echo(f"Grievance for '{job_name}' resolved.")
    else:
        click.echo(f"No active grievance found for '{job_name}'.")
        raise SystemExit(1)


@grievances_cmd.command("show")
@click.argument("job_name")
@click.option("--json", "as_json", is_flag=True)
def show_cmd(job_name, as_json):
    """Show active grievance for a job."""
    conn = _get_conn()
    g = get_grievance(conn, job_name)
    if not g:
        click.echo(f"No active grievance for '{job_name}'.")
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(g, indent=2))
    else:
        click.echo(f"Job: {g['job_name']}  Failures: {g['failure_count']}  First: {g['first_seen']}  Last: {g['last_seen']}")


@grievances_cmd.command("list")
@click.option("--all", "include_resolved", is_flag=True, help="Include resolved grievances")
@click.option("--json", "as_json", is_flag=True)
def list_cmd(include_resolved, as_json):
    """List grievances."""
    conn = _get_conn()
    items = list_grievances(conn, include_resolved=include_resolved)
    if as_json:
        click.echo(json.dumps(items, indent=2))
        return
    if not items:
        click.echo("No grievances found.")
        return
    for g in items:
        status = "resolved" if g["resolved"] else "active"
        click.echo(f"[{status}] {g['job_name']}  failures={g['failure_count']}  last={g['last_seen']}")
