"""CLI commands for managing retention policies."""

import sqlite3
import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.retention import (
    init_retention, set_retention, get_retention,
    remove_retention, list_retention, apply_retention
)


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = get_connection(db_path)
    init_db(conn)
    init_retention(conn)
    return conn


@click.group("retention")
def retention_cmd():
    """Manage data retention policies for cron jobs."""


@retention_cmd.command("set")
@click.argument("job_name")
@click.option("--days", required=True, type=int, help="Maximum age of runs in days.")
@click.option("--max-runs", type=int, default=None, help="Maximum number of runs to keep.")
@click.option("--db", default="cronwatcher.db", show_default=True)
def set_cmd(job_name, days, max_runs, db):
    """Set retention policy for a job."""
    try:
        conn = _get_conn(db)
        set_retention(conn, job_name, days, max_runs)
        click.echo(f"Retention policy set for '{job_name}': {days} days" +
                   (f", max {max_runs} runs" if max_runs else ""))
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)


@retention_cmd.command("show")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def show_cmd(job_name, db):
    """Show retention policy for a job."""
    conn = _get_conn(db)
    policy = get_retention(conn, job_name)
    if policy is None:
        click.echo(f"No retention policy set for '{job_name}'.")
        raise SystemExit(1)
    click.echo(f"job:      {policy['job_name']}")
    click.echo(f"max_days: {policy['max_days']}")
    click.echo(f"max_runs: {policy['max_runs'] or 'unlimited'}")
    click.echo(f"updated:  {policy['updated_at']}")


@retention_cmd.command("remove")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def remove_cmd(job_name, db):
    """Remove retention policy for a job."""
    conn = _get_conn(db)
    removed = remove_retention(conn, job_name)
    if removed:
        click.echo(f"Retention policy removed for '{job_name}'.")
    else:
        click.echo(f"No policy found for '{job_name}'.")
        raise SystemExit(1)


@retention_cmd.command("list")
@click.option("--db", default="cronwatcher.db", show_default=True)
def list_cmd(db):
    """List all retention policies."""
    conn = _get_conn(db)
    policies = list_retention(conn)
    if not policies:
        click.echo("No retention policies configured.")
        return
    click.echo(f"{'JOB':<30} {'MAX DAYS':>10} {'MAX RUNS':>10}")
    click.echo("-" * 54)
    for p in policies:
        click.echo(f"{p['job_name']:<30} {p['max_days']:>10} {str(p['max_runs'] or 'unlimited'):>10}")


@retention_cmd.command("apply")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def apply_cmd(job_name, db):
    """Apply retention policy for a job, deleting old runs."""
    conn = _get_conn(db)
    deleted = apply_retention(conn, job_name)
    click.echo(f"Deleted {deleted} run(s) for '{job_name}' per retention policy.")
