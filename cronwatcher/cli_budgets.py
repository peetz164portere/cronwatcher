"""CLI commands for managing job run-time budgets."""

import sqlite3
import click
from cronwatcher.storage import get_connection
from cronwatcher import budgets as bmod


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    bmod.init_budgets(conn)
    return conn


@click.group(name="budget")
def budgets_cmd():
    """Manage run-time budgets for cron jobs."""


@budgets_cmd.command(name="set")
@click.argument("job_name")
@click.argument("max_seconds", type=float)
@click.option("--action", default="warn", show_default=True,
              type=click.Choice(["warn", "alert", "block"]),
              help="Action to take when budget is exceeded.")
def set_cmd(job_name: str, max_seconds: float, action: str):
    """Set a run-time budget for JOB_NAME."""
    conn = _get_conn()
    try:
        bmod.set_budget(conn, job_name, max_seconds, action)
        click.echo(f"Budget set: {job_name.lower()} <= {max_seconds}s (action: {action})")
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        raise SystemExit(1)


@budgets_cmd.command(name="show")
@click.argument("job_name")
def show_cmd(job_name: str):
    """Show the budget for JOB_NAME."""
    conn = _get_conn()
    b = bmod.get_budget(conn, job_name)
    if b is None:
        click.echo(f"No budget set for '{job_name}'.")
        raise SystemExit(1)
    click.echo(f"job:        {b['job_name']}")
    click.echo(f"max_seconds:{b['max_seconds']}")
    click.echo(f"action:     {b['action']}")
    click.echo(f"updated:    {b['updated_at']}")


@budgets_cmd.command(name="remove")
@click.argument("job_name")
def remove_cmd(job_name: str):
    """Remove the budget for JOB_NAME."""
    conn = _get_conn()
    removed = bmod.remove_budget(conn, job_name)
    if removed:
        click.echo(f"Budget removed for '{job_name.lower()}'.")
    else:
        click.echo(f"No budget found for '{job_name}'.")
        raise SystemExit(1)


@budgets_cmd.command(name="list")
def list_cmd():
    """List all configured budgets."""
    conn = _get_conn()
    rows = bmod.list_budgets(conn)
    if not rows:
        click.echo("No budgets configured.")
        return
    click.echo(f"{'JOB':<30} {'MAX_SECS':>10} {'ACTION':<8}")
    click.echo("-" * 52)
    for r in rows:
        click.echo(f"{r['job_name']:<30} {r['max_seconds']:>10.1f} {r['action']:<8}")
