"""CLI commands for maintenance window management."""

import click
from cronwatcher.storage import get_connection
from cronwatcher.windows import (
    init_windows,
    add_window,
    remove_window,
    list_windows,
    is_in_maintenance,
    DAYS,
)


def _get_conn():
    conn = get_connection()
    init_windows(conn)
    return conn


@click.group(name="windows")
def windows_cmd():
    """Manage maintenance windows."""


@windows_cmd.command(name="add")
@click.argument("job_name")
@click.argument("day", type=click.Choice(DAYS, case_sensitive=False))
@click.argument("start_time")
@click.argument("end_time")
@click.option("--note", default=None, help="Optional note for this window.")
def add_cmd(job_name, day, start_time, end_time, note):
    """Add a maintenance window for JOB_NAME on DAY from START_TIME to END_TIME (HH:MM)."""
    conn = _get_conn()
    try:
        wid = add_window(conn, job_name, day, start_time, end_time, note)
        click.echo(f"Added window #{wid} for '{job_name}' on {day} {start_time}-{end_time}.")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)


@windows_cmd.command(name="remove")
@click.argument("window_id", type=int)
def remove_cmd(window_id):
    """Remove a maintenance window by ID."""
    conn = _get_conn()
    if remove_window(conn, window_id):
        click.echo(f"Removed window #{window_id}.")
    else:
        click.echo(f"No window found with id {window_id}.", err=True)
        raise SystemExit(1)


@windows_cmd.command(name="list")
@click.option("--job", default=None, help="Filter by job name.")
def list_cmd(job):
    """List all maintenance windows."""
    conn = _get_conn()
    rows = list_windows(conn, job)
    if not rows:
        click.echo("No maintenance windows defined.")
        return
    click.echo(f"{'ID':<5} {'Job':<20} {'Day':<5} {'Start':<7} {'End':<7} Note")
    click.echo("-" * 60)
    for r in rows:
        click.echo(
            f"{r['id']:<5} {r['job_name']:<20} {r['day_of_week']:<5} "
            f"{r['start_time']:<7} {r['end_time']:<7} {r['note'] or ''}"
        )


@windows_cmd.command(name="check")
@click.argument("job_name")
def check_cmd(job_name):
    """Check whether JOB_NAME is currently in a maintenance window."""
    conn = _get_conn()
    if is_in_maintenance(conn, job_name):
        click.echo(f"'{job_name}' is currently in a maintenance window.")
    else:
        click.echo(f"'{job_name}' is NOT in a maintenance window.")
