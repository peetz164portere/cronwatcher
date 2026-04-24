"""CLI commands for managing per-job failure thresholds."""

import click
import sqlite3
from cronwatcher.storage import get_connection, init_db
from cronwatcher.thresholds import (
    init_thresholds, set_threshold, get_threshold,
    remove_threshold, get_streak, is_threshold_exceeded,
)


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_db(conn)
    init_thresholds(conn)
    return conn


@click.group("thresholds")
def thresholds_cmd():
    """Manage consecutive-failure thresholds per job."""


@thresholds_cmd.command("set")
@click.argument("job_name")
@click.argument("max_failures", type=int)
def set_cmd(job_name: str, max_failures: int):
    """Set the max consecutive failures before alerting."""
    if max_failures < 1:
        click.echo("Error: max_failures must be >= 1", err=True)
        raise SystemExit(1)
    conn = _get_conn()
    set_threshold(conn, job_name, max_failures)
    click.echo(f"Threshold for '{job_name}' set to {max_failures} consecutive failures.")


@thresholds_cmd.command("show")
@click.argument("job_name")
def show_cmd(job_name: str):
    """Show the threshold and current streak for a job."""
    conn = _get_conn()
    threshold = get_threshold(conn, job_name)
    streak = get_streak(conn, job_name)
    exceeded = is_threshold_exceeded(conn, job_name)
    if threshold is None:
        click.echo(f"No threshold set for '{job_name}'. Current streak: {streak}.")
    else:
        status = " [EXCEEDED]" if exceeded else ""
        click.echo(
            f"Job: {job_name}  threshold: {threshold}  streak: {streak}{status}"
        )


@thresholds_cmd.command("remove")
@click.argument("job_name")
def remove_cmd(job_name: str):
    """Remove the threshold for a job."""
    conn = _get_conn()
    remove_threshold(conn, job_name)
    click.echo(f"Threshold for '{job_name}' removed.")


@thresholds_cmd.command("list")
def list_cmd():
    """List all configured thresholds."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT job_name, max_failures FROM thresholds ORDER BY job_name"
    ).fetchall()
    if not rows:
        click.echo("No thresholds configured.")
        return
    click.echo(f"{'JOB':<30} {'MAX_FAILURES':>12}")
    click.echo("-" * 44)
    for job, max_f in rows:
        click.echo(f"{job:<30} {max_f:>12}")
