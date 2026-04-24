"""CLI commands for pausing and resuming cron jobs."""

import sqlite3
import click
from cronwatcher.storage import get_connection
from cronwatcher import pauses


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    pauses.init_pauses(conn)
    return conn


@click.group("pauses")
def pauses_cmd():
    """Pause or resume alerting for jobs."""


@pauses_cmd.command("pause")
@click.argument("job_name")
@click.option("--reason", default=None, help="Optional reason for pausing.")
def pause_cmd(job_name: str, reason: str):
    """Pause alerting for JOB_NAME."""
    conn = _get_conn()
    pauses.pause_job(conn, job_name, reason)
    click.echo(f"Paused '{job_name}'." + (f" Reason: {reason}" if reason else ""))


@pauses_cmd.command("resume")
@click.argument("job_name")
def resume_cmd(job_name: str):
    """Resume alerting for JOB_NAME."""
    conn = _get_conn()
    removed = pauses.resume_job(conn, job_name)
    if removed:
        click.echo(f"Resumed '{job_name}'.")
    else:
        click.echo(f"'{job_name}' was not paused.", err=True)
        raise SystemExit(1)


@pauses_cmd.command("status")
@click.argument("job_name")
def status_cmd(job_name: str):
    """Show pause status for JOB_NAME."""
    conn = _get_conn()
    info = pauses.get_pause_info(conn, job_name)
    if info is None:
        click.echo(f"'{job_name}' is not paused.")
    else:
        reason_str = f"  reason : {info['reason']}" if info["reason"] else ""
        click.echo(f"'{job_name}' is PAUSED since {info['paused_at']}.{reason_str}")


@pauses_cmd.command("list")
def list_cmd():
    """List all paused jobs."""
    conn = _get_conn()
    rows = pauses.list_paused(conn)
    if not rows:
        click.echo("No jobs are currently paused.")
        return
    for r in rows:
        reason_part = f"  [{r['reason']}]" if r["reason"] else ""
        click.echo(f"{r['job_name']:<30} paused_at={r['paused_at']}{reason_part}")
