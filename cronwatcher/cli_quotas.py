"""CLI commands for managing job run quotas."""

import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.quotas import init_quotas, set_quota, get_quota, remove_quota, list_quotas, is_quota_exceeded


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_db(conn)
    init_quotas(conn)
    return conn


@click.group("quotas")
def quotas_cmd():
    """Manage per-job run quotas."""


@quotas_cmd.command("set")
@click.argument("job_name")
@click.option("--max-runs", required=True, type=int, help="Max allowed runs in window")
@click.option("--window", required=True, type=int, help="Window size in seconds")
@click.option("--db", default="cronwatcher.db", show_default=True)
def set_cmd(job_name, max_runs, window, db):
    """Set a quota for a job."""
    conn = _get_conn(db)
    set_quota(conn, job_name, max_runs, window)
    click.echo(f"Quota set: {job_name} — {max_runs} runs per {window}s")


@quotas_cmd.command("remove")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def remove_cmd(job_name, db):
    """Remove quota for a job."""
    conn = _get_conn(db)
    remove_quota(conn, job_name)
    click.echo(f"Quota removed for {job_name}")


@quotas_cmd.command("show")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def show_cmd(job_name, db):
    """Show quota for a job."""
    conn = _get_conn(db)
    q = get_quota(conn, job_name)
    if q is None:
        click.echo(f"No quota set for {job_name}")
        raise SystemExit(1)
    exceeded = is_quota_exceeded(conn, job_name)
    status = "EXCEEDED" if exceeded else "ok"
    click.echo(f"{q['job_name']}: {q['max_runs']} runs / {q['window_seconds']}s [{status}]")


@quotas_cmd.command("list")
@click.option("--db", default="cronwatcher.db", show_default=True)
def list_cmd(db):
    """List all quotas."""
    conn = _get_conn(db)
    quotas = list_quotas(conn)
    if not quotas:
        click.echo("No quotas configured.")
        return
    for q in quotas:
        click.echo(f"{q['job_name']}: {q['max_runs']} runs / {q['window_seconds']}s")
