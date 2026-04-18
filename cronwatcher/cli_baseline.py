"""CLI commands for baseline management."""

import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.baseline import init_baseline, update_baseline, get_baseline


@click.group("baseline")
def baseline_cmd():
    """Manage job duration baselines."""


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_db(conn)
    init_baseline(conn)
    return conn


@baseline_cmd.command("update")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def update_cmd(job_name, db):
    """Recalculate and store baseline avg duration for a job."""
    conn = _get_conn(db)
    avg = update_baseline(conn, job_name)
    if avg is None:
        click.echo(f"No successful runs found for '{job_name}'.")
    else:
        click.echo(f"Baseline updated for '{job_name}': avg={avg:.2f}s")


@baseline_cmd.command("show")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def show_cmd(job_name, db):
    """Show stored baseline for a job."""
    conn = _get_conn(db)
    b = get_baseline(conn, job_name)
    if not b:
        click.echo(f"No baseline found for '{job_name}'.")
        raise SystemExit(1)
    click.echo(f"job:     {b['job_name']}")
    click.echo(f"avg:     {b['avg_duration']:.2f}s")
    click.echo(f"samples: {b['sample_count']}")
    click.echo(f"updated: {b['updated_at']}")
