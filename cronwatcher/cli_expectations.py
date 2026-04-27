"""CLI commands for managing job expectations."""
import json
import click
from cronwatcher.storage import get_connection
from cronwatcher.expectations import (
    init_expectations, set_expectation, get_expectation,
    remove_expectation, list_expectations,
)


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_expectations(conn)
    return conn


@click.group(name="expectations")
def expectations_cmd():
    """Manage expected duration/frequency for jobs."""


@expectations_cmd.command(name="set")
@click.argument("job_name")
@click.option("--min-duration", type=float, default=None, help="Minimum expected duration (seconds)")
@click.option("--max-duration", type=float, default=None, help="Maximum expected duration (seconds)")
@click.option("--max-interval", type=int, default=None, help="Max seconds between successful runs")
@click.option("--db", default="cronwatcher.db", show_default=True)
def set_cmd(job_name, min_duration, max_duration, max_interval, db):
    """Set expectations for a job."""
    if min_duration is None and max_duration is None and max_interval is None:
        raise click.UsageError("At least one of --min-duration, --max-duration, or --max-interval is required.")
    conn = _get_conn(db)
    set_expectation(conn, job_name, min_duration, max_duration, max_interval)
    click.echo(f"Expectations set for '{job_name}'.")


@expectations_cmd.command(name="show")
@click.argument("job_name")
@click.option("--json", "as_json", is_flag=True)
@click.option("--db", default="cronwatcher.db", show_default=True)
def show_cmd(job_name, as_json, db):
    """Show expectations for a job."""
    conn = _get_conn(db)
    exp = get_expectation(conn, job_name)
    if exp is None:
        click.echo(f"No expectations set for '{job_name}'.", err=True)
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(exp, indent=2))
    else:
        click.echo(f"Job:          {exp['job_name']}")
        click.echo(f"Min duration: {exp['min_duration']}s")
        click.echo(f"Max duration: {exp['max_duration']}s")
        click.echo(f"Max interval: {exp['max_interval_seconds']}s")
        click.echo(f"Updated:      {exp['updated_at']}")


@expectations_cmd.command(name="remove")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def remove_cmd(job_name, db):
    """Remove expectations for a job."""
    conn = _get_conn(db)
    removed = remove_expectation(conn, job_name)
    if removed:
        click.echo(f"Removed expectations for '{job_name}'.")
    else:
        click.echo(f"No expectations found for '{job_name}'.", err=True)
        raise SystemExit(1)


@expectations_cmd.command(name="list")
@click.option("--json", "as_json", is_flag=True)
@click.option("--db", default="cronwatcher.db", show_default=True)
def list_cmd(as_json, db):
    """List all job expectations."""
    conn = _get_conn(db)
    rows = list_expectations(conn)
    if as_json:
        click.echo(json.dumps(rows, indent=2))
    else:
        if not rows:
            click.echo("No expectations configured.")
            return
        click.echo(f"{'JOB':<30} {'MIN':>8} {'MAX':>8} {'INTERVAL':>10}")
        click.echo("-" * 60)
        for r in rows:
            click.echo(f"{r['job_name']:<30} {str(r['min_duration'] or '-'):>8} {str(r['max_duration'] or '-'):>8} {str(r['max_interval_seconds'] or '-'):>10}")
