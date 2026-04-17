"""CLI command for searching cron job history."""
import click
from datetime import datetime
from cronwatcher.storage import get_connection, init_db
from cronwatcher.search import search_history, count_by_status
from cronwatcher.formatter import format_history_table


@click.group()
def search_cmd():
    """Search and filter job history."""
    pass


@search_cmd.command("find")
@click.option("--job", default=None, help="Filter by job name (substring match)")
@click.option("--status", type=click.Choice(["success", "failure", "running"]), default=None)
@click.option("--since", default=None, help="ISO datetime lower bound, e.g. 2024-01-01")
@click.option("--until", default=None, help="ISO datetime upper bound")
@click.option("--limit", default=50, show_default=True, help="Max results")
@click.option("--db", default="cronwatcher.db", show_default=True)
def find_cmd(job, status, since, until, limit, db):
    """Find runs matching filters."""
    conn = get_connection(db)
    init_db(conn)

    since_dt = datetime.fromisoformat(since) if since else None
    until_dt = datetime.fromisoformat(until) if until else None

    rows = search_history(conn, job_name=job, status=status, since=since_dt, until=until_dt, limit=limit)
    if not rows:
        click.echo("No matching runs found.")
        return
    click.echo(format_history_table(rows))


@search_cmd.command("stats")
@click.option("--job", default=None, help="Limit stats to a specific job")
@click.option("--db", default="cronwatcher.db", show_default=True)
def stats_cmd(job, db):
    """Show success/failure counts."""
    conn = get_connection(db)
    init_db(conn)
    counts = count_by_status(conn, job_name=job)
    label = job or "all jobs"
    click.echo(f"Stats for {label}:")
    click.echo(f"  Total:   {counts['total']}")
    click.echo(f"  Success: {counts['success']}")
    click.echo(f"  Failure: {counts['failure']}")
    click.echo(f"  Running: {counts['running']}")
