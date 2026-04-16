"""CLI command for exporting cron job history."""
import click
from cronwatcher.storage import get_connection, init_db, fetch_history
from cronwatcher.export import export_history


@click.command("export")
@click.option("--db", default="cronwatcher.db", show_default=True, help="Path to SQLite database.")
@click.option("--job", default=None, help="Filter by job name.")
@click.option("--limit", default=100, show_default=True, help="Max number of records to export.")
@click.option(
    "--format", "fmt",
    type=click.Choice(["json", "csv"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Output format.",
)
@click.option("--output", "-o", default=None, help="Write output to file instead of stdout.")
def export_cmd(db, job, limit, fmt, output):
    """Export cron job history to JSON or CSV."""
    conn = get_connection(db)
    init_db(conn)
    rows = fetch_history(conn, job_name=job, limit=limit)
    result = export_history(rows, fmt=fmt)

    if output:
        with open(output, "w") as f:
            f.write(result)
        click.echo(f"Exported {len(rows)} records to {output}")
    else:
        click.echo(result)
