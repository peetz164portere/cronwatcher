"""Main CLI entry point for cronwatcher."""

import click
from cronwatcher.storage import init_db, record_start, record_finish, fetch_history
from cronwatcher.notify import maybe_notify
from cronwatcher.formatter import format_history_table
from cronwatcher.cli_digest import digest_cmd
from cronwatcher.cli_schedule import check_schedule_cmd
from cronwatcher.cli_tags import tags_cmd
from cronwatcher.cli_export import export_cmd
from cronwatcher.cli_prune import prune_cmd
from cronwatcher.cli_search import search_cmd, find_cmd, stats_cmd
from cronwatcher.cli_compare import compare_cmd
import subprocess
import time


@click.group()
def cli():
    """cronwatcher — monitor cron job execution and alert on failures."""
    pass


@cli.command()
@click.argument("job_name")
@click.argument("command")
@click.option("--db", default="cronwatcher.db", help="Path to SQLite DB")
def run(job_name: str, command: str, db: str):
    """Run a command and record its execution."""
    conn = init_db(db)
    run_id = record_start(conn, job_name)
    start = time.time()
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        duration = time.time() - start
        status = "success" if result.returncode == 0 else "failure"
        output = result.stdout + result.stderr
        record_finish(conn, run_id, status, output, duration)
        if status == "failure":
            maybe_notify(conn, job_name, run_id, output, db)
        click.echo(f"[{status}] {job_name} ({duration:.1f}s)")
    except Exception as e:
        duration = time.time() - start
        record_finish(conn, run_id, "failure", str(e), duration)
        maybe_notify(conn, job_name, run_id, str(e), db)
        raise


@cli.command()
@click.argument("job_name", required=False)
@click.option("--db", default="cronwatcher.db", help="Path to SQLite DB")
@click.option("--limit", default=20, help="Number of rows to show")
def history(job_name: str, db: str, limit: int):
    """Show execution history."""
    conn = init_db(db)
    rows = fetch_history(conn, job_name=job_name, limit=limit)
    click.echo(format_history_table(rows))


cli.add_command(digest_cmd, "digest")
cli.add_command(check_schedule_cmd, "check-schedule")
cli.add_command(tags_cmd, "tags")
cli.add_command(export_cmd, "export")
cli.add_command(prune_cmd, "prune")
cli.add_command(search_cmd, "search")
cli.add_command(find_cmd, "find")
cli.add_command(stats_cmd, "stats")
cli.add_command(compare_cmd, "compare")


if __name__ == "__main__":
    cli()
