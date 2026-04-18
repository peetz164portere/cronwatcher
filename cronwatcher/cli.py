import click
from cronwatcher.storage import get_connection, init_db, record_start, record_finish, fetch_history
from cronwatcher.notify import maybe_notify
from cronwatcher.formatter import format_history_table
from cronwatcher.cli_digest import digest_cmd
from cronwatcher.cli_schedule import check_schedule_cmd
from cronwatcher.cli_tags import tags_cmd
from cronwatcher.cli_export import export_cmd
from cronwatcher.cli_prune import prune_cmd
from cronwatcher.cli_search import search_cmd, find_cmd, stats_cmd
from cronwatcher.cli_compare import compare_cmd
from cronwatcher.cli_watchdog import watchdog_cmd
from cronwatcher.cli_baseline import baseline_cmd
from cronwatcher.cli_heartbeat import heartbeat_cmd
from cronwatcher.cli_labels import labels_cmd
from cronwatcher.cli_dependencies import deps_cmd
import subprocess
import time


@click.group()
def cli():
    """cronwatcher — monitor cron job execution."""


@cli.command()
@click.argument("job_name")
@click.argument("command", nargs=-1, required=True)
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--webhook", default=None)
def run(job_name, command, db, webhook):
    """Run a command and record the result."""
    conn = get_connection(db)
    init_db(conn)
    run_id = record_start(conn, job_name)
    start = time.time()
    try:
        result = subprocess.run(list(command), capture_output=True, text=True)
        duration = time.time() - start
        success = result.returncode == 0
        status = "success" if success else "failure"
        output = result.stdout + result.stderr
        record_finish(conn, run_id, status, duration, output)
        if not success:
            maybe_notify(conn, job_name, run_id, output, duration, webhook)
            raise SystemExit(result.returncode)
    except FileNotFoundError as e:
        duration = time.time() - start
        record_finish(conn, run_id, "failure", duration, str(e))
        maybe_notify(conn, job_name, run_id, str(e), duration, webhook)
        raise SystemExit(1)


@cli.command()
@click.argument("job_name", required=False)
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--limit", default=20, show_default=True)
def history(job_name, db, limit):
    """Show run history."""
    conn = get_connection(db)
    init_db(conn)
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
cli.add_command(watchdog_cmd, "watchdog")
cli.add_command(baseline_cmd, "baseline")
cli.add_command(heartbeat_cmd, "heartbeat")
cli.add_command(labels_cmd, "labels")
cli.add_command(deps_cmd, "deps")
