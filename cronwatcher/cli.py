import sys
import click
from cronwatcher.storage import init_db, record_start, record_finish, fetch_history
from cronwatcher.config import load_config, should_alert
from cronwatcher.webhook import notify_failure


@click.group()
def cli():
    """cronwatcher — monitor cron job execution history and alert on failures."""
    init_db()


@cli.command()
@click.argument("job_name")
@click.argument("command")
def run(job_name, command):
    """Run a command as a tracked cron job."""
    import subprocess
    import time

    run_id = record_start(job_name, command)
    click.echo(f"[cronwatcher] Starting job '{job_name}' (run_id={run_id})")

    start = time.time()
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    duration = time.time() - start

    exit_code = result.returncode
    output = result.stdout + result.stderr

    record_finish(run_id, exit_code, output)

    if exit_code != 0:
        click.echo(f"[cronwatcher] Job '{job_name}' FAILED (exit_code={exit_code})")
        config = load_config()
        if should_alert(config, job_name):
            notify_failure(config, job_name, command, exit_code, duration, output)
    else:
        click.echo(f"[cronwatcher] Job '{job_name}' succeeded in {duration:.2f}s")

    sys.exit(exit_code)


@cli.command()
@click.argument("job_name", required=False)
@click.option("--limit", default=20, show_default=True, help="Number of records to show")
def history(job_name, limit):
    """Show execution history for a job (or all jobs)."""
    rows = fetch_history(job_name=job_name, limit=limit)
    if not rows:
        click.echo("No history found.")
        return

    click.echo(f"{'ID':<6} {'Job':<20} {'Status':<10} {'Exit':<6} {'Started':<22} {'Duration'}")
    click.echo("-" * 80)
    for row in rows:
        run_id, name, cmd, status, exit_code, started_at, finished_at, duration = row
        duration_str = f"{duration:.2f}s" if duration is not None else "running"
        exit_str = str(exit_code) if exit_code is not None else "-"
        click.echo(f"{run_id:<6} {name:<20} {status:<10} {exit_str:<6} {str(started_at):<22} {duration_str}")


if __name__ == "__main__":
    cli()
