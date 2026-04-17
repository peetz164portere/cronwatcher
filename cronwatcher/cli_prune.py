"""CLI command for pruning old history."""

import click
from cronwatcher.prune import prune_history, prune_alert_log
from cronwatcher.config import load_config


@click.command("prune")
@click.option("--days", default=30, show_default=True, help="Delete records older than this many days.")
@click.option("--job", default=None, help="Limit pruning to a specific job name.")
@click.option("--alerts", "prune_alerts", is_flag=True, default=False, help="Also prune alert log.")
@click.option("--db", default=None, help="Path to database file.")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be deleted without deleting.")
def prune_cmd(days, job, prune_alerts, db, dry_run):
    """Remove old run history (and optionally alert log) from the database."""
    config = load_config()
    db_path = db or config.get("db_path", "cronwatcher.db")

    if dry_run:
        click.echo(f"[dry-run] Would prune runs older than {days} day(s)" +
                   (f" for job '{job}'" if job else "") + ".")
        if prune_alerts:
            click.echo(f"[dry-run] Would prune alert log entries older than {days} day(s).")
        return

    deleted_runs = prune_history(db_path, older_than_days=days, job_name=job)
    click.echo(f"Pruned {deleted_runs} run record(s) older than {days} day(s)" +
               (f" for job '{job}'" if job else "") + ".")

    if prune_alerts:
        deleted_alerts = prune_alert_log(db_path, older_than_days=days)
        click.echo(f"Pruned {deleted_alerts} alert log entry/entries older than {days} day(s).")
