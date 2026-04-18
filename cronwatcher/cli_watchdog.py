"""CLI command for watchdog: detect and resolve hung cron runs."""

import click
import json as _json
from cronwatcher.watchdog import resolve_hung_runs
from cronwatcher.config import load_config


@click.command("watchdog")
@click.option("--db", default=None, help="Path to the SQLite database.")
@click.option("--timeout", default=60, show_default=True, help="Minutes before a run is considered hung.")
@click.option("--dry-run", is_flag=True, default=False, help="Report hung runs without modifying them.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output as JSON.")
def watchdog_cmd(db, timeout, dry_run, as_json):
    """Detect runs that started but never finished."""
    cfg = load_config()
    db_path = db or cfg.get("db_path", "cronwatcher.db")

    hung = resolve_hung_runs(db_path, timeout_minutes=timeout, dry_run=dry_run)

    if as_json:
        click.echo(_json.dumps(hung, indent=2))
        raise SystemExit(1 if hung else 0)

    if not hung:
        click.echo("No hung runs detected.")
        raise SystemExit(0)

    action = "(dry-run, not modified)" if dry_run else "(marked as failed)"
    click.echo(f"Found {len(hung)} hung run(s) {action}:")
    for r in hung:
        click.echo(f"  [{r['id']}] {r['job_name']} — started {r['started_at']}")
    raise SystemExit(1)
