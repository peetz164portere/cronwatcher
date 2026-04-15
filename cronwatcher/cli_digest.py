"""CLI commands for digest report functionality."""

import click
import json as _json
from cronwatcher.digest import build_digest, format_digest_text
from cronwatcher.config import load_config
from cronwatcher.webhook import send_webhook


@click.command("digest")
@click.option("--hours", default=24, show_default=True, help="Look-back window in hours.")
@click.option("--format", "output_format", type=click.Choice(["text", "json"]), default="text", show_default=True)
@click.option("--send", is_flag=True, default=False, help="Post digest to configured webhook.")
@click.option("--db", default=None, help="Path to cronwatcher DB (overrides config).")
def digest_cmd(hours: int, output_format: str, send: bool, db: str) -> None:
    """Print a summary digest of cron job activity."""
    config = load_config()
    db_path = db or config.get("db_path", "cronwatcher.db")

    data = build_digest(db_path, hours=hours)

    if output_format == "json":
        click.echo(_json.dumps(data, indent=2))
    else:
        click.echo(format_digest_text(data))

    if send:
        webhook_url = config.get("webhook_url")
        if not webhook_url:
            click.echo("[warn] No webhook_url configured — skipping send.", err=True)
            return
        payload = {
            "event": "digest",
            "period_hours": data["period_hours"],
            "total_runs": data["total_runs"],
            "failed_runs": data["failed_runs"],
            "failure_rate": data["failure_rate"],
            "generated_at": data["generated_at"],
        }
        ok = send_webhook(webhook_url, payload)
        if ok:
            click.echo("[ok] Digest sent to webhook.")
        else:
            click.echo("[error] Failed to send digest to webhook.", err=True)
