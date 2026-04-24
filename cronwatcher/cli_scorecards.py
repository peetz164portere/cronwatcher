"""CLI commands for scorecards."""

import json
import sqlite3

import click

from cronwatcher.scorecards import init_scorecards, refresh_scorecard, get_scorecard, list_scorecards
from cronwatcher.storage import get_connection, init_db


def _get_conn(db: str) -> sqlite3.Connection:
    conn = get_connection(db)
    init_db(conn)
    init_scorecards(conn)
    return conn


@click.group("scorecards")
def scorecards_cmd():
    """Reliability scorecards for cron jobs."""


@scorecards_cmd.command("refresh")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", envvar="CRONWATCHER_DB")
def refresh_cmd(job_name: str, db: str):
    """Recompute the reliability score for JOB_NAME."""
    conn = _get_conn(db)
    score = refresh_scorecard(conn, job_name)
    click.echo(f"Score for '{job_name}': {score}/100")


@scorecards_cmd.command("show")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", envvar="CRONWATCHER_DB")
@click.option("--json", "as_json", is_flag=True)
def show_cmd(job_name: str, db: str, as_json: bool):
    """Show the scorecard for JOB_NAME."""
    conn = _get_conn(db)
    card = get_scorecard(conn, job_name)
    if card is None:
        click.echo(f"No scorecard found for '{job_name}'.", err=True)
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(card, indent=2))
    else:
        click.echo(
            f"{card['job_name']}  score={card['score']}/100  "
            f"runs={card['runs']}  failures={card['failures']}  updated={card['updated_at']}"
        )


@scorecards_cmd.command("list")
@click.option("--db", default="cronwatcher.db", envvar="CRONWATCHER_DB")
@click.option("--json", "as_json", is_flag=True)
def list_cmd(db: str, as_json: bool):
    """List all scorecards ordered by score (lowest first)."""
    conn = _get_conn(db)
    cards = list_scorecards(conn)
    if as_json:
        click.echo(json.dumps(cards, indent=2))
        return
    if not cards:
        click.echo("No scorecards found.")
        return
    click.echo(f"{'Job':<30} {'Score':>6}  {'Runs':>6}  {'Failures':>8}  Updated")
    click.echo("-" * 72)
    for c in cards:
        click.echo(
            f"{c['job_name']:<30} {c['score']:>6.1f}  {c['runs']:>6}  {c['failures']:>8}  {c['updated_at']}"
        )
