"""CLI commands for heartbeat management."""

import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.heartbeat import init_heartbeat_log, get_heartbeat_history, send_heartbeat, record_heartbeat


@click.group(name="heartbeat")
def heartbeat_cmd():
    """Manage heartbeat pings for cron jobs."""


def _get_conn(db):
    conn = get_connection(db)
    init_db(conn)
    init_heartbeat_log(conn)
    return conn


@heartbeat_cmd.command("ping")
@click.argument("job_name")
@click.argument("url")
@click.option("--db", default="cronwatcher.db", show_default=True)
def ping_cmd(job_name, url, db):
    """Manually send a heartbeat ping for a job."""
    conn = _get_conn(db)
    ok = send_heartbeat(url)
    record_heartbeat(conn, job_name, url, ok)
    if ok:
        click.echo(f"Heartbeat sent for '{job_name}': OK")
    else:
        click.echo(f"Heartbeat failed for '{job_name}': could not reach {url}", err=True)
        raise SystemExit(1)


@heartbeat_cmd.command("history")
@click.argument("job_name")
@click.option("--limit", default=20, show_default=True)
@click.option("--db", default="cronwatcher.db", show_default=True)
def history_cmd(job_name, limit, db):
    """Show heartbeat ping history for a job."""
    conn = _get_conn(db)
    rows = get_heartbeat_history(conn, job_name, limit)
    if not rows:
        click.echo(f"No heartbeat history for '{job_name}'.")
        return
    click.echo(f"{'SENT AT':<30} {'URL':<40} STATUS")
    click.echo("-" * 80)
    for r in rows:
        status = click.style("OK", fg="green") if r["success"] else click.style("FAIL", fg="red")
        click.echo(f"{r['sent_at']:<30} {r['url']:<40} {status}")
