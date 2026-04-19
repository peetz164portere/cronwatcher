"""CLI commands for managing job-to-webhook routing."""

import click
import sqlite3
from cronwatcher.storage import get_connection, init_db
from cronwatcher.routing import init_routing, set_route, get_route, remove_route, list_routes


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_db(conn)
    init_routing(conn)
    return conn


@click.group(name="routing")
def routing_cmd():
    """Manage per-job webhook routing."""


@routing_cmd.command("set")
@click.argument("job_name")
@click.argument("webhook_url")
def set_cmd(job_name, webhook_url):
    """Set a webhook URL for a specific job."""
    conn = _get_conn()
    set_route(conn, job_name, webhook_url)
    click.echo(f"Route set: {job_name} -> {webhook_url}")


@routing_cmd.command("get")
@click.argument("job_name")
def get_cmd(job_name):
    """Show the webhook URL assigned to a job."""
    conn = _get_conn()
    url = get_route(conn, job_name)
    if url:
        click.echo(url)
    else:
        click.echo(f"No route set for '{job_name}'")
        raise SystemExit(1)


@routing_cmd.command("remove")
@click.argument("job_name")
def remove_cmd(job_name):
    """Remove a job's webhook route."""
    conn = _get_conn()
    removed = remove_route(conn, job_name)
    if removed:
        click.echo(f"Route removed for '{job_name}'")
    else:
        click.echo(f"No route found for '{job_name}'")
        raise SystemExit(1)


@routing_cmd.command("list")
def list_cmd():
    """List all job webhook routes."""
    conn = _get_conn()
    routes = list_routes(conn)
    if not routes:
        click.echo("No routes configured.")
        return
    for r in routes:
        click.echo(f"{r['job_name']:<30} {r['webhook_url']}")
