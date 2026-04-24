"""CLI commands for managing saved search filters."""

import json
import click
from cronwatcher.storage import get_connection
from cronwatcher import filters as flt


def _get_conn():
    return get_connection()


@click.group(name="filters")
def filters_cmd():
    """Manage saved search filters."""


@filters_cmd.command(name="save")
@click.argument("name")
@click.option("--job", default=None, help="Filter by job name")
@click.option("--status", default=None, type=click.Choice(["success", "failure"]), help="Filter by status")
@click.option("--limit", default=None, type=int, help="Max results")
def save_cmd(name, job, status, limit):
    """Save a named filter preset."""
    params = {}
    if job:
        params["job"] = job
    if status:
        params["status"] = status
    if limit is not None:
        params["limit"] = limit
    if not params:
        click.echo("Error: provide at least one filter option.", err=True)
        raise SystemExit(1)
    conn = _get_conn()
    flt.init_filters(conn)
    flt.save_filter(conn, name, params)
    click.echo(f"Filter '{name}' saved.")


@filters_cmd.command(name="show")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
def show_cmd(name, as_json):
    """Show a saved filter's parameters."""
    conn = _get_conn()
    flt.init_filters(conn)
    params = flt.get_filter(conn, name)
    if params is None:
        click.echo(f"No filter named '{name}'.", err=True)
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(params, indent=2))
    else:
        for k, v in params.items():
            click.echo(f"  {k}: {v}")


@filters_cmd.command(name="remove")
@click.argument("name")
def remove_cmd(name):
    """Delete a saved filter."""
    conn = _get_conn()
    flt.init_filters(conn)
    removed = flt.remove_filter(conn, name)
    if removed:
        click.echo(f"Filter '{name}' removed.")
    else:
        click.echo(f"No filter named '{name}'.", err=True)
        raise SystemExit(1)


@filters_cmd.command(name="list")
@click.option("--json", "as_json", is_flag=True)
def list_cmd(as_json):
    """List all saved filters."""
    conn = _get_conn()
    flt.init_filters(conn)
    rows = flt.list_filters(conn)
    if as_json:
        click.echo(json.dumps(rows, indent=2))
        return
    if not rows:
        click.echo("No saved filters.")
        return
    for r in rows:
        params_str = ", ".join(f"{k}={v}" for k, v in r["params"].items())
        click.echo(f"  {r['name']:<20} {params_str}")
