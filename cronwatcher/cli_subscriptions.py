"""CLI commands for managing per-job webhook subscriptions."""

import json
import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.subscriptions import (
    init_subscriptions,
    add_subscription,
    get_subscriptions,
    remove_subscription,
    list_all_subscriptions,
    VALID_EVENTS,
)


def _get_conn():
    conn = get_connection()
    init_db(conn)
    init_subscriptions(conn)
    return conn


@click.group("subscriptions")
def subscriptions_cmd():
    """Manage per-job webhook subscriptions."""


@subscriptions_cmd.command("add")
@click.argument("job_name")
@click.argument("event")
@click.argument("url")
@click.option("--header", multiple=True, help="Extra headers as KEY=VALUE pairs.")
def add_cmd(job_name, event, url, header):
    """Subscribe URL to an event for a job."""
    headers = {}
    for h in header:
        if "=" not in h:
            raise click.BadParameter(f"Header must be KEY=VALUE, got: {h}")
        k, v = h.split("=", 1)
        headers[k.strip()] = v.strip()
    conn = _get_conn()
    try:
        sub_id = add_subscription(conn, job_name, event, url, headers)
        click.echo(f"Subscription added (id={sub_id}).")
    except ValueError as e:
        raise click.ClickException(str(e))


@subscriptions_cmd.command("list")
@click.argument("job_name", required=False)
@click.argument("event", required=False)
@click.option("--json", "as_json", is_flag=True)
def list_cmd(job_name, event, as_json):
    """List subscriptions, optionally filtered by job and event."""
    conn = _get_conn()
    if job_name and event:
        rows = get_subscriptions(conn, job_name, event)
    else:
        rows = list_all_subscriptions(conn)
    if as_json:
        click.echo(json.dumps(rows, indent=2))
    else:
        if not rows:
            click.echo("No subscriptions found.")
        for r in rows:
            click.echo(f"[{r['id']}] {r['job_name']} / {r['event']} -> {r['url']}")


@subscriptions_cmd.command("remove")
@click.argument("subscription_id", type=int)
def remove_cmd(subscription_id):
    """Remove a subscription by ID."""
    conn = _get_conn()
    removed = remove_subscription(conn, subscription_id)
    if removed:
        click.echo(f"Subscription {subscription_id} removed.")
    else:
        raise click.ClickException(f"No subscription with id={subscription_id}.")
