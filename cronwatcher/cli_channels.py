"""CLI commands for managing notification channels."""
import json
import click
from cronwatcher.storage import get_connection
from cronwatcher.channels import (
    init_channels, add_channel, get_channel, remove_channel,
    set_enabled, list_channels, VALID_TYPES,
)


def _get_conn(db):
    conn = get_connection(db)
    init_channels(conn)
    return conn


@click.group("channels")
def channels_cmd():
    """Manage notification channels."""


@channels_cmd.command("add")
@click.argument("name")
@click.argument("type", type=click.Choice(sorted(VALID_TYPES)))
@click.option("--config", "cfg", default="{}", help="JSON config blob for the channel")
@click.option("--db", default="cronwatcher.db", show_default=True)
def add_cmd(name, type, cfg, db):
    """Add a new notification channel."""
    try:
        config = json.loads(cfg)
    except json.JSONDecodeError as exc:
        raise click.BadParameter(f"Invalid JSON: {exc}", param_hint="--config")
    conn = _get_conn(db)
    add_channel(conn, name, type, config)
    click.echo(f"Channel '{name}' ({type}) added.")


@channels_cmd.command("remove")
@click.argument("name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def remove_cmd(name, db):
    """Remove a channel by name."""
    conn = _get_conn(db)
    if remove_channel(conn, name):
        click.echo(f"Channel '{name}' removed.")
    else:
        click.echo(f"Channel '{name}' not found.", err=True)
        raise SystemExit(1)


@channels_cmd.command("enable")
@click.argument("name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def enable_cmd(name, db):
    """Enable a channel."""
    conn = _get_conn(db)
    set_enabled(conn, name, True)
    click.echo(f"Channel '{name}' enabled.")


@channels_cmd.command("disable")
@click.argument("name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def disable_cmd(name, db):
    """Disable a channel."""
    conn = _get_conn(db)
    set_enabled(conn, name, False)
    click.echo(f"Channel '{name}' disabled.")


@channels_cmd.command("list")
@click.option("--json", "as_json", is_flag=True)
@click.option("--db", default="cronwatcher.db", show_default=True)
def list_cmd(as_json, db):
    """List all channels."""
    conn = _get_conn(db)
    channels = list_channels(conn)
    if as_json:
        click.echo(json.dumps(channels, indent=2))
        return
    if not channels:
        click.echo("No channels configured.")
        return
    click.echo(f"{'NAME':<20} {'TYPE':<12} {'ENABLED':<8}")
    click.echo("-" * 42)
    for ch in channels:
        status = "yes" if ch["enabled"] else "no"
        click.echo(f"{ch['name']:<20} {ch['type']:<12} {status:<8}")
