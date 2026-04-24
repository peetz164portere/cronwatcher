"""CLI commands for managing workflows."""

import click
import json
from cronwatcher.storage import get_connection
from cronwatcher import workflows as wf_mod


def _get_conn():
    return get_connection()


@click.group(name="workflow")
def workflows_cmd():
    """Manage job workflows."""


@workflows_cmd.command("create")
@click.argument("name")
@click.option("--description", "-d", default="", help="Workflow description")
@click.option("--steps", "-s", default="[]", help="JSON array of job names in order")
def create_cmd(name, description, steps):
    """Create a new workflow."""
    try:
        parsed = json.loads(steps)
    except json.JSONDecodeError:
        click.echo("Error: --steps must be a valid JSON array", err=True)
        raise SystemExit(1)
    conn = _get_conn()
    wf_mod.init_workflows(conn)
    row_id = wf_mod.create_workflow(conn, name, description, parsed)
    if row_id:
        click.echo(f"Created workflow '{name}' (id={row_id})")
    else:
        click.echo(f"Workflow '{name}' already exists")


@workflows_cmd.command("show")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True)
def show_cmd(name, as_json):
    """Show a workflow definition."""
    conn = _get_conn()
    wf_mod.init_workflows(conn)
    wf = wf_mod.get_workflow(conn, name)
    if wf is None:
        click.echo(f"Workflow '{name}' not found", err=True)
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(wf, indent=2))
    else:
        click.echo(f"Name: {wf['name']}")
        click.echo(f"Description: {wf['description'] or '(none)'}")
        click.echo(f"Steps: {', '.join(wf['steps']) or '(none)'}")
        click.echo(f"Created: {wf['created_at']}")


@workflows_cmd.command("remove")
@click.argument("name")
def remove_cmd(name):
    """Remove a workflow."""
    conn = _get_conn()
    wf_mod.init_workflows(conn)
    if wf_mod.remove_workflow(conn, name):
        click.echo(f"Removed workflow '{name}'")
    else:
        click.echo(f"Workflow '{name}' not found", err=True)
        raise SystemExit(1)


@workflows_cmd.command("list")
@click.option("--json", "as_json", is_flag=True)
def list_cmd(as_json):
    """List all workflows."""
    conn = _get_conn()
    wf_mod.init_workflows(conn)
    items = wf_mod.list_workflows(conn)
    if as_json:
        click.echo(json.dumps(items, indent=2))
    else:
        if not items:
            click.echo("No workflows defined.")
        for w in items:
            click.echo(f"{w['name']}: {len(w['steps'])} steps  {w['description']}")
