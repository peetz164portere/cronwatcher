import click
import sqlite3
from cronwatcher.storage import get_connection, init_db
from cronwatcher.escalations import init_escalations, set_escalation, get_escalations, remove_escalation


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_db(conn)
    init_escalations(conn)
    return conn


@click.group(name="escalations")
def escalations_cmd():
    """Manage job alert escalation policies."""
    pass


@escalations_cmd.command(name="add")
@click.argument("job_name")
@click.argument("webhook_url")
@click.option("--level", default=1, show_default=True, help="Escalation level (1=first, 2=second, ...)")
@click.option("--threshold", default=30, show_default=True, help="Minutes before escalating")
def add_cmd(job_name, webhook_url, level, threshold):
    """Add an escalation level for a job."""
    conn = _get_conn()
    eid = set_escalation(conn, job_name, level, webhook_url, threshold)
    click.echo(f"Escalation added (id={eid}) for '{job_name}' at level {level}.")


@escalations_cmd.command(name="list")
@click.argument("job_name")
def list_cmd(job_name):
    """List escalation levels for a job."""
    conn = _get_conn()
    entries = get_escalations(conn, job_name)
    if not entries:
        click.echo(f"No escalations configured for '{job_name}'.")
        return
    for e in entries:
        click.echo(f"[{e['id']}] level={e['level']} threshold={e['threshold_minutes']}m url={e['webhook_url']}")


@escalations_cmd.command(name="remove")
@click.argument("escalation_id", type=int)
def remove_cmd(escalation_id):
    """Remove an escalation by ID."""
    conn = _get_conn()
    removed = remove_escalation(conn, escalation_id)
    if removed:
        click.echo(f"Escalation {escalation_id} removed.")
    else:
        click.echo(f"No escalation found with id {escalation_id}.", err=True)
        raise SystemExit(1)
