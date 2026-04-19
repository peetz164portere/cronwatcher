import click
import sqlite3
from cronwatcher.storage import get_connection, init_db
from cronwatcher.checkpoints import init_checkpoints, set_checkpoint, get_checkpoint, list_checkpoints, remove_checkpoint


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_db(conn)
    init_checkpoints(conn)
    return conn


@click.group(name="checkpoints")
def checkpoints_cmd():
    """Manage job checkpoints."""


@checkpoints_cmd.command(name="set")
@click.argument("job_name")
@click.argument("label")
@click.option("--note", default=None, help="Optional note for this checkpoint.")
def set_cmd(job_name, label, note):
    """Set or update a checkpoint for a job."""
    conn = _get_conn()
    row_id = set_checkpoint(conn, job_name, label, note)
    click.echo(f"Checkpoint '{label}' set for '{job_name}' (id={row_id}).")


@checkpoints_cmd.command(name="get")
@click.argument("job_name")
@click.argument("label")
def get_cmd(job_name, label):
    """Get a specific checkpoint."""
    conn = _get_conn()
    cp = get_checkpoint(conn, job_name, label)
    if cp is None:
        click.echo(f"No checkpoint '{label}' found for '{job_name}'.")
        raise SystemExit(1)
    click.echo(f"[{cp['recorded_at']}] {cp['job_name']} / {cp['label']}" + (f" — {cp['note']}" if cp['note'] else ""))


@checkpoints_cmd.command(name="list")
@click.argument("job_name")
def list_cmd(job_name):
    """List all checkpoints for a job."""
    conn = _get_conn()
    cps = list_checkpoints(conn, job_name)
    if not cps:
        click.echo(f"No checkpoints for '{job_name}'.")
        return
    for cp in cps:
        note_str = f" — {cp['note']}" if cp['note'] else ""
        click.echo(f"  [{cp['recorded_at']}] {cp['label']}{note_str}")


@checkpoints_cmd.command(name="remove")
@click.argument("job_name")
@click.argument("label")
def remove_cmd(job_name, label):
    """Remove a checkpoint."""
    conn = _get_conn()
    removed = remove_checkpoint(conn, job_name, label)
    if removed:
        click.echo(f"Checkpoint '{label}' removed from '{job_name}'.")
    else:
        click.echo(f"Checkpoint '{label}' not found for '{job_name}'.")
        raise SystemExit(1)
