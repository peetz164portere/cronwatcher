"""CLI commands for managing per-job notes."""
import click
import sqlite3
from cronwatcher.storage import get_connection, init_db
from cronwatcher.notes import init_notes, set_note, get_note, remove_note, list_notes


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_db(conn)
    init_notes(conn)
    return conn


@click.group("notes")
def notes_cmd():
    """Manage notes attached to cron jobs."""


@notes_cmd.command("set")
@click.argument("job_name")
@click.argument("note")
def set_cmd(job_name, note):
    """Set or update a note for a job."""
    conn = _get_conn()
    set_note(conn, job_name, note)
    click.echo(f"Note set for '{job_name}'.")


@notes_cmd.command("get")
@click.argument("job_name")
def get_cmd(job_name):
    """Show the note for a job."""
    conn = _get_conn()
    result = get_note(conn, job_name)
    if result is None:
        click.echo(f"No note found for '{job_name}'.")
        raise SystemExit(1)
    click.echo(f"{result['job_name']}: {result['note']}  (updated {result['updated_at']})")


@notes_cmd.command("remove")
@click.argument("job_name")
def remove_cmd(job_name):
    """Remove the note for a job."""
    conn = _get_conn()
    removed = remove_note(conn, job_name)
    if removed:
        click.echo(f"Note removed for '{job_name}'.")
    else:
        click.echo(f"No note found for '{job_name}'.")
        raise SystemExit(1)


@notes_cmd.command("list")
def list_cmd():
    """List all job notes."""
    conn = _get_conn()
    rows = list_notes(conn)
    if not rows:
        click.echo("No notes stored.")
        return
    for r in rows:
        click.echo(f"{r['job_name']}: {r['note']}")
