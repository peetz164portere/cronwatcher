import click
import json
import sqlite3
from cronwatcher.bookmarks import init_bookmarks, add_bookmark, remove_bookmark, get_bookmarks, list_all_bookmarks
from cronwatcher.storage import get_connection


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_bookmarks(conn)
    return conn


@click.group("bookmarks")
def bookmarks_cmd():
    """Manage bookmarked runs."""


@bookmarks_cmd.command("add")
@click.argument("job_name")
@click.argument("run_id", type=int)
@click.option("--label", default=None, help="Optional label for the bookmark.")
def add_cmd(job_name, run_id, label):
    """Bookmark a run."""
    conn = _get_conn()
    row_id = add_bookmark(conn, job_name, run_id, label)
    if row_id:
        click.echo(f"Bookmarked run {run_id} for job '{job_name}'.")
    else:
        click.echo("Bookmark already exists.")


@bookmarks_cmd.command("remove")
@click.argument("job_name")
@click.argument("run_id", type=int)
def remove_cmd(job_name, run_id):
    """Remove a bookmark."""
    conn = _get_conn()
    removed = remove_bookmark(conn, job_name, run_id)
    if removed:
        click.echo(f"Removed bookmark for run {run_id} of job '{job_name}'.")
    else:
        click.echo("Bookmark not found.", err=True)
        raise SystemExit(1)


@bookmarks_cmd.command("list")
@click.argument("job_name", required=False)
@click.option("--json", "as_json", is_flag=True)
def list_cmd(job_name, as_json):
    """List bookmarks."""
    conn = _get_conn()
    entries = get_bookmarks(conn, job_name) if job_name else list_all_bookmarks(conn)
    if as_json:
        click.echo(json.dumps(entries, indent=2))
    else:
        if not entries:
            click.echo("No bookmarks found.")
            return
        for e in entries:
            label = f" [{e['label']}]" if e["label"] else ""
            click.echo(f"{e['job_name']} run={e['run_id']}{label} @ {e['created_at']}")
