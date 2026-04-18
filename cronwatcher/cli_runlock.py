"""CLI commands for managing run locks."""
import click
from cronwatcher.storage import get_connection
from cronwatcher.runlock import init_runlock, get_lock, release_lock, clear_stale_locks


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_runlock(conn)
    return conn


@click.group(name="runlock")
def runlock_cmd():
    """Manage job run locks."""


@runlock_cmd.command(name="status")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def status_cmd(job_name, db):
    """Show lock status for a job."""
    conn = _get_conn(db)
    lock = get_lock(conn, job_name)
    if lock is None:
        click.echo(f"{job_name}: not locked")
    else:
        import time
        age = time.time() - lock["locked_at"]
        click.echo(f"{job_name}: locked by pid {lock['pid']} ({age:.0f}s ago)")


@runlock_cmd.command(name="release")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def release_cmd(job_name, db):
    """Force-release a lock."""
    conn = _get_conn(db)
    removed = release_lock(conn, job_name)
    if removed:
        click.echo(f"Lock released for {job_name}.")
    else:
        click.echo(f"No lock found for {job_name}.")


@runlock_cmd.command(name="clear-stale")
@click.option("--max-age", default=3600, show_default=True, help="Max age in seconds.")
@click.option("--db", default="cronwatcher.db", show_default=True)
def clear_stale_cmd(max_age, db):
    """Remove stale locks older than --max-age seconds."""
    conn = _get_conn(db)
    count = clear_stale_locks(conn, max_age_seconds=max_age)
    click.echo(f"Removed {count} stale lock(s).")
