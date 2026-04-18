"""CLI commands for managing job labels."""
import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.labels import init_labels, set_label, get_labels, remove_label, get_jobs_by_label


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_db(conn)
    init_labels(conn)
    return conn


@click.group("labels")
def labels_cmd():
    """Manage key-value labels for jobs."""


@labels_cmd.command("set")
@click.argument("job_name")
@click.argument("key")
@click.argument("value")
@click.option("--db", default="cronwatcher.db", show_default=True)
def set_cmd(job_name, key, value, db):
    """Set a label on a job."""
    conn = _get_conn(db)
    set_label(conn, job_name, key, value)
    click.echo(f"Set {key}={value} on {job_name}")


@labels_cmd.command("get")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def get_cmd(job_name, db):
    """List all labels for a job."""
    conn = _get_conn(db)
    labels = get_labels(conn, job_name)
    if not labels:
        click.echo(f"No labels for {job_name}")
        return
    for k, v in labels.items():
        click.echo(f"  {k}={v}")


@labels_cmd.command("remove")
@click.argument("job_name")
@click.argument("key")
@click.option("--db", default="cronwatcher.db", show_default=True)
def remove_cmd(job_name, key, db):
    """Remove a label from a job."""
    conn = _get_conn(db)
    removed = remove_label(conn, job_name, key)
    if removed:
        click.echo(f"Removed label '{key}' from {job_name}")
    else:
        click.echo(f"Label '{key}' not found on {job_name}")


@labels_cmd.command("find")
@click.argument("key")
@click.argument("value")
@click.option("--db", default="cronwatcher.db", show_default=True)
def find_cmd(key, value, db):
    """Find jobs matching a label key=value."""
    conn = _get_conn(db)
    jobs = get_jobs_by_label(conn, key, value)
    if not jobs:
        click.echo("No jobs found.")
        return
    for j in jobs:
        click.echo(j)
