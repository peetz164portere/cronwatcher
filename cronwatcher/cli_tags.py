"""CLI commands for managing job tags."""

import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.tags import init_tags, add_tag, remove_tag, get_tags, get_jobs_by_tag, clear_tags


@click.group("tags")
def tags_cmd():
    """Manage tags for cron jobs."""


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_db(conn)
    init_tags(conn)
    return conn


@tags_cmd.command("add")
@click.argument("job_name")
@click.argument("tag")
@click.option("--db", default="cronwatcher.db", show_default=True)
def add_tag_cmd(job_name, tag, db):
    """Add a tag to a job."""
    conn = _get_conn(db)
    add_tag(conn, job_name, tag)
    click.echo(f"Tagged '{job_name}' with '{tag.strip().lower()}'.")


@tags_cmd.command("remove")
@click.argument("job_name")
@click.argument("tag")
@click.option("--db", default="cronwatcher.db", show_default=True)
def remove_tag_cmd(job_name, tag, db):
    """Remove a tag from a job."""
    conn = _get_conn(db)
    remove_tag(conn, job_name, tag)
    click.echo(f"Removed tag '{tag.strip().lower()}' from '{job_name}'.")


@tags_cmd.command("list")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
def list_tags_cmd(job_name, db):
    """List tags for a job."""
    conn = _get_conn(db)
    tags = get_tags(conn, job_name)
    if tags:
        click.echo(", ".join(tags))
    else:
        click.echo(f"No tags for '{job_name}'.")


@tags_cmd.command("jobs")
@click.argument("tag")
@click.option("--db", default="cronwatcher.db", show_default=True)
def jobs_by_tag_cmd(tag, db):
    """List jobs with a given tag."""
    conn = _get_conn(db)
    jobs = get_jobs_by_tag(conn, tag)
    if jobs:
        for job in jobs:
            click.echo(job)
    else:
        click.echo(f"No jobs tagged '{tag.strip().lower()}'.")
