"""CLI commands for managing job dependencies."""
import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.dependencies import (
    init_dependencies, add_dependency, remove_dependency,
    get_dependencies, get_dependents, check_ready,
)


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_db(conn)
    init_dependencies(conn)
    return conn


@click.group("deps")
def deps_cmd():
    """Manage job run-order dependencies."""


@deps_cmd.command("add")
@click.argument("job")
@click.argument("depends_on")
@click.option("--db", default="cronwatcher.db", show_default=True)
def add_cmd(job, depends_on, db):
    """Add a dependency: JOB must run after DEPENDS_ON."""
    conn = _get_conn(db)
    try:
        add_dependency(conn, job, depends_on)
        click.echo(f"Added: {job} depends on {depends_on}")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)


@deps_cmd.command("remove")
@click.argument("job")
@click.argument("depends_on")
@click.option("--db", default="cronwatcher.db", show_default=True)
def remove_cmd(job, depends_on, db):
    """Remove a dependency between JOB and DEPENDS_ON."""
    conn = _get_conn(db)
    removed = remove_dependency(conn, job, depends_on)
    if removed:
        click.echo(f"Removed: {job} no longer depends on {depends_on}")
    else:
        click.echo("Dependency not found.", err=True)
        raise SystemExit(1)


@deps_cmd.command("list")
@click.argument("job")
@click.option("--db", default="cronwatcher.db", show_default=True)
def list_cmd(job, db):
    """List dependencies for JOB."""
    conn = _get_conn(db)
    deps = get_dependencies(conn, job)
    dependents = get_dependents(conn, job)
    click.echo(f"Dependencies of '{job}': {', '.join(deps) or 'none'}")
    click.echo(f"Dependents on '{job}': {', '.join(dependents) or 'none'}")


@deps_cmd.command("check")
@click.argument("job")
@click.option("--db", default="cronwatcher.db", show_default=True)
def check_cmd(job, db):
    """Check if JOB is ready to run (all deps succeeded)."""
    conn = _get_conn(db)
    result = check_ready(conn, job)
    if result["ready"]:
        click.echo(f"'{job}' is ready to run.")
    else:
        click.echo(f"'{job}' is blocked by: {', '.join(result['blocking'])}")
        raise SystemExit(1)
