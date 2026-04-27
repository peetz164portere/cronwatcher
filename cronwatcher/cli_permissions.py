import click
import sqlite3
from cronwatcher.permissions import (
    init_permissions, grant_permission, revoke_permission,
    has_permission, get_permissions, list_all_permissions
)
from cronwatcher.storage import get_connection, init_db


def _get_conn() -> sqlite3.Connection:
    conn = get_connection()
    init_db(conn)
    init_permissions(conn)
    return conn


@click.group("permissions")
def permissions_cmd():
    """Manage per-job access permissions."""


@permissions_cmd.command("grant")
@click.argument("job")
@click.argument("principal")
@click.argument("action")
def grant_cmd(job, principal, action):
    """Grant PRINCIPAL the ACTION permission on JOB."""
    conn = _get_conn()
    try:
        pid = grant_permission(conn, job, principal, action)
        click.echo(f"Granted '{action}' to '{principal}' on '{job}' (id={pid})")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)


@permissions_cmd.command("revoke")
@click.argument("job")
@click.argument("principal")
@click.argument("action")
def revoke_cmd(job, principal, action):
    """Revoke PRINCIPAL's ACTION permission on JOB."""
    conn = _get_conn()
    removed = revoke_permission(conn, job, principal, action)
    if removed:
        click.echo(f"Revoked '{action}' from '{principal}' on '{job}'")
    else:
        click.echo("No matching permission found.", err=True)
        raise SystemExit(1)


@permissions_cmd.command("check")
@click.argument("job")
@click.argument("principal")
@click.argument("action")
def check_cmd(job, principal, action):
    """Check whether PRINCIPAL has ACTION permission on JOB."""
    conn = _get_conn()
    allowed = has_permission(conn, job, principal, action)
    if allowed:
        click.echo(f"ALLOWED: '{principal}' can '{action}' on '{job}'")
    else:
        click.echo(f"DENIED: '{principal}' cannot '{action}' on '{job}'")
        raise SystemExit(1)


@permissions_cmd.command("list")
@click.argument("job", required=False)
@click.option("--all", "all_jobs", is_flag=True, help="List permissions for all jobs.")
def list_cmd(job, all_jobs):
    """List permissions for a JOB (or all jobs with --all)."""
    conn = _get_conn()
    if all_jobs:
        rows = list_all_permissions(conn)
    elif job:
        rows = get_permissions(conn, job)
    else:
        click.echo("Provide a job name or use --all.", err=True)
        raise SystemExit(1)

    if not rows:
        click.echo("No permissions found.")
        return
    click.echo(f"{'ID':<5} {'Job':<20} {'Principal':<20} {'Action':<10} Granted At")
    click.echo("-" * 70)
    for r in rows:
        click.echo(f"{r['id']:<5} {r['job_name']:<20} {r['principal']:<20} {r['action']:<10} {r['granted_at']}")
