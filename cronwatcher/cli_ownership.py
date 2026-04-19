"""CLI commands for managing job ownership."""

import click
from cronwatcher.ownership import (
    init_ownership,
    set_owner,
    get_owner,
    remove_owner,
    list_owners,
)
from cronwatcher.storage import get_connection


def _get_conn():
    from cronwatcher.config import load_config
    cfg = load_config()
    db_path = cfg.get("db_path", "cronwatcher.db")
    conn = get_connection(db_path)
    init_ownership(conn)
    return conn


@click.group("ownership")
def ownership_cmd():
    """Manage job ownership assignments."""
    pass


@ownership_cmd.command("set")
@click.argument("job_name")
@click.argument("owner")
@click.option("--email", default=None, help="Owner email address.")
@click.option("--team", default=None, help="Team name.")
def set_cmd(job_name, owner, email, team):
    """Assign an owner to a job."""
    conn = _get_conn()
    set_owner(conn, job_name, owner, email=email, team=team)
    click.echo(f"Owner '{owner}' assigned to job '{job_name}'.")
    if email:
        click.echo(f"  Email: {email}")
    if team:
        click.echo(f"  Team: {team}")


@ownership_cmd.command("get")
@click.argument("job_name")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
def get_cmd(job_name, as_json):
    """Show the owner of a job."""
    import json
    conn = _get_conn()
    record = get_owner(conn, job_name)
    if not record:
        click.echo(f"No owner set for '{job_name}'.")
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(record, indent=2))
    else:
        click.echo(f"Job:   {job_name}")
        click.echo(f"Owner: {record['owner']}")
        if record.get("email"):
            click.echo(f"Email: {record['email']}")
        if record.get("team"):
            click.echo(f"Team:  {record['team']}")


@ownership_cmd.command("remove")
@click.argument("job_name")
def remove_cmd(job_name):
    """Remove ownership record for a job."""
    conn = _get_conn()
    remove_owner(conn, job_name)
    click.echo(f"Ownership record removed for '{job_name}'.")


@ownership_cmd.command("list")
@click.option("--team", default=None, help="Filter by team.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
def list_cmd(team, as_json):
    """List all job ownership records."""
    import json
    conn = _get_conn()
    records = list_owners(conn)
    if team:
        records = [r for r in records if (r.get("team") or "").lower() == team.lower()]
    if as_json:
        click.echo(json.dumps(records, indent=2))
        return
    if not records:
        click.echo("No ownership records found.")
        return
    header = f"{'Job':<30} {'Owner':<20} {'Email':<25} {'Team':<15}"
    click.echo(header)
    click.echo("-" * len(header))
    for r in records:
        click.echo(
            f"{r['job_name']:<30} {r['owner']:<20} {(r.get('email') or ''):<25} {(r.get('team') or ''):<15}"
        )
