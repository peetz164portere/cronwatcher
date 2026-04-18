import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.incidents import init_incidents, open_incident, close_incident, list_incidents


def _get_conn():
    conn = get_connection()
    init_db(conn)
    init_incidents(conn)
    return conn


@click.group("incidents")
def incidents_cmd():
    """Manage job incidents."""


@incidents_cmd.command("open")
@click.argument("job_name")
@click.option("--run-id", type=int, default=None)
@click.option("--note", default=None)
def open_cmd(job_name, run_id, note):
    """Open an incident for a job."""
    conn = _get_conn()
    iid = open_incident(conn, job_name, run_id=run_id, note=note)
    click.echo(f"Incident #{iid} opened for '{job_name}'")


@incidents_cmd.command("close")
@click.argument("job_name")
@click.option("--note", default=None)
def close_cmd(job_name, note):
    """Close the open incident for a job."""
    conn = _get_conn()
    closed = close_incident(conn, job_name, note=note)
    if closed:
        click.echo(f"Incident closed for '{job_name}'")
    else:
        click.echo(f"No open incident found for '{job_name}'")
        raise SystemExit(1)


@incidents_cmd.command("list")
@click.option("--job", default=None)
@click.option("--status", type=click.Choice(["open", "closed"]), default=None)
def list_cmd(job, status):
    """List incidents."""
    conn = _get_conn()
    rows = list_incidents(conn, job_name=job, status=status)
    if not rows:
        click.echo("No incidents found.")
        return
    for r in rows:
        closed = r["closed_at"] or "-"
        click.echo(f"[{r['status'].upper()}] #{r['id']} {r['job_name']} opened={r['opened_at']} closed={closed}")
