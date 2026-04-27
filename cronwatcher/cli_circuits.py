"""CLI commands for circuit breaker management."""

import click
import json
from cronwatcher.storage import get_connection
from cronwatcher import circuits


def _get_conn():
    return get_connection()


@click.group(name="circuit")
def circuits_cmd():
    """Manage circuit breakers for jobs."""


@circuits_cmd.command("status")
@click.argument("job_name")
@click.option("--recovery", default=300, help="Recovery window in seconds.")
@click.option("--json", "as_json", is_flag=True)
def status_cmd(job_name, recovery, as_json):
    """Show circuit breaker state for a job."""
    conn = _get_conn()
    circuits.init_circuits(conn)
    circuit = circuits.get_circuit(conn, job_name)
    open_state = circuits.is_open(conn, job_name, recovery_seconds=recovery)
    if circuit is None:
        if as_json:
            click.echo(json.dumps({"job": job_name, "state": "closed", "failure_count": 0}))
        else:
            click.echo(f"{job_name}: closed (no data)")
        return
    if as_json:
        click.echo(json.dumps(circuit))
    else:
        state = circuit["state"]
        count = circuit["failure_count"]
        click.echo(f"{job_name}: {state} (failures={count}, open={open_state})")


@circuits_cmd.command("reset")
@click.argument("job_name")
def reset_cmd(job_name):
    """Reset the circuit breaker for a job."""
    conn = _get_conn()
    circuits.init_circuits(conn)
    circuits.reset_circuit(conn, job_name)
    click.echo(f"Circuit reset for {job_name}.")


@circuits_cmd.command("list")
@click.option("--json", "as_json", is_flag=True)
def list_cmd(as_json):
    """List all circuit breakers."""
    conn = _get_conn()
    circuits.init_circuits(conn)
    rows = circuits.list_circuits(conn)
    if as_json:
        click.echo(json.dumps(rows))
        return
    if not rows:
        click.echo("No circuit breakers recorded.")
        return
    for r in rows:
        click.echo(f"{r['job_name']}: {r['state']} (failures={r['failure_count']})")
