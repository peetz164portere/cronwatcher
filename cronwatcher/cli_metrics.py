"""CLI commands for job metrics."""
import json
import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.metrics import init_metrics, refresh_metrics, get_metrics, get_all_metrics
from cronwatcher.trends import get_all_job_names


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_db(conn)
    init_metrics(conn)
    return conn


@click.group("metrics")
def metrics_cmd():
    """View per-job runtime metrics."""


@metrics_cmd.command("refresh")
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--job", default=None, help="Refresh a specific job only")
def refresh_cmd(db, job):
    """Recompute and store metrics for all (or one) jobs."""
    conn = _get_conn(db)
    jobs = [job] if job else get_all_job_names(conn)
    updated = 0
    for j in jobs:
        result = refresh_metrics(conn, j)
        if result:
            updated += 1
    click.echo(f"Refreshed metrics for {updated} job(s).")


@metrics_cmd.command("show")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--json", "as_json", is_flag=True)
def show_cmd(job_name, db, as_json):
    """Show stored metrics for a job."""
    conn = _get_conn(db)
    m = get_metrics(conn, job_name)
    if m is None:
        click.echo(f"No metrics found for '{job_name}'. Run 'metrics refresh' first.")
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(m, indent=2))
    else:
        click.echo(f"Job:         {m['job_name']}")
        click.echo(f"Runs:        {m['run_count']} (success={m['success_count']}, failure={m['failure_count']})")
        click.echo(f"Avg:         {m['avg_duration']}s")
        click.echo(f"p50:         {m['p50_duration']}s")
        click.echo(f"p95:         {m['p95_duration']}s")
        click.echo(f"Max:         {m['max_duration']}s")
        click.echo(f"Updated:     {m['updated_at']}")


@metrics_cmd.command("list")
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--json", "as_json", is_flag=True)
def list_cmd(db, as_json):
    """List metrics for all jobs."""
    conn = _get_conn(db)
    rows = get_all_metrics(conn)
    if as_json:
        click.echo(json.dumps(rows, indent=2))
        return
    if not rows:
        click.echo("No metrics stored yet.")
        return
    header = f"{'JOB':<25} {'RUNS':>6} {'OK':>6} {'FAIL':>6} {'AVG':>8} {'p95':>8} {'MAX':>8}"
    click.echo(header)
    click.echo("-" * len(header))
    for m in rows:
        click.echo(
            f"{m['job_name']:<25} {m['run_count']:>6} {m['success_count']:>6} "
            f"{m['failure_count']:>6} {str(m['avg_duration'] or '-'):>8} "
            f"{str(m['p95_duration'] or '-'):>8} {str(m['max_duration'] or '-'):>8}"
        )
