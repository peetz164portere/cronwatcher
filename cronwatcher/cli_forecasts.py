"""CLI commands for job run forecasting."""

import json
import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.forecasts import init_forecasts, compute_forecast, save_forecast, get_forecast, list_forecasts


def _get_conn(db_path):
    conn = get_connection(db_path)
    init_db(conn)
    init_forecasts(conn)
    return conn


@click.group(name="forecast")
def forecasts_cmd():
    """Forecast next run time and expected duration."""


@forecasts_cmd.command(name="refresh")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--interval", default=None, type=int, help="Expected interval between runs in seconds")
@click.option("--samples", default=10, show_default=True, help="Number of recent runs to sample")
def refresh_cmd(job_name, db, interval, samples):
    """Compute and save a forecast for a job."""
    conn = _get_conn(db)
    forecast = compute_forecast(conn, job_name, interval_seconds=interval, sample_size=samples)
    if not forecast:
        click.echo(f"No data available for job '{job_name}'.", err=True)
        raise SystemExit(1)
    save_forecast(conn, forecast)
    click.echo(
        f"Forecast saved: duration={forecast['predicted_duration_s']}s, "
        f"confidence={forecast['confidence']}, samples={forecast['sample_size']}"
    )


@forecasts_cmd.command(name="show")
@click.argument("job_name")
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--json", "as_json", is_flag=True)
def show_cmd(job_name, db, as_json):
    """Show the saved forecast for a job."""
    conn = _get_conn(db)
    forecast = get_forecast(conn, job_name.lower())
    if not forecast:
        click.echo(f"No forecast found for '{job_name}'.", err=True)
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(forecast, indent=2))
    else:
        click.echo(f"Job:               {forecast['job_name']}")
        click.echo(f"Predicted duration: {forecast['predicted_duration_s']}s")
        click.echo(f"Next run:          {forecast['predicted_next_run'] or 'N/A'}")
        click.echo(f"Confidence:        {forecast['confidence']}")
        click.echo(f"Updated:           {forecast['updated_at']}")


@forecasts_cmd.command(name="list")
@click.option("--db", default="cronwatcher.db", show_default=True)
@click.option("--json", "as_json", is_flag=True)
def list_cmd(db, as_json):
    """List all saved forecasts."""
    conn = _get_conn(db)
    rows = list_forecasts(conn)
    if as_json:
        click.echo(json.dumps(rows, indent=2))
        return
    if not rows:
        click.echo("No forecasts saved.")
        return
    click.echo(f"{'JOB':<30} {'DURATION(s)':<14} {'CONFIDENCE':<12} {'NEXT RUN'}")
    for r in rows:
        click.echo(
            f"{r['job_name']:<30} {r['predicted_duration_s']:<14} {r['confidence']:<12} {r['predicted_next_run'] or 'N/A'}"
        )
