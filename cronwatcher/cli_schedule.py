"""CLI command to check if a job is overdue."""

import click
from cronwatcher.storage import get_connection, init_db
from cronwatcher.schedule import check_schedule
from cronwatcher.config import load_config
from cronwatcher.notify import maybe_notify


@click.command("check-schedule")
@click.argument("job_name")
@click.option("--interval", required=True, type=int, help="Expected interval in seconds")
@click.option("--db", default=None, help="Path to database file")
@click.option("--notify", "do_notify", is_flag=True, default=False, help="Send webhook if overdue")
@click.pass_context
def check_schedule_cmd(ctx, job_name: str, interval: int, db: str, do_notify: bool):
    """Check if JOB_NAME has run within the expected INTERVAL seconds."""
    db_path = db or ctx.obj.get("db", "cronwatcher.db")
    conn = get_connection(db_path)
    init_db(conn)

    result = check_schedule(conn, job_name, interval)
    conn.close()

    if result["overdue"]:
        last = result["last_success"] or "never"
        click.echo(
            click.style(
                f"OVERDUE: {job_name} last succeeded at {last} (interval {interval}s)",
                fg="red",
            )
        )
        if do_notify:
            cfg = load_config()
            maybe_notify(
                job_name=job_name,
                exit_code=1,
                duration=None,
                output=f"Job overdue. Last success: {last}",
                config=cfg,
                db_path=db_path,
            )
        ctx.exit(1)
    else:
        click.echo(
            click.style(
                f"OK: {job_name} last succeeded at {result['last_success']}",
                fg="green",
            )
        )
