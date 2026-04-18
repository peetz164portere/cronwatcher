"""CLI command to compare two saved snapshots."""

import json
import click
from cronwatcher.snapshots import load_snapshot
from cronwatcher.compare import compare_snapshots, format_compare_text


@click.command("compare")
@click.argument("old_path", type=click.Path(exists=True))
@click.argument("new_path", type=click.Path(exists=True))
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def compare_cmd(old_path: str, new_path: str, as_json: bool):
    """Compare two snapshot files and show what changed."""
    with open(old_path) as f:
        old = json.load(f)
    with open(new_path) as f:
        new = json.load(f)

    diff = compare_snapshots(old, new)

    if as_json:
        click.echo(json.dumps(diff, indent=2))
    else:
        click.echo(format_compare_text(diff))

    summary = diff["summary"]
    if summary["added"] or summary["removed"] or summary["changed"]:
        raise SystemExit(1)
