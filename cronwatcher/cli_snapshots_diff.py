"""CLI command: cronwatcher snapshot-diff <snap1.json> <snap2.json>"""

from __future__ import annotations

import json
import sys

import click

from cronwatcher.snapshots_diff import diff_snapshots, format_diff_text, has_changes, summary_counts


@click.group("snapshot-diff")
def snapshot_diff_cmd() -> None:
    """Compare two snapshot files."""


@snapshot_diff_cmd.command("compare")
@click.argument("old_file", type=click.Path(exists=True))
@click.argument("new_file", type=click.Path(exists=True))
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.option("--show-unchanged", is_flag=True, default=False, help="Include unchanged jobs.")
@click.option("--summary", is_flag=True, default=False, help="Print counts only.")
def compare_cmd(
    old_file: str,
    new_file: str,
    as_json: bool,
    show_unchanged: bool,
    summary: bool,
) -> None:
    """Compare two snapshot JSON files and show what changed."""
    with open(old_file) as f:
        old = json.load(f)
    with open(new_file) as f:
        new = json.load(f)

    diff = diff_snapshots(old, new)
    changed = has_changes(diff)

    if as_json:
        if summary:
            click.echo(json.dumps(summary_counts(diff), indent=2))
        else:
            click.echo(json.dumps(diff, indent=2))
    elif summary:
        counts = summary_counts(diff)
        click.echo(
            f"added={counts['added']}  removed={counts['removed']}  "
            f"changed={counts['changed']}  unchanged={counts['unchanged']}"
        )
    else:
        click.echo(format_diff_text(diff, show_unchanged=show_unchanged))

    sys.exit(1 if changed else 0)
