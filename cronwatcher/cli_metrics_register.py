"""Register metrics_cmd into the main CLI."""
# This module is imported by cronwatcher/cli.py to attach the metrics group.
# Usage in cli.py:
#   from cronwatcher.cli_metrics_register import register
#   register(cli)

from cronwatcher.cli_metrics import metrics_cmd


def register(cli_group):
    """Attach the metrics command group to the root CLI."""
    cli_group.add_command(metrics_cmd)
