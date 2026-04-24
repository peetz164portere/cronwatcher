"""Register workflow commands with the main CLI."""

from cronwatcher.cli_workflows import workflows_cmd


def register(cli):
    cli.add_command(workflows_cmd)
