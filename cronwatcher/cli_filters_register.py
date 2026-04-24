"""Register filters commands with the main CLI."""

from cronwatcher.cli_filters import filters_cmd


def register(cli):
    cli.add_command(filters_cmd)
