"""Register channels commands with the root CLI."""
from cronwatcher.cli_channels import channels_cmd


def register(cli):
    cli.add_command(channels_cmd)
