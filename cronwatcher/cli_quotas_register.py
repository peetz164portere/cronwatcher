"""Register quotas CLI commands."""

from cronwatcher.cli_quotas import quotas_cmd


def register(cli):
    cli.add_command(quotas_cmd)
