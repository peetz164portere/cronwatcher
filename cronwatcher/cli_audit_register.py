"""Register audit commands with the main CLI."""
from cronwatcher.cli import cli
from cronwatcher.cli_audit import audit_cmd


def register():
    cli.add_command(audit_cmd)
