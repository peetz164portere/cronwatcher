from cronwatcher.cli_permissions import permissions_cmd


def register(cli):
    cli.add_command(permissions_cmd)
