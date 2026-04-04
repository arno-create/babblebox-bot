from __future__ import annotations

import discord
from discord import app_commands


ADMIN_ROOT_CONTEXTS = app_commands.AppCommandContext(guild=True, dm_channel=False, private_channel=False)
ADMIN_ROOT_INSTALLS = app_commands.AppInstallationType(guild=True, user=False)
ADMIN_ROOT_PERMISSIONS = discord.Permissions(manage_guild=True)


def harden_admin_root_group(command: object) -> None:
    app_command = getattr(command, "app_command", command)
    if not isinstance(app_command, app_commands.Group):
        return

    app_command.default_permissions = ADMIN_ROOT_PERMISSIONS
    app_command.guild_only = True
    app_command.allowed_contexts = ADMIN_ROOT_CONTEXTS
    app_command.allowed_installs = ADMIN_ROOT_INSTALLS
