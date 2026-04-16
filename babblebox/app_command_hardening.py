from __future__ import annotations

import discord
from discord import app_commands


ADMIN_ROOT_CONTEXTS = app_commands.AppCommandContext(guild=True, dm_channel=False, private_channel=False)
ADMIN_ROOT_INSTALLS = app_commands.AppInstallationType(guild=True, user=False)
ADMIN_ROOT_PERMISSIONS = discord.Permissions(manage_guild=True)


def _harden_root_group(command: object, *, default_permissions: discord.Permissions | None) -> None:
    app_command = getattr(command, "app_command", command)
    if not isinstance(app_command, app_commands.Group):
        return

    app_command.default_permissions = default_permissions
    app_command.guild_only = True
    app_command.allowed_contexts = ADMIN_ROOT_CONTEXTS
    app_command.allowed_installs = ADMIN_ROOT_INSTALLS


def harden_admin_root_group(command: object) -> None:
    _harden_root_group(command, default_permissions=ADMIN_ROOT_PERMISSIONS)


def harden_lock_root_group(command: object) -> None:
    # Discord treats default_member_permissions as a required bitset, so `/lock`
    # cannot publish Babblebox's intended moderator-or-admin runtime rule there.
    _harden_root_group(command, default_permissions=None)


def harden_timeout_root_group(command: object) -> None:
    # `/timeout` also relies on a runtime moderation rule instead of one fixed
    # default-member-permissions bitset.
    _harden_root_group(command, default_permissions=None)
