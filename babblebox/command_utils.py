from __future__ import annotations

from typing import Any

import discord
from discord.ext import commands

from babblebox import game_engine as ge


async def send_hybrid_response(
    ctx: commands.Context,
    content: str | None = None,
    *,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
    ephemeral: bool = False,
) -> discord.Message | None:
    kwargs: dict[str, Any] = {}
    if content is not None:
        kwargs["content"] = content
    if embed is not None:
        kwargs["embed"] = embed
    if view is not None:
        kwargs["view"] = view

    interaction = getattr(ctx, "interaction", None)
    if interaction is None:
        return await ctx.send(**kwargs)

    if interaction.response.is_done():
        return await interaction.followup.send(wait=True, ephemeral=ephemeral, **kwargs)

    await interaction.response.send_message(ephemeral=ephemeral, **kwargs)
    return await interaction.original_response()


async def require_channel_permissions(
    ctx: commands.Context,
    required_permissions,
    command_name: str,
) -> bool:
    if getattr(ctx, "interaction", None) is not None:
        return await ge.require_bot_permissions(ctx.interaction, required_permissions, command_name)
    return await ge.require_bot_permissions_prefix(ctx, required_permissions, command_name)


def is_slash_invocation(ctx: commands.Context) -> bool:
    return getattr(ctx, "interaction", None) is not None
