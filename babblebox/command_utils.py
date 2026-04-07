from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import discord
from discord.ext import commands

from babblebox import game_engine as ge


@dataclass
class HybridPanelSendResult:
    delivered: bool
    message: discord.Message | None = None
    message_id: int | None = None
    path: str = "prefix"
    handle_status: str = "missing"
    error: Exception | None = None


def _build_hybrid_send_kwargs(
    content: str | None,
    *,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
    ephemeral: bool = False,
    delete_after: float | None = None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if content is not None:
        kwargs["content"] = content
    if embed is not None:
        kwargs["embed"] = embed
    if view is not None:
        kwargs["view"] = view
    if delete_after is not None:
        kwargs["delete_after"] = delete_after
    kwargs["ephemeral"] = ephemeral
    return kwargs


def _successful_panel_send(
    *,
    path: str,
    message: discord.Message | None = None,
    message_id: int | None = None,
    handle_status: str = "missing",
) -> HybridPanelSendResult:
    if message is not None and message_id is None:
        message_id = getattr(message, "id", None)
    return HybridPanelSendResult(
        delivered=True,
        message=message,
        message_id=message_id,
        path=path,
        handle_status=handle_status,
    )


async def send_hybrid_panel_response(
    ctx: commands.Context,
    content: str | None = None,
    *,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
    ephemeral: bool = False,
    delete_after: float | None = None,
) -> HybridPanelSendResult:
    path = "context_send"
    try:
        message = await send_hybrid_response(
            ctx,
            content,
            embed=embed,
            view=view,
            ephemeral=ephemeral,
            delete_after=delete_after,
        )
        return _successful_panel_send(
            path=path,
            message=message,
            handle_status="available" if message is not None else "missing",
        )
    except (discord.ClientException, discord.HTTPException, discord.NotFound, TypeError, ValueError) as exc:
        return HybridPanelSendResult(delivered=False, path=path, error=exc)


async def defer_hybrid_response(
    ctx: commands.Context,
    *,
    ephemeral: bool = False,
) -> bool:
    interaction = getattr(ctx, "interaction", None)
    if interaction is None or interaction.is_expired() or interaction.response.is_done():
        return False
    try:
        await ctx.defer(ephemeral=ephemeral)
    except (discord.InteractionResponded, discord.NotFound):
        return False
    return True


async def send_hybrid_response(
    ctx: commands.Context,
    content: str | None = None,
    *,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
    ephemeral: bool = False,
    delete_after: float | None = None,
) -> discord.Message | None:
    kwargs = _build_hybrid_send_kwargs(
        content,
        embed=embed,
        view=view,
        ephemeral=ephemeral,
        delete_after=delete_after,
    )
    return await ctx.send(**kwargs)


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


async def is_command_message(bot: commands.Bot, message: discord.Message) -> bool:
    prefixes = await bot.get_prefix(message)
    if isinstance(prefixes, str):
        prefixes = [prefixes]
    return any(message.content.startswith(prefix) for prefix in prefixes)
