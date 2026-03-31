from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.question_drops_content import QUESTION_DROP_CATEGORIES, QUESTION_DROP_TONES
from babblebox.question_drops_service import QuestionDropsService


STATE_CHOICES = [
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
]
TONE_CHOICES = [app_commands.Choice(name=value.title(), value=value) for value in QUESTION_DROP_TONES]
ACTIVITY_GATE_CHOICES = [
    app_commands.Choice(name="Light", value="light"),
    app_commands.Choice(name="Off", value="off"),
]
CATEGORY_CHOICES = [app_commands.Choice(name=value.title(), value=value) for value in QUESTION_DROP_CATEGORIES]
CHANNEL_ACTION_CHOICES = [
    app_commands.Choice(name="Add", value="add"),
    app_commands.Choice(name="Remove", value="remove"),
    app_commands.Choice(name="Clear", value="clear"),
]
CATEGORY_ACTION_CHOICES = [
    app_commands.Choice(name="Enable", value="enable"),
    app_commands.Choice(name="Disable", value="disable"),
    app_commands.Choice(name="Reset", value="reset"),
]
VISIBILITY_CHOICES = [
    app_commands.Choice(name="Public", value="public"),
    app_commands.Choice(name="Only me", value="private"),
]


class QuestionDropsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = QuestionDropsService(bot)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "question_drops_service", self.service)

    def cog_unload(self):
        if getattr(self.bot, "question_drops_service", None) is self.service:
            delattr(self.bot, "question_drops_service")
        self.bot.loop.create_task(self.service.close())

    async def _require_storage(self, ctx: commands.Context, feature_name: str, *, defer_response: bool = True) -> bool:
        if defer_response:
            await defer_hybrid_response(ctx, ephemeral=True)
        if self.service.storage_ready:
            return True
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                f"{feature_name} Unavailable",
                self.service.storage_message(feature_name),
                tone="warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )
        return False

    def _is_admin(self, member) -> bool:
        perms = getattr(member, "guild_permissions", None)
        return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))

    async def _require_admin(self, ctx: commands.Context) -> bool:
        if self._is_admin(ctx.author):
            return True
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Admin Only",
                "You need **Manage Server** or administrator access to configure Question Drops.",
                tone="warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )
        return False

    def _is_private(self, visibility: str) -> bool:
        return visibility == "private"

    @commands.hybrid_group(
        name="drops",
        with_app_command=True,
        description="Configure and inspect Babblebox Question Drops",
        invoke_without_command=True,
    )
    async def drops_group(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "Question Drops only work inside a server.",
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    @drops_group.command(name="panel", with_app_command=True, description="Open the Question Drops config panel")
    @app_commands.default_permissions(manage_guild=True)
    async def drops_panel_command(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(ctx, embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"), ephemeral=True)
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_admin(ctx):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    @drops_group.command(name="status", with_app_command=True, description="Show Question Drops status for this server")
    async def drops_status_command(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(ctx, embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"), ephemeral=True)
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    @drops_group.command(name="config", with_app_command=True, description="Update Question Drops timing and tone")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        enabled="Turn scheduled drops on or off",
        drops_per_day="How many drops per day to schedule (1-4)",
        timezone_name="Server timezone like Asia/Yerevan or UTC+04:00",
        answer_window_seconds="How many seconds answers stay open",
        tone_mode="Wrong-answer tone",
        activity_gate="Require recent chat activity before a drop can post",
        active_start_hour="Local hour when drops may start",
        active_end_hour="Local hour when drops stop",
    )
    @app_commands.choices(enabled=STATE_CHOICES, tone_mode=TONE_CHOICES, activity_gate=ACTIVITY_GATE_CHOICES)
    async def drops_config_command(
        self,
        ctx: commands.Context,
        *,
        enabled: Optional[str] = None,
        drops_per_day: Optional[int] = None,
        timezone_name: Optional[str] = None,
        answer_window_seconds: Optional[int] = None,
        tone_mode: Optional[str] = None,
        activity_gate: Optional[str] = None,
        active_start_hour: Optional[int] = None,
        active_end_hour: Optional[int] = None,
    ):
        if ctx.guild is None:
            await send_hybrid_response(ctx, embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"), ephemeral=True)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_config(
            ctx.guild.id,
            enabled={"on": True, "off": False}.get(enabled) if enabled is not None else None,
            drops_per_day=drops_per_day,
            timezone_name=timezone_name,
            answer_window_seconds=answer_window_seconds,
            tone_mode=tone_mode,
            activity_gate=activity_gate,
            active_start_hour=active_start_hour,
            active_end_hour=active_end_hour,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Question Drops Config",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    @drops_group.command(name="channels", with_app_command=True, description="Add or remove Question Drops channels")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(action="Add, remove, or clear channels", channel="Channel to update")
    @app_commands.choices(action=CHANNEL_ACTION_CHOICES)
    async def drops_channels_command(self, ctx: commands.Context, action: str, channel: Optional[discord.TextChannel] = None):
        if ctx.guild is None:
            await send_hybrid_response(ctx, embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"), ephemeral=True)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_channels(ctx.guild.id, action=action, channel_id=channel.id if channel is not None else None)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Question Drops Channels",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    @drops_group.command(name="categories", with_app_command=True, description="Enable or disable Question Drops categories")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(action="Enable, disable, or reset categories", category="Category to update")
    @app_commands.choices(action=CATEGORY_ACTION_CHOICES, category=CATEGORY_CHOICES)
    async def drops_categories_command(self, ctx: commands.Context, action: str, category: Optional[str] = None):
        if ctx.guild is None:
            await send_hybrid_response(ctx, embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"), ephemeral=True)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_categories(ctx.guild.id, action=action, category=category)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Question Drops Categories",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    @drops_group.command(name="stats", with_app_command=True, description="View Question Drops progress and top categories")
    @app_commands.describe(user="Whose Question Drops stats to view", visibility="Show the card publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def drops_stats_command(self, ctx: commands.Context, user: Optional[discord.User] = None, visibility: str = "public"):
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Profile Unavailable",
                    "Question Drops stats need the Babblebox profile store.",
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        target = user or ctx.author
        summary = await profile_service.get_question_drop_summary(target.id)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_stats_embed(target, summary),
            ephemeral=self._is_private(visibility),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(QuestionDropsCog(bot))
