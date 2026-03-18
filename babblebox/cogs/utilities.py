from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, require_channel_permissions, send_hybrid_response
from babblebox.utility_helpers import deserialize_datetime
from babblebox.utility_service import WATCH_KEYWORD_LIMIT, UtilityService


WATCH_SCOPE_CHOICES = [
    app_commands.Choice(name="This server", value="server"),
    app_commands.Choice(name="Global", value="global"),
]
WATCH_OFF_SCOPE_CHOICES = WATCH_SCOPE_CHOICES + [app_commands.Choice(name="All watch settings", value="all")]
WATCH_STATE_CHOICES = [
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
]
WATCH_MODE_CHOICES = [
    app_commands.Choice(name="Contains phrase", value="contains"),
    app_commands.Choice(name="Whole word", value="word"),
]
REMINDER_DELIVERY_CHOICES = [
    app_commands.Choice(name="DM me", value="dm"),
    app_commands.Choice(name="This channel", value="here"),
]
LATER_CLEAR_CHOICES = [
    app_commands.Choice(name="This channel", value="here"),
    app_commands.Choice(name="All saved markers", value="all"),
]
CAPTURE_REQUIRED_PERMS = ("view_channel", "read_message_history", "send_messages", "embed_links")
LATER_REQUIRED_PERMS = ("view_channel", "read_message_history", "send_messages", "embed_links")


class UtilityCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = UtilityService(bot)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "utility_service", self.service)

    def cog_unload(self):
        if getattr(self.bot, "utility_service", None) is self.service:
            delattr(self.bot, "utility_service")
        self.bot.loop.create_task(self.service.close())

    async def _send_private_embed(
        self,
        ctx: commands.Context,
        *,
        embed: discord.Embed,
        delete_after: float | None = None,
    ):
        return await send_hybrid_response(
            ctx,
            embed=embed,
            ephemeral=True,
            delete_after=delete_after,
        )

    async def _send_short_confirmation(self, ctx: commands.Context, title: str, description: str, *, tone: str = "success"):
        return await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(title, description, tone=tone, footer="Babblebox Utilities"),
            ephemeral=True,
            delete_after=10.0,
        )

    async def _send_usage(self, ctx: commands.Context, title: str, description: str):
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(title, description, tone="info", footer="Babblebox Utilities"),
        )

    async def _require_storage(self, ctx: commands.Context, feature_name: str) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if self.service.storage_ready:
            return True
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                f"{feature_name} Unavailable",
                self.service.storage_message(feature_name),
                tone="warning",
                footer="Babblebox Utilities",
            ),
        )
        return False

    def _watch_settings_embed(self, user: discord.abc.User, guild: discord.Guild | None) -> discord.Embed:
        summary = self.service.get_watch_summary(user.id, guild_id=guild.id if guild else None)
        embed = discord.Embed(
            title="Babblebox Watch Settings",
            description=f"Watch settings for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["accent"],
        )
        if guild is not None:
            embed.add_field(
                name="Mention Alerts",
                value=(
                    f"This server: {'On' if summary['mention_server_enabled'] else 'Off'}\n"
                    f"Global: {'On' if summary['mention_global'] else 'Off'}"
                ),
                inline=False,
            )
            server_keywords = summary["server_keywords"]
            global_keywords = summary["global_keywords"]
            embed.add_field(name=f"{guild.name} Keywords", value=str(len(server_keywords)), inline=True)
            embed.add_field(name="Global Keywords", value=str(len(global_keywords)), inline=True)
        else:
            embed.add_field(
                name="Mention Alerts",
                value=f"Global: {'On' if summary['mention_global'] else 'Off'}",
                inline=False,
            )
            embed.add_field(name="Global Keywords", value=str(len(summary["global_keywords"])), inline=True)
        embed.add_field(name="Keyword Limit", value=str(WATCH_KEYWORD_LIMIT), inline=True)
        return ge.style_embed(embed, footer="Babblebox Watch | Use /watch keyword add or bb!watch keyword add.")

    def _watch_list_embed(self, user: discord.abc.User, guild: discord.Guild | None) -> discord.Embed:
        summary = self.service.get_watch_summary(user.id, guild_id=guild.id if guild else None)
        embed = discord.Embed(
            title="Babblebox Watch Keywords",
            description=f"Saved keyword watches for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["accent"],
        )

        def render(items: list[dict]) -> str:
            if not items:
                return "None"
            lines = []
            for item in items[:10]:
                lines.append(f"`{item['phrase']}` ({item.get('mode', 'contains')})")
            if len(items) > 10:
                lines.append(f"...and {len(items) - 10} more")
            return "\n".join(lines)

        embed.add_field(name="Global", value=render(summary["global_keywords"]), inline=False)
        if guild is not None:
            embed.add_field(name=guild.name, value=render(summary["server_keywords"]), inline=False)
        return ge.style_embed(embed, footer="Babblebox Watch | Message alerts are DM-only.")

    def _later_list_embed(self, user: discord.abc.User, markers: list[dict], *, guild: discord.Guild | None) -> discord.Embed:
        embed = discord.Embed(
            title="Babblebox Later Markers",
            description=f"Saved reading markers for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["info"],
        )
        if not markers:
            embed.description += "\n\nYou do not have any saved markers."
            return ge.style_embed(embed, footer="Babblebox Later | Mark a channel with /later mark.")

        lines = []
        for marker in markers[:10]:
            saved_at = ge.format_timestamp(deserialize_datetime(marker.get("saved_at")), "R")
            location = f"{marker.get('guild_name', 'Unknown server')} / #{marker.get('channel_name', 'unknown')}"
            if guild is not None:
                location = f"#{marker.get('channel_name', 'unknown')}"
            lines.append(f"**{location}** - {marker.get('author_name', 'Unknown')} - saved {saved_at}")
        if len(markers) > 10:
            lines.append(f"...and {len(markers) - 10} more")
        embed.add_field(name="Markers", value="\n".join(lines), inline=False)
        return ge.style_embed(embed, footer="Babblebox Later | Use /later clear or bb!later clear.")

    def _reminder_list_embed(self, user: discord.abc.User, reminders: list[dict]) -> discord.Embed:
        embed = discord.Embed(
            title="Babblebox Reminders",
            description=f"Active reminders for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["success"],
        )
        if not reminders:
            embed.description += "\n\nYou do not have any active reminders."
            return ge.style_embed(embed, footer="Babblebox Remind | Use /remind set to add one.")

        lines = []
        for reminder in reminders[:10]:
            due_at = deserialize_datetime(reminder.get("due_at"))
            destination = "DM" if reminder.get("delivery") == "dm" else f"#{reminder.get('channel_name', 'unknown')}"
            lines.append(
                f"`{reminder['id'][:8]}` - {ge.format_timestamp(due_at, 'R')} - {destination} - "
                f"{ge.safe_field_text(reminder.get('text', ''), limit=70)}"
            )
        if len(reminders) > 10:
            lines.append(f"...and {len(reminders) - 10} more")
        embed.add_field(name="Scheduled", value="\n".join(lines), inline=False)
        return ge.style_embed(embed, footer="Babblebox Remind | Cancel with /remind cancel <id>.")

    async def _resolve_later_target(self, ctx: commands.Context) -> discord.Message | None:
        if ctx.guild is None or ctx.channel is None:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "Later markers only make sense in server channels.",
                    tone="warning",
                    footer="Babblebox Later",
                ),
            )
            return None
        if not await require_channel_permissions(ctx, LATER_REQUIRED_PERMS, "/later mark"):
            return None

        history_kwargs = {"limit": 15}
        if getattr(ctx, "message", None) is not None:
            history_kwargs["before"] = ctx.message

        async for candidate in ctx.channel.history(**history_kwargs):
            if candidate.type not in {discord.MessageType.default, discord.MessageType.reply}:
                continue
            if candidate.author.bot:
                continue
            return candidate

        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                "Nothing To Mark",
                "I could not find a recent message in this channel to use as your reading marker.",
                tone="warning",
                footer="Babblebox Later",
            ),
        )
        return None

    @commands.hybrid_group(
        name="watch",
        with_app_command=True,
        description="Configure mention and keyword DM alerts",
        invoke_without_command=True,
    )
    async def watch_group(self, ctx: commands.Context):
        await self.watch_settings_command(ctx)

    @watch_group.command(name="mentions", with_app_command=True, description="Enable or disable mention alerts")
    @app_commands.describe(state="Turn mention alerts on or off", scope="Use this server or global scope")
    @app_commands.choices(state=WATCH_STATE_CHOICES, scope=WATCH_SCOPE_CHOICES)
    async def watch_mentions_command(self, ctx: commands.Context, state: str = "on", scope: str = "server"):
        if not await self._require_storage(ctx, "Watch"):
            return
        enabled = state.lower() == "on"
        ok, message = await self.service.set_watch_mentions(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            scope=scope,
            enabled=enabled,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Mentions", message, tone=tone, footer="Babblebox Watch"),
        )

    @watch_group.group(name="keyword", with_app_command=True, invoke_without_command=True, description="Manage keyword alerts")
    async def watch_keyword_group(self, ctx: commands.Context):
        await self.watch_list_command(ctx)

    @watch_keyword_group.command(name="add", with_app_command=True, description="Add a watched keyword")
    @app_commands.describe(scope="Use this server or global scope", mode="Contains phrase or whole word", phrase="The keyword or phrase to watch")
    @app_commands.choices(scope=WATCH_SCOPE_CHOICES, mode=WATCH_MODE_CHOICES)
    async def watch_keyword_add_command(
        self,
        ctx: commands.Context,
        scope: str = "server",
        mode: str = "contains",
        *,
        phrase: str,
    ):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self.service.add_watch_keyword(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            phrase=phrase,
            scope=scope,
            mode=mode,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Keyword", message, tone=tone, footer="Babblebox Watch"),
        )

    @watch_keyword_group.command(name="remove", with_app_command=True, description="Remove a watched keyword")
    @app_commands.describe(scope="Remove from this server or global scope", phrase="The exact saved keyword or phrase")
    @app_commands.choices(scope=WATCH_SCOPE_CHOICES)
    async def watch_keyword_remove_command(
        self,
        ctx: commands.Context,
        scope: str = "server",
        *,
        phrase: str,
    ):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self.service.remove_watch_keyword(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            phrase=phrase,
            scope=scope,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Keyword", message, tone=tone, footer="Babblebox Watch"),
        )

    @watch_group.command(name="settings", with_app_command=True, description="View watch settings")
    async def watch_settings_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Watch"):
            return
        await self._send_private_embed(ctx, embed=self._watch_settings_embed(ctx.author, ctx.guild))

    @watch_group.command(name="list", with_app_command=True, description="List watched keywords")
    async def watch_list_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Watch"):
            return
        await self._send_private_embed(ctx, embed=self._watch_list_embed(ctx.author, ctx.guild))

    @watch_group.command(name="off", with_app_command=True, description="Disable watch settings for a scope")
    @app_commands.describe(scope="Clear this server, global, or all watch settings")
    @app_commands.choices(scope=WATCH_OFF_SCOPE_CHOICES)
    async def watch_off_command(self, ctx: commands.Context, scope: str = "server"):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self.service.disable_watch(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            scope=scope,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Updated", message, tone=tone, footer="Babblebox Watch"),
        )

    @commands.hybrid_group(
        name="later",
        with_app_command=True,
        description="Save a reading marker for this channel",
        invoke_without_command=True,
    )
    async def later_group(self, ctx: commands.Context):
        await self.later_mark_command(ctx)

    @later_group.command(name="mark", with_app_command=True, description="Mark where you stopped reading")
    async def later_mark_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Later"):
            return
        target = await self._resolve_later_target(ctx)
        if target is None:
            return

        ok, marker = await self.service.save_later_marker(user=ctx.author, channel=ctx.channel, message=target)
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Later Unavailable", marker, tone="warning", footer="Babblebox Later"),
            )
            return
        try:
            await self.service.send_later_marker_dm(ctx.author, marker)
        except discord.Forbidden:
            await self.service.clear_later_marker(ctx.author.id, channel_id=ctx.channel.id)
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "DMs Required",
                    "I saved nothing because I could not DM you the Later link. Please open your DMs and try again.",
                    tone="warning",
                    footer="Babblebox Later",
                ),
            )
            return

        await self._send_short_confirmation(
            ctx,
            "Later Marker Saved",
            f"I DM'd you a jump link for **#{ctx.channel.name}**.",
        )

    @later_group.command(name="list", with_app_command=True, description="List your saved reading markers")
    async def later_list_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Later"):
            return
        markers = self.service.list_later_markers(ctx.author.id, guild_id=ctx.guild.id if ctx.guild else None)
        await self._send_private_embed(
            ctx,
            embed=self._later_list_embed(ctx.author, markers, guild=ctx.guild),
        )

    @later_group.command(name="clear", with_app_command=True, description="Clear Later markers")
    @app_commands.describe(scope="Clear just this channel or all of your markers")
    @app_commands.choices(scope=LATER_CLEAR_CHOICES)
    async def later_clear_command(self, ctx: commands.Context, scope: str = "here"):
        if not await self._require_storage(ctx, "Later"):
            return
        if scope == "here" and ctx.guild is None:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "Use `all` in DMs, or run this in a server channel to clear only that channel's marker.",
                    tone="warning",
                    footer="Babblebox Later",
                ),
            )
            return
        channel_id = None if scope == "all" else (ctx.channel.id if ctx.guild and ctx.channel else None)
        ok, message = await self.service.clear_later_marker(ctx.author.id, channel_id=channel_id)
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Later Updated", message, tone=tone, footer="Babblebox Later"),
        )

    @commands.hybrid_command(name="capture", with_app_command=True, description="DM yourself a snapshot of recent channel messages")
    @app_commands.describe(count="How many recent messages to capture (5-25)")
    async def capture_command(self, ctx: commands.Context, count: int = 10):
        if ctx.guild is None or ctx.channel is None:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "Capture only works in server channels.",
                    tone="warning",
                    footer="Babblebox Capture",
                ),
            )
            return
        await defer_hybrid_response(ctx, ephemeral=True)
        if not await require_channel_permissions(ctx, CAPTURE_REQUIRED_PERMS, "/capture"):
            return
        if not (5 <= count <= 25):
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Invalid Count",
                    "Capture count must be between 5 and 25 messages.",
                    tone="warning",
                    footer="Babblebox Capture",
                ),
            )
            return
        allowed, error = self.service.can_run_capture(ctx.author.id)
        if not allowed:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Capture Cooldown", error, tone="warning", footer="Babblebox Capture"),
            )
            return

        history_kwargs = {"limit": count}
        if getattr(ctx, "message", None) is not None:
            history_kwargs["before"] = ctx.message

        messages = [
            message
            async for message in ctx.channel.history(**history_kwargs)
            if message.type in {discord.MessageType.default, discord.MessageType.reply}
        ]
        if not messages:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Nothing To Capture",
                    "I could not find recent messages to capture in this channel.",
                    tone="warning",
                    footer="Babblebox Capture",
                ),
            )
            return

        try:
            await self.service.send_capture_dm(
                user=ctx.author,
                guild_name=ctx.guild.name,
                channel_name=ctx.channel.name,
                messages=messages,
                requested_count=count,
            )
        except discord.Forbidden:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "DMs Required",
                    "I could not DM the capture transcript to you. Please open your DMs and try again.",
                    tone="warning",
                    footer="Babblebox Capture",
                ),
            )
            return

        await self._send_short_confirmation(
            ctx,
            "Capture Sent",
            f"I DM'd you a private snapshot of **{len(messages)}** recent messages.",
        )

    @commands.hybrid_group(
        name="remind",
        with_app_command=True,
        description="Create and manage one-time reminders",
        invoke_without_command=True,
    )
    async def remind_group(self, ctx: commands.Context):
        await self._send_usage(
            ctx,
            "Babblebox Remind",
            "Use `/remind set` or `bb!remind set <duration> <dm|here> <text>` to create a reminder.",
        )

    @remind_group.command(name="set", with_app_command=True, description="Create a one-time reminder")
    @app_commands.describe(when="Relative time like 10m, 2h, or 1d12h", delivery="DM me or post in this channel", text="Reminder text")
    @app_commands.choices(delivery=REMINDER_DELIVERY_CHOICES)
    async def remind_set_command(
        self,
        ctx: commands.Context,
        when: str,
        delivery: str = "dm",
        *,
        text: str,
    ):
        if not await self._require_storage(ctx, "Reminders"):
            return
        delay_seconds = self.service.parse_relative_duration(when)
        if delay_seconds is None:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Invalid Duration",
                    "Use a relative duration like `10m`, `2h`, `1d`, or `1d12h`.",
                    tone="warning",
                    footer="Babblebox Remind",
                ),
            )
            return
        if delivery == "here" and (ctx.guild is None or ctx.channel is None):
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Channel Delivery Unavailable",
                    "Channel reminders can only be created inside a server channel.",
                    tone="warning",
                    footer="Babblebox Remind",
                ),
            )
            return
        if delivery == "here" and not await require_channel_permissions(ctx, ("send_messages", "embed_links"), "/remind set"):
            return

        ok, result = await self.service.create_reminder(
            user=ctx.author,
            text=text,
            delay_seconds=delay_seconds,
            delivery=delivery,
            guild=ctx.guild,
            channel=ctx.channel,
            origin_jump_url=ctx.message.jump_url if getattr(ctx, "message", None) is not None and ctx.guild else None,
        )
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Reminder Rejected", result, tone="warning", footer="Babblebox Remind"),
            )
            return

        destination = "DM" if result["delivery"] == "dm" else f"#{result.get('channel_name', 'this channel')}"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                "Reminder Scheduled",
                f"I'll remind you in {when} via **{destination}**.\nID: `{result['id'][:8]}`",
                tone="success",
                footer="Babblebox Remind",
            ),
        )

    @remind_group.command(name="list", with_app_command=True, description="List your active reminders")
    async def remind_list_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Reminders"):
            return
        reminders = self.service.list_reminders(ctx.author.id)
        await self._send_private_embed(ctx, embed=self._reminder_list_embed(ctx.author, reminders))

    @remind_group.command(name="cancel", with_app_command=True, description="Cancel a reminder by ID")
    @app_commands.describe(reminder_id="The 8-character reminder ID shown in /remind list")
    async def remind_cancel_command(self, ctx: commands.Context, reminder_id: str):
        if not await self._require_storage(ctx, "Reminders"):
            return
        ok, message = await self.service.cancel_reminder(ctx.author.id, reminder_id)
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Reminder Updated", message, tone=tone, footer="Babblebox Remind"),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(UtilityCog(bot))
