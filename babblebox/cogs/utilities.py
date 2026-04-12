from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, require_channel_permissions, send_hybrid_response
from babblebox.utility_helpers import build_jump_view, deserialize_datetime
from babblebox.utility_service import WATCH_KEYWORD_LIMIT, UtilityService


WATCH_SCOPE_CHOICES = [
    app_commands.Choice(name="This channel", value="channel"),
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
RETURN_WATCH_DURATION_SECONDS = {
    "1h": 3600,
    "6h": 6 * 3600,
    "24h": 24 * 3600,
}
RETURN_WATCH_DURATION_CHOICES = [
    app_commands.Choice(name="1 hour", value="1h"),
    app_commands.Choice(name="6 hours", value="6h"),
    app_commands.Choice(name="24 hours", value="24h"),
]


class AfkReturnWatchDurationSelect(discord.ui.Select):
    def __init__(self, cog: "UtilityCog", *, guild_id: int, target_user_id: int, target_name: str):
        super().__init__(
            placeholder="Keep this return ping for...",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="1 hour", value="1h", description="Quiet one-shot ping"),
                discord.SelectOption(label="6 hours", value="6h", description="Good for the rest of the day"),
                discord.SelectOption(label="24 hours", value="24h", description="Longest V1 window"),
            ],
        )
        self.cog = cog
        self.guild_id = guild_id
        self.target_user_id = target_user_id
        self.target_name = target_name

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild or interaction.client.get_guild(self.guild_id)
        watcher = guild.get_member(interaction.user.id) if guild is not None else None
        target = guild.get_member(self.target_user_id) if guild is not None else None
        if watcher is None or target is None:
            await interaction.response.edit_message(
                embed=ge.make_status_embed(
                    "Return Ping Unavailable",
                    "I couldn't confirm that alert safely in this server anymore.",
                    tone="warning",
                    footer="Babblebox Watch",
                ),
                view=None,
            )
            return
        ok, message = await self.cog._create_user_return_watch(
            watcher=watcher,
            guild=guild,
            target=target,
            duration_key=self.values[0],
            created_from="afk_button",
        )
        await interaction.response.edit_message(
            embed=ge.make_status_embed(
                "Return Ping Ready" if ok else "Return Ping Unavailable",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Watch",
            ),
            view=None,
        )


class AfkReturnWatchDurationView(discord.ui.View):
    def __init__(self, cog: "UtilityCog", *, guild_id: int, target_user_id: int, target_name: str):
        super().__init__(timeout=60)
        self.add_item(
            AfkReturnWatchDurationSelect(
                cog,
                guild_id=guild_id,
                target_user_id=target_user_id,
                target_name=target_name,
            )
        )


class AfkReturnWatchButton(discord.ui.Button):
    def __init__(self, cog: "UtilityCog", *, guild_id: int, target_user_id: int):
        super().__init__(
            label="Notify me when they're back",
            style=discord.ButtonStyle.secondary,
        )
        self.cog = cog
        self.guild_id = guild_id
        self.target_user_id = target_user_id

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild or interaction.client.get_guild(self.guild_id)
        target = guild.get_member(self.target_user_id) if guild is not None else None
        if target is None:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Return Ping Unavailable",
                    "I couldn't find that person in this server anymore.",
                    tone="warning",
                    footer="Babblebox Watch",
                ),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "Return Ping",
                f"I'll keep a quiet one-shot alert ready for **{ge.display_name_of(target)}**. Pick how long it should stay active.",
                tone="info",
                footer="Babblebox Watch",
            ),
            view=AfkReturnWatchDurationView(
                self.cog,
                guild_id=self.guild_id,
                target_user_id=self.target_user_id,
                target_name=ge.display_name_of(target),
            ),
            ephemeral=True,
        )


class AfkReturnWatchView(discord.ui.View):
    def __init__(self, cog: "UtilityCog", *, guild_id: int, target_user_id: int):
        super().__init__(timeout=30)
        self.add_item(AfkReturnWatchButton(cog, guild_id=guild_id, target_user_id=target_user_id))


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

    def _profile_service(self):
        return getattr(self.bot, "profile_service", None)

    async def _record_utility_action(self, user_id: int, action: str):
        profile_service = self._profile_service()
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            return
        await profile_service.record_utility_action(user_id, action)

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

    def _watch_channel_id(self, channel) -> int | None:
        return channel.id if isinstance(channel, (discord.TextChannel, discord.Thread)) else None

    def _parse_return_watch_duration(self, raw: str) -> tuple[bool, int | str]:
        duration_seconds = RETURN_WATCH_DURATION_SECONDS.get(str(raw or "").strip().lower())
        if duration_seconds is None:
            return False, "Pick 1 hour, 6 hours, or 24 hours for this alert."
        return True, duration_seconds

    def _return_watch_duration_text(self, duration_seconds: int) -> str:
        if duration_seconds == 3600:
            return "1 hour"
        if duration_seconds == 6 * 3600:
            return "6 hours"
        if duration_seconds == 24 * 3600:
            return "24 hours"
        return f"{duration_seconds // 3600} hours"

    async def _create_user_return_watch(
        self,
        *,
        watcher: discord.abc.User,
        guild: discord.Guild | None,
        target: discord.abc.User,
        duration_key: str,
        created_from: str,
    ) -> tuple[bool, str]:
        if guild is None:
            return False, "User return pings only work inside a server."
        if target.id == watcher.id:
            return False, "I'll only ping you when someone else speaks again."
        if target.bot:
            return False, "Bots don't need a return ping."
        ok, duration_or_error = self._parse_return_watch_duration(duration_key)
        if not ok:
            return False, str(duration_or_error)
        duration_seconds = int(duration_or_error)
        ok, record_or_error, refreshed = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=guild.id,
            target_type="user",
            target_id=target.id,
            duration_seconds=duration_seconds,
            created_from=created_from,
        )
        if not ok:
            return False, str(record_or_error)
        duration_text = self._return_watch_duration_text(duration_seconds)
        if refreshed:
            return True, f"I'll keep that return ping for **{ge.display_name_of(target)}** active for another {duration_text}."
        return True, f"I'll DM you when **{ge.display_name_of(target)}** sends their next message in this server. This return ping expires in {duration_text}."

    async def _create_channel_return_watch(
        self,
        *,
        watcher: discord.Member,
        channel: discord.TextChannel | discord.Thread | None,
        duration_key: str,
        created_from: str,
    ) -> tuple[bool, str]:
        if channel is None:
            return False, "Channel alerts only work in server text channels and threads."
        perms = channel.permissions_for(watcher)
        if not (perms.view_channel and perms.read_message_history):
            return False, "You need access to that channel before I can keep a private alert for it."
        ok, duration_or_error = self._parse_return_watch_duration(duration_key)
        if not ok:
            return False, str(duration_or_error)
        duration_seconds = int(duration_or_error)
        ok, record_or_error, refreshed = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=channel.guild.id,
            target_type="channel",
            target_id=channel.id,
            duration_seconds=duration_seconds,
            created_from=created_from,
        )
        if not ok:
            return False, str(record_or_error)
        duration_text = self._return_watch_duration_text(duration_seconds)
        if refreshed:
            return True, f"I'll keep that channel alert for {channel.mention} active for another {duration_text}."
        return True, f"I'll DM you when {channel.mention} gets its next message. This alert expires in {duration_text}."

    def build_afk_return_watch_view(self, *, guild_id: int, target_user_id: int) -> discord.ui.View:
        return AfkReturnWatchView(self, guild_id=guild_id, target_user_id=target_user_id)

    def _render_watch_keywords(self, items: list[dict]) -> str:
        if not items:
            return "None saved."
        lines = []
        for item in items[:10]:
            scope = "global"
            if item.get("channel_id") is not None:
                scope = "channel"
            elif item.get("guild_id") is not None:
                scope = "server"
            mode = "whole word" if item.get("mode") == "word" else "contains"
            lines.append(f"`{item['phrase']}` - {mode} - {scope}")
        if len(items) > 10:
            lines.append(f"...and {len(items) - 10} more")
        return "\n".join(lines)

    def _resolve_watch_channel_mentions(self, guild: discord.Guild | None, channel_ids: list[int]) -> str:
        if not channel_ids:
            return "None"
        rendered = []
        for channel_id in channel_ids[:5]:
            channel = self.bot.get_channel(channel_id) if guild is not None else None
            if channel is not None and getattr(channel, "mention", None):
                rendered.append(channel.mention)
            else:
                rendered.append(f"`{channel_id}`")
        if len(channel_ids) > 5:
            rendered.append(f"+{len(channel_ids) - 5} more")
        return ", ".join(rendered)

    def _resolve_ignored_user_labels(self, guild: discord.Guild | None, user_ids: list[int]) -> str:
        if not user_ids:
            return "None"
        rendered = []
        for user_id in user_ids[:5]:
            member = guild.get_member(user_id) if guild is not None else None
            user = member or self.bot.get_user(user_id)
            rendered.append(ge.display_name_of(user) if user is not None else f"`{user_id}`")
        if len(user_ids) > 5:
            rendered.append(f"+{len(user_ids) - 5} more")
        return ", ".join(rendered)

    def _watch_settings_embed(
        self,
        user: discord.abc.User,
        guild: discord.Guild | None,
        channel: discord.abc.GuildChannel | discord.Thread | None,
    ) -> discord.Embed:
        summary = self.service.get_watch_summary(
            user.id,
            guild_id=guild.id if guild else None,
            channel_id=self._watch_channel_id(channel),
        )
        embed = discord.Embed(
            title="Watch Settings",
            description=(
                f"Quiet DM alerts for **{ge.display_name_of(user)}**. "
                "Mentions mean explicit `@user` tags, replies mean replies to your message, and keywords stay split and compact."
            ),
            color=ge.EMBED_THEME["accent"],
        )
        alert_lines = [
            f"Mentions: global **{'On' if summary['mention_global'] else 'Off'}**",
            f"Replies: global **{'On' if summary['reply_global'] else 'Off'}**",
        ]
        if guild is not None:
            alert_lines.append(f"This server: mentions **{'On' if summary['mention_server_enabled'] else 'Off'}** | replies **{'On' if summary['reply_server_enabled'] else 'Off'}**")
        if channel is not None:
            alert_lines.append(f"This channel: mentions **{'On' if summary['mention_channel_enabled'] else 'Off'}** | replies **{'On' if summary['reply_channel_enabled'] else 'Off'}**")
        embed.add_field(name="Alert Modes", value="\n".join(alert_lines), inline=False)
        embed.add_field(
            name="Keyword Buckets",
            value=(
                f"Global: **{len(summary['global_keywords'])}**\n"
                f"Server: **{len(summary['server_keywords'])}**\n"
                f"Channel: **{len(summary['channel_keywords'])}**\n"
                f"Saved total: **{summary['total_keywords']} / {WATCH_KEYWORD_LIMIT}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Focused Channels",
            value=(
                f"Mentions: {self._resolve_watch_channel_mentions(guild, summary['mention_channel_ids'])}\n"
                f"Replies: {self._resolve_watch_channel_mentions(guild, summary['reply_channel_ids'])}"
            ),
            inline=True,
        )
        counts = summary["recent_counts"]
        embed.add_field(
            name="Filters",
            value=(
                f"Ignored channels: {self._resolve_watch_channel_mentions(guild, summary['ignored_channel_ids'])}\n"
                f"Ignored users: {self._resolve_ignored_user_labels(guild, summary['ignored_user_ids'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Recent Pings",
            value=(
                f"Mentions: **{counts['mentions']}**\n"
                f"Replies: **{counts['replies']}**\n"
                f"Keywords: **{counts['keywords']}**\n"
                f"Total sent: **{counts['total']}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value=(
                "`/watch mentions on server`\n"
                "`/watch replies on channel`\n"
                "`/watch keyword add channel contains camera`\n"
                "Mentions = explicit `@user` tags | replies = Discord replies to your message"
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Watch | DM-only alerts, compact filters, no message archive")

    def _watch_list_embed(
        self,
        user: discord.abc.User,
        guild: discord.Guild | None,
        channel: discord.abc.GuildChannel | discord.Thread | None,
    ) -> discord.Embed:
        summary = self.service.get_watch_summary(
            user.id,
            guild_id=guild.id if guild else None,
            channel_id=self._watch_channel_id(channel),
        )
        embed = discord.Embed(
            title="Watch Keywords",
            description=f"What can trigger a Watch DM for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(name="Global", value=self._render_watch_keywords(summary["global_keywords"]), inline=False)
        if guild is not None:
            embed.add_field(name=f"{guild.name}", value=self._render_watch_keywords(summary["server_keywords"]), inline=False)
        if channel is not None:
            embed.add_field(name="This Channel", value=self._render_watch_keywords(summary["channel_keywords"]), inline=False)
        embed.add_field(
            name="Focused Channels",
            value=(
                f"Mentions: {self._resolve_watch_channel_mentions(guild, summary['mention_channel_ids'])}\n"
                f"Replies: {self._resolve_watch_channel_mentions(guild, summary['reply_channel_ids'])}"
            ),
            inline=False,
        )
        embed.add_field(name="Note", value="Watch stays DM-only and never stores a message archive.", inline=False)
        return ge.style_embed(embed, footer="Babblebox Watch | Use /watch ignore to trim noisy places or people")

    def _later_list_embed(self, user: discord.abc.User, markers: list[dict], *, guild: discord.Guild | None) -> discord.Embed:
        embed = discord.Embed(
            title="Babblebox Later Markers",
            description=f"Saved reading markers for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["info"],
        )
        if not markers:
            embed.description += "\n\nNo markers yet. Use `/later mark` when you want a clean jump-back link."
            return ge.style_embed(embed, footer="Babblebox Later | Mark a channel with /later mark.")

        lines = []
        for marker in markers[:8]:
            saved_at = ge.format_timestamp(deserialize_datetime(marker.get("saved_at")), "R")
            location = f"{marker.get('guild_name', 'Unknown server')} / #{marker.get('channel_name', 'unknown')}"
            if guild is not None:
                location = f"#{marker.get('channel_name', 'unknown')}"
            preview = ge.safe_field_text(marker.get("preview", "[quiet message]"), limit=80)
            lines.append(f"**{location}** | {saved_at}\nBy {marker.get('author_name', 'Unknown')}\n{preview}")
        if len(markers) > 8:
            lines.append(f"...and {len(markers) - 8} more")
        embed.add_field(name="Markers", value="\n\n".join(lines), inline=False)
        embed.add_field(name="Quick Use", value="`/later mark` refreshes this channel\n`/later clear here` removes its saved marker", inline=False)
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
            retry_after = deserialize_datetime(reminder.get("retry_after"))
            destination = "DM" if reminder.get("delivery") == "dm" else f"#{reminder.get('channel_name', 'unknown')}"
            timing = ge.format_timestamp(due_at, "R")
            if retry_after is not None and retry_after > ge.now_utc():
                timing = f"Retrying delivery {ge.format_timestamp(retry_after, 'R')}"
            lines.append(
                f"`{reminder['id'][:8]}` - {timing} - {destination} - "
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
        description="Set quiet one-shot alerts plus mention, reply, and keyword DMs",
        invoke_without_command=True,
    )
    async def watch_group(self, ctx: commands.Context):
        await self.watch_settings_command(ctx)

    @watch_group.command(name="mentions", with_app_command=True, description="Enable or disable mention alerts")
    @app_commands.describe(state="Turn mention alerts on or off", scope="Use this channel, server, or global scope")
    @app_commands.choices(state=WATCH_STATE_CHOICES, scope=WATCH_SCOPE_CHOICES)
    async def watch_mentions_command(self, ctx: commands.Context, state: str = "on", scope: str = "server"):
        if not await self._require_storage(ctx, "Watch"):
            return
        enabled = state.lower() == "on"
        ok, message = await self.service.set_watch_mentions(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            channel_id=self._watch_channel_id(ctx.channel),
            scope=scope,
            enabled=enabled,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Mentions", message, tone=tone, footer="Babblebox Watch"),
        )

    @watch_group.command(name="replies", with_app_command=True, description="Enable or disable reply alerts")
    @app_commands.describe(state="Turn reply alerts on or off", scope="Use this channel, server, or global scope")
    @app_commands.choices(state=WATCH_STATE_CHOICES, scope=WATCH_SCOPE_CHOICES)
    async def watch_replies_command(self, ctx: commands.Context, state: str = "on", scope: str = "server"):
        if not await self._require_storage(ctx, "Watch"):
            return
        enabled = state.lower() == "on"
        ok, message = await self.service.set_watch_replies(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            channel_id=self._watch_channel_id(ctx.channel),
            scope=scope,
            enabled=enabled,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Replies", message, tone=tone, footer="Babblebox Watch"),
        )

    @watch_group.command(name="user", with_app_command=True, description="Ping me when someone's next message comes through")
    @app_commands.describe(user="The person to quietly ping you about", duration="How long this one-shot alert should stay active")
    @app_commands.choices(duration=RETURN_WATCH_DURATION_CHOICES)
    async def watch_user_command(self, ctx: commands.Context, user: discord.Member, duration: str = "6h"):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self._create_user_return_watch(
            watcher=ctx.author,
            guild=ctx.guild,
            target=user,
            duration_key=duration,
            created_from="slash_command",
        )
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                "Return Ping Ready" if ok else "Return Ping Unavailable",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Watch",
            ),
        )

    @watch_group.command(name="channel", with_app_command=True, description="Alert me when this channel is active again")
    @app_commands.describe(channel="Leave blank to use this channel", duration="How long this one-shot alert should stay active")
    @app_commands.choices(duration=RETURN_WATCH_DURATION_CHOICES)
    async def watch_channel_command(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
        duration: str = "6h",
    ):
        if not await self._require_storage(ctx, "Watch"):
            return
        target_channel = channel
        if target_channel is None and isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
            target_channel = ctx.channel
        ok, message = await self._create_channel_return_watch(
            watcher=ctx.author,
            channel=target_channel,
            duration_key=duration,
            created_from="slash_command",
        )
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                "Channel Alert Ready" if ok else "Channel Alert Unavailable",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Watch",
            ),
        )

    @watch_group.group(name="keyword", with_app_command=True, invoke_without_command=True, description="Manage keyword alerts")
    async def watch_keyword_group(self, ctx: commands.Context):
        await self.watch_list_command(ctx)

    @watch_keyword_group.command(name="add", with_app_command=True, description="Add a watched keyword")
    @app_commands.describe(scope="Use this channel, server, or global scope", mode="Contains phrase or whole word", phrase="The keyword or phrase to watch")
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
            channel_id=self._watch_channel_id(ctx.channel),
            phrase=phrase,
            scope=scope,
            mode=mode,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Keyword", message, tone=tone, footer="Babblebox Watch"),
        )
        if ok:
            await self._record_utility_action(ctx.author.id, "watch_keyword")

    @watch_keyword_group.command(name="remove", with_app_command=True, description="Remove a watched keyword")
    @app_commands.describe(scope="Remove from this channel, server, or global scope", phrase="The exact saved keyword or phrase")
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
            channel_id=self._watch_channel_id(ctx.channel),
            phrase=phrase,
            scope=scope,
        )
        tone = "success" if ok else "warning"
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Keyword", message, tone=tone, footer="Babblebox Watch"),
        )

    @watch_group.group(name="ignore", with_app_command=True, invoke_without_command=True, description="Ignore noisy channels or users")
    async def watch_ignore_group(self, ctx: commands.Context):
        await self.watch_settings_command(ctx)

    @watch_ignore_group.command(name="channel", with_app_command=True, description="Exclude this channel from Watch alerts")
    async def watch_ignore_channel_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self.service.add_watch_ignored_channel(
            ctx.author.id,
            channel_id=self._watch_channel_id(ctx.channel),
        )
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Ignore", message, tone="success" if ok else "warning", footer="Babblebox Watch"),
        )

    @watch_ignore_group.command(name="channel-remove", with_app_command=True, description="Remove this channel from your ignore list")
    async def watch_ignore_channel_remove_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self.service.remove_watch_ignored_channel(
            ctx.author.id,
            channel_id=self._watch_channel_id(ctx.channel),
        )
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Ignore", message, tone="success" if ok else "warning", footer="Babblebox Watch"),
        )

    @watch_ignore_group.command(name="user", with_app_command=True, description="Ignore a specific user's messages")
    @app_commands.describe(user="The user to ignore in Watch alerts")
    async def watch_ignore_user_command(self, ctx: commands.Context, user: discord.User):
        if not await self._require_storage(ctx, "Watch"):
            return
        if user.bot:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Watch Ignore", "Bots are already ignored by Watch.", tone="warning", footer="Babblebox Watch"),
            )
            return
        ok, message = await self.service.add_watch_ignored_user(ctx.author.id, ignored_user_id=user.id)
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Ignore", message, tone="success" if ok else "warning", footer="Babblebox Watch"),
        )

    @watch_ignore_group.command(name="user-remove", with_app_command=True, description="Remove a user from your Watch ignore list")
    @app_commands.describe(user="The user to remove from your ignore list")
    async def watch_ignore_user_remove_command(self, ctx: commands.Context, user: discord.User):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self.service.remove_watch_ignored_user(ctx.author.id, ignored_user_id=user.id)
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Watch Ignore", message, tone="success" if ok else "warning", footer="Babblebox Watch"),
        )

    @watch_group.command(name="settings", with_app_command=True, description="View watch settings")
    async def watch_settings_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Watch"):
            return
        await self._send_private_embed(ctx, embed=self._watch_settings_embed(ctx.author, ctx.guild, ctx.channel if ctx.guild else None))

    @watch_group.command(name="list", with_app_command=True, description="List watched keywords")
    async def watch_list_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Watch"):
            return
        await self._send_private_embed(ctx, embed=self._watch_list_embed(ctx.author, ctx.guild, ctx.channel if ctx.guild else None))

    @watch_group.command(name="off", with_app_command=True, description="Disable watch settings for a scope")
    @app_commands.describe(scope="Clear this channel, server, global, or all watch settings")
    @app_commands.choices(scope=WATCH_OFF_SCOPE_CHOICES)
    async def watch_off_command(self, ctx: commands.Context, scope: str = "server"):
        if not await self._require_storage(ctx, "Watch"):
            return
        ok, message = await self.service.disable_watch(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            channel_id=self._watch_channel_id(ctx.channel),
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
        await self._record_utility_action(ctx.author.id, "later")

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
        await self._record_utility_action(ctx.author.id, "capture")

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
        await self._record_utility_action(ctx.author.id, "reminder")

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
