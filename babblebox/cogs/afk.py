from __future__ import annotations

from datetime import datetime
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.premium_service import format_saved_state_status, preserved_over_limit_note
from babblebox.utility_helpers import (
    AFK_QUICK_REASONS,
    build_afk_reason_text,
    format_afk_timezone_label,
    get_afk_preset_default_duration,
    load_afk_timezone,
    parse_afk_clock_input,
    parse_afk_start_at,
    parse_afk_weekday,
)


AFK_PRESET_CHOICES = [
    app_commands.Choice(name=f"{payload['emoji']} {payload['label']}", value=key)
    for key, payload in AFK_QUICK_REASONS.items()
]
AFK_REPEAT_CHOICES = [
    app_commands.Choice(name="Every day", value="daily"),
    app_commands.Choice(name="Weekdays", value="weekdays"),
    app_commands.Choice(name="Every week", value="weekly"),
]
AFK_WEEKDAY_CHOICES = [
    app_commands.Choice(name=label, value=label.casefold())
    for label in ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
]


class AfkCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _service(self):
        return getattr(self.bot, "utility_service", None)

    def _profile_service(self):
        return getattr(self.bot, "profile_service", None)

    async def _send_storage_unavailable(self, ctx: commands.Context):
        service = self._service()
        message = "AFK is temporarily unavailable because Babblebox could not reach its utility database."
        if service is not None:
            message = service.storage_message("AFK")
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "AFK Unavailable",
                message,
                tone="warning",
                footer="Babblebox AFK",
            ),
            ephemeral=True,
        )

    async def _require_storage(self, ctx: commands.Context) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        service = self._service()
        if service is not None and getattr(service, "storage_ready", False):
            return True
        await self._send_storage_unavailable(ctx)
        return False

    async def _send_private_embed(self, ctx: commands.Context, *, embed: discord.Embed):
        await send_hybrid_response(ctx, embed=embed, ephemeral=True)

    def _parse_afk_duration(self, raw: Optional[str], *, field_name: str, max_minutes: int) -> tuple[bool, int | str | None]:
        if raw is None:
            return True, None
        service = self._service()
        seconds = service.parse_relative_duration(raw) if service is not None else None
        if seconds is None:
            return False, f"Use `{field_name}` like `30m`, `2h`, or `2d`."
        max_seconds = max_minutes * 60
        if not (60 <= seconds <= max_seconds):
            return False, f"`{field_name}` must be between 1 minute and {max_minutes // (24 * 60)} days."
        return True, seconds

    def _resolve_afk_payload(self, reason: Optional[str], preset: Optional[str]) -> tuple[str | None, str | None]:
        chosen_preset = preset.strip().casefold() if isinstance(preset, str) and preset.strip() else None
        custom_reason = reason
        if chosen_preset is None and isinstance(reason, str):
            lowered = reason.strip().casefold()
            if lowered in AFK_QUICK_REASONS:
                chosen_preset = lowered
                custom_reason = None
        return chosen_preset, build_afk_reason_text(preset=chosen_preset, custom_reason=custom_reason)

    def _apply_default_preset_duration(self, preset: str | None, duration_seconds: int | None) -> int | None:
        if duration_seconds is not None:
            return duration_seconds
        return get_afk_preset_default_duration(preset)

    def _local_now_text(self, timezone_name: str) -> str:
        tzinfo = load_afk_timezone(timezone_name)
        if tzinfo is None:
            return "Unknown"
        local_now = datetime.now(tzinfo)
        return local_now.strftime("%Y-%m-%d %H:%M")

    def _schedule_overview_embed(self, user: discord.abc.User, *, timezone_name: str | None, schedules: list[dict]) -> discord.Embed:
        service = self._service()
        embed = discord.Embed(
            title="AFK Schedules",
            description=f"**{ge.display_name_of(user)}**",
            color=ge.EMBED_THEME["info"],
            timestamp=ge.now_utc(),
        )
        timezone_text = "Not set"
        if timezone_name:
            timezone_text = f"{format_afk_timezone_label(timezone_name)}\nLocal now: `{self._local_now_text(timezone_name)}`"
        embed.add_field(name="Default Timezone", value=timezone_text, inline=False)
        if schedules:
            lines = [self._service().build_afk_schedule_summary_line(schedule) for schedule in schedules[:6]]
            embed.add_field(name="Saved Schedules", value=ge.join_limited_lines(lines), inline=False)
        else:
            embed.add_field(name="Saved Schedules", value="No recurring AFK schedules yet. Use `/afkschedule add` after setting `/afktimezone set`.", inline=False)
        if service is not None:
            summary = service.get_afk_schedule_summary(user.id)
            note = preserved_over_limit_note(
                saved_count=int(summary.get("saved", 0)),
                active_count=int(summary.get("active", 0)),
            )
            if note:
                embed.add_field(
                    name="Saved Above Current Plan",
                    value="\n".join(
                        [
                            f"Recurring AFK schedules: {format_saved_state_status(saved_count=int(summary.get('saved', 0)), active_count=int(summary.get('active', 0)), limit_value=service.afk_schedule_limit(user.id))}",
                            note,
                        ]
                    ),
                    inline=False,
                )
        return ge.style_embed(embed, footer="Babblebox AFK | Remove one with /afkschedule remove <id> or clear all with /afkschedule clear.")

    def _overview_embed(self, user: discord.abc.User, *, timezone_name: str | None, next_schedule: dict | None) -> discord.Embed:
        embed = ge.make_status_embed(
            "AFK Overview",
            "You do not have an active or one-shot AFK status right now.",
            tone="info",
            footer="Babblebox AFK",
        )
        if timezone_name:
            embed.add_field(
                name="Default Timezone",
                value=f"{format_afk_timezone_label(timezone_name)}\nLocal now: `{self._local_now_text(timezone_name)}`",
                inline=False,
            )
        if next_schedule is not None:
            embed.add_field(
                name="Next Recurring Schedule",
                value=self._service().build_afk_schedule_summary_line(next_schedule),
                inline=False,
            )
        return embed

    @commands.hybrid_command(name="afk", with_app_command=True, description="Set, schedule, or clear your AFK status safely")
    @app_commands.describe(
        reason="Short safe AFK reason (1-3 sentences, no links)",
        duration="Optional auto-clear timer like 30m, 2h, or 2d",
        start_in="Optional delayed start like 30m, 2h, or 2d",
        start_at="Optional local clock time in your saved AFK timezone like 23:00 or tomorrow 08:30",
        preset="Optional quick AFK preset with emoji and a sensible default duration",
    )
    @app_commands.choices(preset=AFK_PRESET_CHOICES)
    async def afk_command(
        self,
        ctx: commands.Context,
        reason: Optional[str] = None,
        duration: Optional[str] = None,
        start_in: Optional[str] = None,
        start_at: Optional[str] = None,
        preset: Optional[str] = None,
    ):
        if not await self._require_storage(ctx):
            return
        service = self._service()

        if duration is None and start_in is None and start_at is None and reason is not None and preset is None:
            inferred_seconds = service.parse_relative_duration(reason)
            if inferred_seconds is not None:
                duration = reason
                reason = None

        if start_in is not None and start_at is not None:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "Invalid Schedule",
                    "Use either `start_in` or `start_at`, not both.",
                    tone="warning",
                    footer="Babblebox AFK",
                ),
            )
            return

        chosen_preset, resolved_reason = self._resolve_afk_payload(reason, preset)
        if resolved_reason and len(resolved_reason) > ge.AFK_REASON_MAX_LEN:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Reason Too Long",
                    f"AFK reason must be {ge.AFK_REASON_MAX_LEN} characters or fewer.",
                    tone="warning",
                    footer="Babblebox AFK",
                ),
            )
            return

        ok, duration_seconds_or_error = self._parse_afk_duration(duration, field_name="duration", max_minutes=ge.AFK_MAX_DURATION_MINUTES)
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Invalid Duration", str(duration_seconds_or_error), tone="warning", footer="Babblebox AFK"),
            )
            return

        ok, start_in_seconds_or_error = self._parse_afk_duration(start_in, field_name="start_in", max_minutes=ge.AFK_MAX_SCHEDULE_MINUTES)
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Invalid Schedule", str(start_in_seconds_or_error), tone="warning", footer="Babblebox AFK"),
            )
            return

        timezone_name = service.get_afk_timezone(ctx.author.id)
        ok, start_at_or_error = parse_afk_start_at(start_at, timezone_name=timezone_name)
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Invalid Schedule", str(start_at_or_error), tone="warning", footer="Babblebox AFK"),
            )
            return

        user_id = ctx.author.id
        existing = service.get_afk_record(user_id)
        duration_seconds = duration_seconds_or_error if isinstance(duration_seconds_or_error, int) else None
        duration_seconds = self._apply_default_preset_duration(chosen_preset, duration_seconds)
        start_in_seconds = start_in_seconds_or_error if isinstance(start_in_seconds_or_error, int) else None
        start_at_dt = start_at_or_error if isinstance(start_at_or_error, datetime) else None
        if start_at_dt is not None:
            max_schedule_seconds = ge.AFK_MAX_SCHEDULE_MINUTES * 60
            start_in_seconds = max(60, int((start_at_dt - ge.now_utc()).total_seconds()))
            if start_in_seconds > max_schedule_seconds:
                await self._send_private_embed(
                    ctx,
                    embed=ge.make_status_embed(
                        "Invalid Schedule",
                        f"`start_at` must be within {ge.AFK_MAX_SCHEDULE_MINUTES // (24 * 60)} days.",
                        tone="warning",
                        footer="Babblebox AFK",
                    ),
                )
                return
        has_custom_payload = any(value is not None for value in (resolved_reason, duration_seconds, start_in_seconds, chosen_preset, start_at_dt))

        if existing and not has_custom_payload:
            await service.clear_afk(user_id)
            message = "Your scheduled AFK has been cancelled." if existing.get("status") == "scheduled" else "Welcome back! I removed your AFK status."
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("AFK Updated", message, tone="success", footer="Babblebox AFK"),
            )
            return

        ok, result = await service.set_afk(
            user=ctx.author,
            reason=resolved_reason,
            duration_seconds=duration_seconds,
            start_in_seconds=start_in_seconds,
            start_at=start_at_dt,
            preset=chosen_preset,
        )
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("AFK Rejected", result, tone="warning", footer="Babblebox AFK"),
            )
            return

        await self._send_private_embed(
            ctx,
            embed=service.build_afk_status_embed_for(
                ctx.author,
                result,
                title="AFK Scheduled" if start_in_seconds is not None or start_at_dt is not None else "AFK Enabled",
            ),
        )
        profile_service = self._profile_service()
        if profile_service is not None and getattr(profile_service, "storage_ready", False):
            await profile_service.record_utility_action(ctx.author.id, "afk")

    @commands.hybrid_command(name="afkstatus", with_app_command=True, description="View your AFK state, timezone, and next recurring schedule")
    async def afkstatus_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        record = service.get_afk_record(ctx.author.id)
        timezone_name = service.get_afk_timezone(ctx.author.id)
        next_schedule = service.get_next_afk_schedule(ctx.author.id)
        if record is None:
            await self._send_private_embed(
                ctx,
                embed=self._overview_embed(ctx.author, timezone_name=timezone_name, next_schedule=next_schedule),
            )
            return

        embed = service.build_afk_status_embed_for(ctx.author, record)
        if timezone_name:
            embed.add_field(
                name="Default Timezone",
                value=f"{format_afk_timezone_label(timezone_name)}\nLocal now: `{self._local_now_text(timezone_name)}`",
                inline=False,
            )
        if next_schedule is not None:
            embed.add_field(
                name="Next Recurring Schedule",
                value=service.build_afk_schedule_summary_line(next_schedule),
                inline=False,
            )
        await self._send_private_embed(ctx, embed=embed)

    @commands.hybrid_group(
        name="afktimezone",
        with_app_command=True,
        description="Set or view the timezone Babblebox uses for AFK clock times",
        invoke_without_command=True,
    )
    async def afktimezone_group(self, ctx: commands.Context):
        await self.afktimezone_view_command(ctx)

    @afktimezone_group.command(name="set", with_app_command=True, description="Save your AFK timezone")
    @app_commands.describe(timezone="IANA timezone like Asia/Yerevan or America/New_York, or a fixed offset like UTC+04:00")
    async def afktimezone_set_command(self, ctx: commands.Context, timezone: str):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        ok, result = await service.set_afk_timezone(ctx.author.id, timezone)
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("AFK Timezone Rejected", result, tone="warning", footer="Babblebox AFK"),
            )
            return
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                "AFK Timezone Saved",
                f"Using **{format_afk_timezone_label(result)}** for `/afk start_at` and new recurring AFK schedules.\nLocal now: `{self._local_now_text(result)}`",
                tone="success",
                footer="Babblebox AFK",
            ),
        )

    @afktimezone_group.command(name="view", with_app_command=True, description="View your AFK timezone")
    async def afktimezone_view_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        timezone_name = service.get_afk_timezone(ctx.author.id)
        if timezone_name is None:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Timezone Not Set",
                    "Set one with `/afktimezone set Asia/Yerevan` or `/afktimezone set America/New_York` so `start_at` and recurring AFK schedules can use your local time.",
                    tone="info",
                    footer="Babblebox AFK",
                ),
            )
            return
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                "AFK Timezone",
                f"Current timezone: **{format_afk_timezone_label(timezone_name)}**\nLocal now: `{self._local_now_text(timezone_name)}`",
                tone="info",
                footer="Babblebox AFK",
            ),
        )

    @afktimezone_group.command(name="clear", with_app_command=True, description="Clear your AFK timezone")
    async def afktimezone_clear_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        ok, result = await service.clear_afk_timezone(ctx.author.id)
        tone = "success" if ok else "warning"
        extra = ""
        if ok and service.list_afk_schedules(ctx.author.id):
            extra = "\nExisting recurring AFK schedules keep the timezone they were created with."
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("AFK Timezone Updated", f"{result}{extra}", tone=tone, footer="Babblebox AFK"),
        )

    @commands.hybrid_group(
        name="afkschedule",
        with_app_command=True,
        description="Create and manage recurring AFK schedules",
        invoke_without_command=True,
    )
    async def afkschedule_group(self, ctx: commands.Context):
        await self.afkschedule_list_command(ctx)

    @afkschedule_group.command(name="add", with_app_command=True, description="Create a recurring AFK schedule")
    @app_commands.describe(
        repeat="How often Babblebox should repeat this AFK schedule",
        at="Local time in your saved AFK timezone, like 23:30 or 11:30pm",
        day="Weekday for weekly schedules",
        duration="Optional override like 30m, 2h, or 8h",
        preset="Optional AFK preset with emoji and default duration",
        reason="Optional short AFK note",
    )
    @app_commands.choices(repeat=AFK_REPEAT_CHOICES, day=AFK_WEEKDAY_CHOICES, preset=AFK_PRESET_CHOICES)
    async def afkschedule_add_command(
        self,
        ctx: commands.Context,
        repeat: str,
        at: str,
        day: Optional[str] = None,
        duration: Optional[str] = None,
        preset: Optional[str] = None,
        *,
        reason: Optional[str] = None,
    ):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        timezone_name = service.get_afk_timezone(ctx.author.id)
        if timezone_name is None:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Timezone Needed",
                    "Set your AFK timezone first with `/afktimezone set` so recurring schedules can use your local clock time.",
                    tone="warning",
                    footer="Babblebox AFK",
                ),
            )
            return

        ok, clock_or_error = parse_afk_clock_input(at)
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Invalid Time", str(clock_or_error), tone="warning", footer="Babblebox AFK"),
            )
            return

        weekday = None
        if repeat == "weekly":
            ok, weekday_or_error = parse_afk_weekday(day)
            if not ok:
                await self._send_private_embed(
                    ctx,
                    embed=ge.make_status_embed("Weekday Required", str(weekday_or_error), tone="warning", footer="Babblebox AFK"),
                )
                return
            weekday = weekday_or_error if isinstance(weekday_or_error, int) else None

        chosen_preset, resolved_reason = self._resolve_afk_payload(reason, preset)
        if resolved_reason and len(resolved_reason) > ge.AFK_REASON_MAX_LEN:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Reason Too Long",
                    f"AFK reason must be {ge.AFK_REASON_MAX_LEN} characters or fewer.",
                    tone="warning",
                    footer="Babblebox AFK",
                ),
            )
            return

        ok, duration_seconds_or_error = self._parse_afk_duration(duration, field_name="duration", max_minutes=ge.AFK_MAX_DURATION_MINUTES)
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Invalid Duration", str(duration_seconds_or_error), tone="warning", footer="Babblebox AFK"),
            )
            return

        duration_seconds = duration_seconds_or_error if isinstance(duration_seconds_or_error, int) else None
        duration_seconds = self._apply_default_preset_duration(chosen_preset, duration_seconds)
        clock = clock_or_error if isinstance(clock_or_error, tuple) else (0, 0)
        ok, result = await service.create_afk_schedule(
            user=ctx.author,
            repeat=repeat,
            timezone_name=timezone_name,
            local_hour=clock[0],
            local_minute=clock[1],
            weekday=weekday,
            reason=resolved_reason,
            preset=chosen_preset,
            duration_seconds=duration_seconds,
        )
        if not ok:
            await self._send_private_embed(
                ctx,
                embed=ge.make_status_embed("Recurring AFK Rejected", result, tone="warning", footer="Babblebox AFK"),
            )
            return

        schedule = result
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed(
                "Recurring AFK Saved",
                (
                    f"ID: `{schedule['id'][:8]}`\n"
                    f"{service.build_afk_schedule_summary_line(schedule)}"
                ),
                tone="success",
                footer="Babblebox AFK",
            ),
        )

    @afkschedule_group.command(name="list", with_app_command=True, description="List your recurring AFK schedules")
    async def afkschedule_list_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        schedules = service.list_afk_schedules(ctx.author.id)
        timezone_name = service.get_afk_timezone(ctx.author.id)
        await self._send_private_embed(
            ctx,
            embed=self._schedule_overview_embed(ctx.author, timezone_name=timezone_name, schedules=schedules),
        )

    @afkschedule_group.command(name="remove", with_app_command=True, description="Remove one recurring AFK schedule by ID")
    @app_commands.describe(schedule_id="The 8-character schedule ID shown in /afkschedule list")
    async def afkschedule_remove_command(self, ctx: commands.Context, schedule_id: str):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        ok, result = await service.remove_afk_schedule(ctx.author.id, schedule_id)
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Recurring AFK Updated", result, tone="success" if ok else "warning", footer="Babblebox AFK"),
        )

    @afkschedule_group.command(name="clear", with_app_command=True, description="Clear all of your recurring AFK schedules")
    async def afkschedule_clear_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        service = self._service()
        ok, result = await service.clear_all_afk_schedules(ctx.author.id)
        await self._send_private_embed(
            ctx,
            embed=ge.make_status_embed("Recurring AFK Updated", result, tone="success" if ok else "warning", footer="Babblebox AFK"),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AfkCog(bot))
