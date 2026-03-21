from __future__ import annotations

from typing import Optional

from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response


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

    @commands.hybrid_command(name="afk", with_app_command=True, description="Set, schedule, or clear your AFK status safely")
    @app_commands.describe(
        reason="Short safe AFK reason (1-3 sentences, no links)",
        duration="Optional auto-clear timer like 30m, 2h, or 2d",
        start_in="Optional delayed start like 30m, 2h, or 2d",
    )
    async def afk_command(
        self,
        ctx: commands.Context,
        reason: Optional[str] = None,
        duration: Optional[str] = None,
        start_in: Optional[str] = None,
    ):
        await defer_hybrid_response(ctx, ephemeral=True)
        service = self._service()
        if service is None or not service.storage_ready:
            await self._send_storage_unavailable(ctx)
            return

        if duration is None and start_in is None and reason is not None:
            inferred_seconds = service.parse_relative_duration(reason)
            if inferred_seconds is not None:
                duration = reason
                reason = None

        if reason and len(reason) > ge.AFK_REASON_MAX_LEN:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Reason Too Long",
                    f"AFK reason must be {ge.AFK_REASON_MAX_LEN} characters or fewer.",
                    tone="warning",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        ok, duration_seconds_or_error = self._parse_afk_duration(duration, field_name="duration", max_minutes=ge.AFK_MAX_DURATION_MINUTES)
        if not ok:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Invalid Duration",
                    str(duration_seconds_or_error),
                    tone="warning",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        ok, start_in_seconds_or_error = self._parse_afk_duration(start_in, field_name="start_in", max_minutes=ge.AFK_MAX_SCHEDULE_MINUTES)
        if not ok:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Invalid Schedule",
                    str(start_in_seconds_or_error),
                    tone="warning",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        user_id = ctx.author.id
        existing = service.get_afk_record(user_id)
        duration_seconds = duration_seconds_or_error if isinstance(duration_seconds_or_error, int) else None
        start_in_seconds = start_in_seconds_or_error if isinstance(start_in_seconds_or_error, int) else None
        has_custom_payload = any(value is not None for value in (reason, duration_seconds, start_in_seconds))

        if existing and not has_custom_payload:
            await service.clear_afk(user_id)
            message = "Your scheduled AFK has been cancelled." if existing.get("status") == "scheduled" else "Welcome back! I removed your AFK status."
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Updated",
                    message,
                    tone="success",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        ok, result = await service.set_afk(
            user=ctx.author,
            reason=reason,
            duration_seconds=duration_seconds,
            start_in_seconds=start_in_seconds,
        )
        if not ok:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Rejected",
                    result,
                    tone="warning",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        await send_hybrid_response(
            ctx,
            embed=service.build_afk_status_embed_for(
                ctx.author,
                result,
                title="AFK Scheduled" if start_in_seconds is not None else "AFK Enabled",
            ),
            ephemeral=True,
        )
        profile_service = self._profile_service()
        if profile_service is not None and getattr(profile_service, "storage_ready", False):
            await profile_service.record_utility_action(ctx.author.id, "afk")

    @commands.hybrid_command(name="afkstatus", with_app_command=True, description="View your current AFK or scheduled AFK status")
    async def afkstatus_command(self, ctx: commands.Context):
        await defer_hybrid_response(ctx, ephemeral=True)
        service = self._service()
        if service is None or not service.storage_ready:
            await self._send_storage_unavailable(ctx)
            return

        record = service.get_afk_record(ctx.author.id)
        if record is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "AFK Not Set",
                    "You do not currently have an AFK status or schedule.",
                    tone="info",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        await send_hybrid_response(
            ctx,
            embed=service.build_afk_status_embed_for(ctx.author, record),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AfkCog(bot))
