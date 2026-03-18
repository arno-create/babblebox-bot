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

    @commands.hybrid_command(name="afk", with_app_command=True, description="Set, schedule, or clear your AFK status safely")
    @app_commands.describe(
        reason="Short safe AFK reason (1-3 sentences, no links)",
        duration_minutes="Optional auto-clear timer in minutes",
        start_in_minutes="Optional delayed start in minutes",
    )
    async def afk_command(
        self,
        ctx: commands.Context,
        reason: Optional[str] = None,
        duration_minutes: Optional[int] = None,
        start_in_minutes: Optional[int] = None,
    ):
        await defer_hybrid_response(ctx, ephemeral=True)
        service = self._service()
        if service is None or not service.storage_ready:
            await self._send_storage_unavailable(ctx)
            return

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

        if duration_minutes is not None and not (1 <= duration_minutes <= ge.AFK_MAX_DURATION_MINUTES):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Invalid Duration",
                    f"`duration_minutes` must be between 1 and {ge.AFK_MAX_DURATION_MINUTES}.",
                    tone="warning",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        if start_in_minutes is not None and not (1 <= start_in_minutes <= ge.AFK_MAX_SCHEDULE_MINUTES):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Invalid Schedule",
                    f"`start_in_minutes` must be between 1 and {ge.AFK_MAX_SCHEDULE_MINUTES}.",
                    tone="warning",
                    footer="Babblebox AFK",
                ),
                ephemeral=True,
            )
            return

        user_id = ctx.author.id
        existing = service.get_afk_record(user_id)
        has_custom_payload = any(value is not None for value in (reason, duration_minutes, start_in_minutes))

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
            duration_minutes=duration_minutes,
            start_in_minutes=start_in_minutes,
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
                title="AFK Scheduled" if start_in_minutes is not None else "AFK Enabled",
            ),
            ephemeral=True,
        )

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
