from __future__ import annotations

import contextlib

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import send_hybrid_response
from babblebox.premium_limits import (
    LIMIT_AFK_SCHEDULES,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_WATCH_FILTERS,
    LIMIT_WATCH_KEYWORDS,
    user_limit as premium_user_limit,
)
from babblebox.premium_models import PLAN_FREE, PLAN_SUPPORTER, SYSTEM_PREMIUM_OWNER_USER_IDS
from babblebox.vote_service import TOPGG_VOTE_LIMITS, VoteService


BOOST_ROWS = (
    ("Watch keywords", LIMIT_WATCH_KEYWORDS),
    ("Watch filters", LIMIT_WATCH_FILTERS),
    ("Active reminders", LIMIT_REMINDERS_ACTIVE),
    ("Active public reminders", LIMIT_REMINDERS_PUBLIC_ACTIVE),
    ("Recurring AFK schedules", LIMIT_AFK_SCHEDULES),
)


class VotePanelView(discord.ui.View):
    def __init__(self, cog: "VoteCog", *, author_id: int):
        super().__init__(timeout=900)
        self.cog = cog
        self.author_id = int(author_id)
        self.add_item(discord.ui.Button(label="Vote on Top.gg", style=discord.ButtonStyle.link, url=self.cog.service.vote_url(), row=0))

    def _sync_labels(self, snapshot: dict[str, object]):
        self.reminder_button.label = "Vote Reminders Off" if snapshot.get("reminder_opt_in") else "Vote Reminders On"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "This Panel Is Locked",
                "Use `/vote` to open your own Top.gg vote panel.",
                tone="info",
                footer="Babblebox Vote Bonus",
            ),
            ephemeral=True,
        )
        return False

    async def _update_panel(self, interaction: discord.Interaction, *, note: str | None = None, ok: bool = True):
        snapshot = self.cog.service.status_snapshot(self.author_id)
        self._sync_labels(snapshot)
        embed = self.cog.build_vote_embed(snapshot, note=note, ok=ok)
        edited = await ge.safe_edit_interaction_message(interaction, embed=embed, view=self)
        if edited:
            return
        await ge.safe_send_interaction(interaction, embed=embed, view=self, ephemeral=True)

    async def on_timeout(self):
        for child in self.children:
            if getattr(child, "style", None) != discord.ButtonStyle.link:
                child.disabled = True
        message = getattr(self, "message", None)
        if message is not None:
            with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                await message.edit(view=self)

    @discord.ui.button(label="Refresh Status", style=discord.ButtonStyle.primary, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, message = await self.cog.service.refresh_user_vote_status(interaction.user.id)
        await self._update_panel(interaction, note=message, ok=ok)

    @discord.ui.button(label="Vote Reminders On", style=discord.ButtonStyle.secondary, row=1)
    async def reminder_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        snapshot = self.cog.service.status_snapshot(interaction.user.id)
        enabled = not bool(snapshot.get("reminder_opt_in", False))
        await self.cog.service.set_reminder_preference(interaction.user.id, enabled=enabled)
        message = "Vote reminders are now on." if enabled else "Vote reminders are now off."
        await self._update_panel(interaction, note=message, ok=True)


class VoteCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = VoteService(bot)

    async def cog_load(self):
        setattr(self.bot, "vote_service", self.service)
        await self.service.start()

    def cog_unload(self):
        if getattr(self.bot, "vote_service", None) is self.service:
            delattr(self.bot, "vote_service")
        self.bot.loop.create_task(self.service.close())

    def build_vote_embed(self, snapshot: dict[str, object], *, note: str | None = None, ok: bool = True) -> discord.Embed:
        state = str(snapshot.get("configuration_state") or "disabled")
        plan_code = str(snapshot.get("plan_code") or PLAN_FREE)
        eligible = bool(snapshot.get("eligible", False))
        active = bool(snapshot.get("active", False))
        expires_at = snapshot.get("expires_at")
        tone = "accent"
        description = "Vote on Top.gg to unlock a temporary utility boost for a few personal limits."

        if state == "misconfigured":
            tone = "warning"
            description = str(snapshot.get("configuration_message") or "Top.gg vote bonuses are not configured safely on this deployment.")
        elif state == "disabled":
            tone = "info"
            description = str(snapshot.get("configuration_message") or "Top.gg vote bonuses are disabled on this deployment.")
        elif not eligible:
            tone = "info"
            description = "Your current paid lane already stays above this temporary utility-only vote lane. Votes still help Babblebox on Top.gg, but they do not raise your limits here."
        elif active and expires_at:
            tone = "success"
            description = f"Your Vote Bonus is active until **{expires_at}**."
            if snapshot.get("timing_source") == "legacy_estimated":
                description += " This legacy Top.gg window is estimated from the standard 12-hour vote cadence."
        else:
            description = "You are eligible for the temporary Vote Bonus, but it is not active right now. Vote on Top.gg to start a new utility window."

        embed = ge.make_status_embed(
            "Vote Bonus",
            description,
            tone=tone,
            footer="Babblebox Vote Bonus",
        )
        if note:
            embed.add_field(name="Latest Update", value=str(note), inline=False)
        embed.add_field(name="Current Plan", value=str(snapshot.get("plan_label") or "Free"), inline=True)
        embed.add_field(
            name="Reminders",
            value="On" if snapshot.get("reminder_opt_in") else "Off",
            inline=True,
        )
        if state == "disabled":
            embed.add_field(
                name="Refresh",
                value="Live refresh stays off until Top.gg vote bonuses are explicitly enabled on this deployment.",
                inline=True,
            )
        elif state != "configured":
            embed.add_field(
                name="Refresh",
                value="Live refresh stays off until this Top.gg vote lane is configured safely.",
                inline=True,
            )
        elif bool(snapshot.get("api_refresh_available", False)):
            embed.add_field(name="Refresh", value="Live Top.gg refresh is available.", inline=True)
        else:
            embed.add_field(name="Refresh", value="Live refresh stays off until `TOPGG_TOKEN` is configured.", inline=True)
        if snapshot.get("timing_note"):
            embed.add_field(name="Legacy Timing", value=str(snapshot.get("timing_note")), inline=False)

        if eligible:
            base_plan = PLAN_SUPPORTER if plan_code == PLAN_SUPPORTER else PLAN_FREE
            lines = []
            for label, limit_key in BOOST_ROWS:
                base_limit = premium_user_limit(base_plan, limit_key)
                boosted_limit = TOPGG_VOTE_LIMITS[limit_key]
                lines.append(f"{label}: `{base_limit} -> {boosted_limit}`")
            embed.add_field(name="Temporary Vote Boost", value="\n".join(lines), inline=False)
            embed.add_field(
                name="How It Differs From Plus",
                value="The Vote Bonus is temporary, utility-only, and not a premium tier. Babblebox Plus stays higher and does not expire every vote window.",
                inline=False,
            )
        else:
            embed.add_field(
                name="Vote Recognition",
                value="Top.gg votes still help visibility, but Babblebox Plus and Guild Pro already stay above this temporary user-only vote lane.",
                inline=False,
            )
        return embed

    @app_commands.command(name="vote", description="View your Top.gg vote status, bonus, and reminder controls")
    async def vote_command(self, interaction: discord.Interaction):
        snapshot = self.service.status_snapshot(interaction.user.id)
        view = VotePanelView(self, author_id=interaction.user.id)
        view._sync_labels(snapshot)
        embed = self.build_vote_embed(snapshot)
        result = await ge.safe_send_interaction(interaction, embed=embed, view=view, ephemeral=True)
        message = getattr(result, "resource", None) if result is not None else None
        if message is None:
            with contextlib.suppress(discord.ClientException, discord.HTTPException, discord.NotFound):
                message = await interaction.original_response()
        if message is not None:
            view.message = message

    @commands.command(name="topggvote", hidden=True)
    async def topggvote_prefix_command(self, ctx: commands.Context):
        snapshot = self.service.status_snapshot(ctx.author.id)
        await send_hybrid_response(ctx, embed=self.build_vote_embed(snapshot))

    def _is_vote_owner(self, user_id: int) -> bool:
        return int(user_id or 0) in SYSTEM_PREMIUM_OWNER_USER_IDS

    def _build_topgg_owner_status_embed(self, *, user_id: int | None = None, note: str | None = None) -> discord.Embed:
        diagnostics = self.service.diagnostics_snapshot()
        description = "Private maintainer status for this deployment's Top.gg vote lane."
        embed = ge.make_status_embed(
            "Top.gg Vote Owner Status",
            description if user_id is None else "Private maintainer status for one member's Top.gg vote lane state.",
            tone="info",
            footer="Babblebox Vote Bonus",
        )
        config_lines = [
            f"Enabled: **{'yes' if diagnostics.get('enabled') else 'no'}**",
            f"State: **{diagnostics.get('configuration_state') or 'unknown'}**",
            f"Mode: **{diagnostics.get('webhook_mode') or 'none'}**",
            f"Storage: **{'ready' if diagnostics.get('storage_ready') else 'unavailable'}** ({diagnostics.get('storage_backend') or 'unknown'})",
            f"Public route ready: **{'yes' if diagnostics.get('public_routes_ready') else 'no'}**",
            f"Refresh API: **{'on' if diagnostics.get('api_refresh_available') else 'off'}**",
        ]
        embed.add_field(name="Configuration", value="\n".join(config_lines), inline=False)
        if user_id is None:
            webhook = diagnostics.get("webhook_summary") if isinstance(diagnostics.get("webhook_summary"), dict) else {}
            webhook_lines = [
                f"Status: **{webhook.get('status') or 'unknown'}**",
                f"Replay: {webhook.get('replay_protection') or 'unknown'}",
                f"Timing: **{webhook.get('timing_source') or 'unknown'}**",
                f"Last event: {webhook.get('last_event_at') or 'never'}",
            ]
            embed.add_field(name="Webhook", value="\n".join(webhook_lines), inline=False)
        else:
            snapshot = self.service.status_snapshot(int(user_id))
            record = self.service.get_vote_record(int(user_id)) or {}
            user_lines = [
                f"User: `{int(user_id)}`",
                f"Plan: **{snapshot.get('plan_label') or 'Unknown'}**",
                f"Eligible: **{'yes' if snapshot.get('eligible') else 'no'}**",
                f"Active: **{'yes' if snapshot.get('active') else 'no'}**",
                f"Expires: {snapshot.get('expires_at') or 'none'}",
                f"Timing: **{snapshot.get('timing_source') or 'unknown'}**",
                f"Reminder: **{'on' if snapshot.get('reminder_opt_in') else 'off'}**",
                f"Vote ID: {record.get('topgg_vote_id') or 'none'}",
            ]
            embed.add_field(name="User", value="\n".join(user_lines), inline=False)
            receipt_lines = [
                f"Webhook status: **{record.get('webhook_status') or 'none'}**",
                f"Created: {record.get('created_at') or 'none'}",
                f"Trace: {record.get('webhook_trace_id') or 'none'}",
            ]
            embed.add_field(name="Stored Record", value="\n".join(receipt_lines), inline=False)
        if note:
            embed.add_field(name="Note", value=note, inline=False)
        return embed

    @commands.command(name="topggvoteadmin", hidden=True)
    async def topggvote_admin_command(self, ctx: commands.Context, *parts: str):
        if ctx.guild is not None:
            await ctx.send(content="That command is only available in DM.")
            return
        author_id = getattr(ctx.author, "id", 0)
        if not self._is_vote_owner(author_id):
            await ctx.send(content="That command is unavailable.")
            return
        tokens = [str(part).strip() for part in parts if str(part).strip()]
        if not tokens:
            tokens = ["status"]
        if tokens[0].casefold() != "status":
            await ctx.send(
                embed=self._build_topgg_owner_status_embed(
                    note="Use `status` or `status user <discord_user_id>`.",
                )
            )
            return
        if len(tokens) == 1:
            await ctx.send(embed=self._build_topgg_owner_status_embed())
            return
        if len(tokens) == 3 and tokens[1].casefold() == "user":
            try:
                user_id = int(tokens[2])
            except ValueError:
                await ctx.send(embed=self._build_topgg_owner_status_embed(note="Discord user IDs must be numeric."))
                return
            await ctx.send(embed=self._build_topgg_owner_status_embed(user_id=user_id))
            return
        await ctx.send(
            embed=self._build_topgg_owner_status_embed(
                note="Use `status` or `status user <discord_user_id>`.",
            )
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(VoteCog(bot))
