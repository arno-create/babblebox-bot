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
from babblebox.premium_models import PLAN_FREE, PLAN_SUPPORTER
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
        description = "Vote on Top.gg to unlock a temporary boost for a few personal utility limits."

        if state == "misconfigured":
            tone = "warning"
            description = str(snapshot.get("configuration_message") or "Top.gg vote bonuses are not configured safely on this deployment.")
        elif state == "disabled":
            tone = "info"
            description = str(snapshot.get("configuration_message") or "Top.gg vote bonuses are disabled on this deployment.")
        elif not eligible:
            tone = "info"
            description = "Your current paid lane already stays above the temporary vote bonus. Votes still help Babblebox on Top.gg, but they do not raise your limits."
        elif active and expires_at:
            tone = "success"
            description = f"Your Vote Bonus is active until **{expires_at}**."

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
        if bool(snapshot.get("api_refresh_available", False)):
            embed.add_field(name="Refresh", value="Live Top.gg refresh is available.", inline=True)
        else:
            embed.add_field(name="Refresh", value="Live refresh stays off until `TOPGG_TOKEN` is configured.", inline=True)

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
                value="The Vote Bonus is temporary and utility-only. Babblebox Plus stays higher and does not expire every vote window.",
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


async def setup(bot: commands.Bot):
    await bot.add_cog(VoteCog(bot))
