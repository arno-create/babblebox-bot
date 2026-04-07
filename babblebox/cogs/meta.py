from __future__ import annotations

import contextlib
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import require_channel_permissions, send_hybrid_response


LEADERBOARD_LABELS = {
    "wins": "Wins",
    "bomb_wins": "Bomb Wins",
    "bomb_words": "Bomb Words",
    "spy_wins": "Spy Wins",
}
VISIBILITY_CHOICES = [
    app_commands.Choice(name="Public", value="public"),
    app_commands.Choice(name="Only me", value="private"),
]
HELP_PAGES: list[dict[str, str]] = [
    {
        "title": "Babblebox Guide",
        "emoji": "\U0001f3e0",
        "description": "Six clear lanes: Party Games, Question Drops, Daily Arcade, Utilities, Buddy/Profile, and Shield.",
        "body": (
            "Use the arrows to browse the product by lane.\n"
            "Showable cards lean public by default so they feel native in chat.\n"
            "Personal utilities, setup, and sensitive admin work stay private."
        ),
        "try": "`/play`, `/drops status`, `/daily`, `/profile`, `/watch`",
    },
    {
        "title": "Party Games",
        "emoji": "\U0001f389",
        "description": "Short multiplayer rooms for live server energy.",
        "body": (
            "`/play` opens the lobby for Broken Telephone, Exquisite Corpse, Spyfall, Word Bomb, and Pattern Hunt.\n"
            "Broken Telephone runs as a DM relay: original clip, panic mimics, then one final typed guess.\n"
            "Exquisite Corpse keeps every prompt hidden until the room gets one stitched-together masterpiece.\n"
            "Spyfall gives everyone a secret DM role, rotates the spotlight through the room, and lets any player call the vote.\n"
            "Word Bomb stays fast: one real word, live syllable, no repeats, and the fuse keeps shrinking.\n"
            "Pattern Hunt: one public clue loop, one hidden rule, and private guesses with `/hunt guess`. Coders need server DMs open before start. `Contains Digits` means digits `0-9` only.\n"
            "`/hunt status` mirrors the live state card privately, and `/hunt guess` stays quiet until a Pattern Hunt room is live."
        ),
        "try": "`/play`, `/hunt status`",
    },
    {
        "title": "Question Drops",
        "emoji": "\U0001f4da",
        "description": "Guild knowledge drops with visible mastery, scholar ranks, and low-noise progression.",
        "body": (
            "`/drops status` shows the guild knowledge lane clearly.\n"
            "`/drops stats` and `/drops leaderboard` stay guild-first, while Buddy and Profile surfaces fold the knowledge lane into identity cleanly.\n"
            "`/drops roles status`, `/drops roles remove`, and `/drops roles preference` give members a private way to remove current Babblebox roles or stop future grants without touching achievement history.\n"
            "`/dropsadmin mastery category ...` and `/dropsadmin mastery scholar ...` configure category mastery roles, the guild scholar ladder, and custom mastery announcement copy in one place.\n"
            "`/dropsadmin config` also controls the difficulty profile: Standard stays welcoming, Smart leans medium/hard, and Hard makes the lane noticeably tougher without changing point values.\n"
            "Template editing stays inside those mastery commands with `template_action`, supports a default template plus optional tier overrides, and falls back as: tier override -> scope default -> Babblebox default.\n"
            "Category tokens: `{user.mention}` `{user.name}` `{user.display_name}` `{role.name}` `{tier.label}` `{threshold}` `{category.name}`. Scholar tokens: `{user.mention}` `{user.name}` `{user.display_name}` `{role.name}` `{tier.label}` `{threshold}`.\n"
            "Admins can use `/dropsadmin` to run 1-10 drops a day, pick channels and categories, and opt into rare AI celebration copy without turning the lane into spam."
        ),
        "try": "`/drops status`, `/dropsadmin mastery category`, `/drops leaderboard`",
    },
    {
        "title": "Everyday Utilities",
        "emoji": "\U0001f9f0",
        "description": "Low-noise tools built for real server life.",
        "body": (
            "`/watch mentions`, `/watch replies`, and `/watch keyword ...` keep private DM alerts tidy.\n"
            "`/later`, `/capture`, `/remind`, `/afk`, `/afktimezone`, and `/afkschedule` stay personal by default.\n"
            "`/moment` turns a message or exchange into a shareable keepsake card with a live link."
        ),
        "try": "`/watch settings`, `/later mark`, `/capture`, `/moment from-reply`",
    },
    {
        "title": "Daily Arcade",
        "emoji": "\U0001f579\ufe0f",
        "description": "Three fast arcade booths, kept separate from knowledge mastery.",
        "body": (
            "`/daily` opens Shuffle, Emoji, and Signal.\n"
            "`/daily play <guess>` still defaults to Shuffle Booth.\n"
            "Shuffle now leans on stronger word shapes, Emoji uses layered clueing, and Signal rotates across Caesar shift, mirror alphabet, and adjacent-pair swap.\n"
            "Open and result cards show difficulty, answer length, and the booth profile without spoiling failed answers.\n"
            "Daily stays arcade-only, so clears and streaks here do not feed Question Drops mastery roles or scholar ranks.\n"
            "Question Drops stay separate as the guild knowledge lane, so the arcade never blurs into mastery progression.\n"
            "Numeric or multiple-choice drops still accept the correct option letter or option text, and quiet channels can skip a slot.\n"
            "A live drop still blocks `/play` in that same channel until it resolves.\n"
            "`/daily`, `/daily play`, `/daily stats`, `/daily share`, and `/daily leaderboard` lean public by default while warnings stay private."
        ),
        "try": "`/daily`, `/daily stats`, `/drops status`",
    },
    {
        "title": "Buddy / Profile / Vault",
        "emoji": "\U0001f324\ufe0f",
        "description": "One identity layer instead of a giant economy system.",
        "body": (
            "`/buddy` and `/profile` are public-friendly by default.\n"
            "`/vault` stays more personal.\n"
            "Buddy style, streaks, titles, badges, utilities, multiplayer highlights, and guild-first knowledge flavor all meet here.\n"
            "Question Drops mastery roles, scholar ranks, and leaderboard position show up here without turning the card into a giant stat slab.\n"
            "Profiles keep Knowledge, Daily Arcade, and party-game highlights separate so the lanes stay easy to understand."
        ),
        "try": "`/buddy`, `/profile`, `/vault`",
    },
    {
        "title": "Shield / Admin Safety",
        "emoji": "\U0001f6e1\ufe0f",
        "description": "Optional server-side protection and compact admin automations with conservative defaults.",
        "body": (
            "Shield can watch for privacy leaks, invite or promo abuse, and experimental scam heuristics.\n"
            "`/shield panel`, `/shield rules`, `/shield ai`, and `/shield test` cover the core admin flow.\n"
            "`/admin panel`, `/admin followup`, `/admin verification`, and `/admin sync` cover returned-after-ban follow-up roles and unverified cleanup.\n"
            "Everything stays off until an admin configures it.\n"
            "Defaults are log-first, with low/medium/high action policy, allowlists, trusted-role bypasses, private mod-log alerts, and safe advanced wildcard patterns instead of raw regex.\n"
            "Repeated noisy links can stay low-confidence and log-only instead of being misread as strong promo.\n"
            "AI assist is optional, admin-only, support-server limited by default, and only reviews messages that local Shield already flagged. It never punishes on its own.\n"
            "Admin lifecycle helpers stay compact: no Babblebox ban/kick command suite, no giant case archive, and no per-member task explosion."
        ),
        "try": "`/shield panel`, `/admin panel`, `/admin verification`, `/admin sync`",
    },
    {
        "title": "Setup / Tips",
        "emoji": "\u2728",
        "description": "A few quick habits make Babblebox feel much better.",
        "body": (
            "Keep DMs open for Watch, Later, Capture, reminders, Pattern Hunt, and other DM-based party moments.\n"
            "Use public visibility for showable cards, and private visibility for sensitive utility flows.\n"
            "Let a live Question Drop finish before opening `/play` in that same channel.\n"
            "If you run Shield, start with log-only or low-confidence logging and tune filters before enabling deletes."
        ),
        "try": "Open DMs, start with `/help`, then pick one lane to try first.",
    },
]


def build_help_page_embed(page_index: int) -> discord.Embed:
    page = HELP_PAGES[page_index]
    embed = discord.Embed(
        title=f"{page['emoji']} {page['title']}",
        description=page["description"],
        color=ge.EMBED_THEME["accent"] if page_index else discord.Color.gold(),
    )
    embed.add_field(name="Overview", value=page["body"], inline=False)
    embed.add_field(name="Try", value=page.get("try", "`/help`"), inline=False)
    embed.add_field(name="Page", value=f"{page_index + 1}/{len(HELP_PAGES)}", inline=True)
    embed.add_field(name="Visibility", value="Showable cards default public. Sensitive utilities stay private.", inline=True)
    return ge.style_embed(embed, footer="Babblebox Manual | Use the arrows to browse")


class HelpPanelView(discord.ui.View):
    def __init__(self, *, author_id: int, start_index: int = 0):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.page_index = start_index
        self.message: discord.Message | None = None
        self._refresh_buttons()

    def current_embed(self) -> discord.Embed:
        return build_help_page_embed(self.page_index)

    def _refresh_buttons(self):
        self.previous_button.disabled = self.page_index <= 0
        self.home_button.disabled = self.page_index == 0
        self.next_button.disabled = self.page_index >= len(HELP_PAGES) - 1

    async def _render(self, interaction: discord.Interaction):
        self._refresh_buttons()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "This Panel Is Locked",
                "Use `/help` to open your own help panel.",
                tone="info",
                footer="Babblebox Manual",
            ),
            ephemeral=True,
        )
        return False

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            with contextlib.suppress(discord.HTTPException):
                await self.message.edit(view=self)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="\u2b05\ufe0f")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page_index = max(0, self.page_index - 1)
        await self._render(interaction)

    @discord.ui.button(label="Home", style=discord.ButtonStyle.primary, emoji="\U0001f3e0")
    async def home_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page_index = 0
        await self._render(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="\u27a1\ufe0f")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page_index = min(len(HELP_PAGES) - 1, self.page_index + 1)
        await self._render(interaction)


class MetaCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._help_user_cooldowns: dict[int, float] = {}
        self._help_channel_cooldowns: dict[int, float] = {}

    def _is_private(self, visibility: str) -> bool:
        return visibility == "private"

    def _help_cooldown_error(self, ctx: commands.Context, *, visibility: str) -> str | None:
        if self._is_private(visibility):
            return None
        now = self.bot.loop.time()
        user_remaining = 15.0 - (now - self._help_user_cooldowns.get(ctx.author.id, 0.0))
        channel_key = ctx.channel.id if ctx.channel is not None else 0
        channel_remaining = 8.0 - (now - self._help_channel_cooldowns.get(channel_key, 0.0))
        if user_remaining > 0 or channel_remaining > 0:
            wait_for = int(max(user_remaining, channel_remaining)) + 1
            return f"The public manual is on cooldown. Try again in about {wait_for} seconds, or switch visibility to private."
        self._help_user_cooldowns[ctx.author.id] = now
        if channel_key:
            self._help_channel_cooldowns[channel_key] = now
        return None

    @commands.hybrid_command(name="help", with_app_command=True, description="View the Babblebox manual, categories, and command guide")
    @app_commands.describe(visibility="Show the manual publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def help_command(self, ctx: commands.Context, visibility: str = "public"):
        if not await require_channel_permissions(ctx, ge.HELP_REQUIRED_PERMS, "/help"):
            return
        cooldown_error = self._help_cooldown_error(ctx, visibility=visibility)
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Help Cooldown", cooldown_error, tone="warning", footer="Babblebox Manual"),
                ephemeral=True,
            )
            return
        view = HelpPanelView(author_id=ctx.author.id)
        message = await send_hybrid_response(
            ctx,
            embed=view.current_embed(),
            view=view,
            ephemeral=self._is_private(visibility),
        )
        if message is not None:
            view.message = message

    @commands.hybrid_command(name="ping", with_app_command=True, description="Check if the bot is online and responsive")
    async def ping_command(self, ctx: commands.Context):
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Pong!",
                "Babblebox is online, responsive, and ready for games, utilities, Daily, and Buddy commands.",
                tone="success",
            ),
            ephemeral=True,
        )

    @commands.hybrid_command(name="stats", with_app_command=True, description="View Babblebox session stats")
    @app_commands.describe(user="Whose session stats to view")
    async def stats_command(self, ctx: commands.Context, user: Optional[discord.User] = None):
        target = user or ctx.author
        stats = ge.session_stats.get(target.id)
        if not stats:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "No Stats Yet",
                    "No session stats were found for that player yet. Finish a game first.",
                    tone="warning",
                    footer="Babblebox Session Stats",
                ),
                ephemeral=True,
            )
            return

        await send_hybrid_response(ctx, embed=ge.build_stats_embed(target, stats), ephemeral=True)

    @commands.hybrid_command(name="leaderboard", with_app_command=True, description="View the Babblebox session leaderboard")
    @app_commands.describe(metric="What to rank players by")
    @app_commands.choices(
        metric=[
            app_commands.Choice(name="Wins", value="wins"),
            app_commands.Choice(name="Bomb Wins", value="bomb_wins"),
            app_commands.Choice(name="Bomb Words", value="bomb_words"),
            app_commands.Choice(name="Spy Wins", value="spy_wins"),
        ]
    )
    async def leaderboard_command(self, ctx: commands.Context, metric: str = "wins"):
        if metric not in LEADERBOARD_LABELS:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Unknown Metric",
                    f"Try one of: {', '.join(LEADERBOARD_LABELS)}.",
                    tone="warning",
                    footer="Babblebox Leaderboard",
                ),
                ephemeral=True,
            )
            return

        entries = [value for value in ge.session_stats.values() if value.get(metric, 0) > 0]
        if not entries:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "No Leaderboard Data",
                    "Nobody has any stats in that category yet. Finish a few games first.",
                    tone="warning",
                    footer="Babblebox Leaderboard",
                ),
                ephemeral=True,
            )
            return

        entries.sort(
            key=lambda item: (item.get(metric, 0), item.get("wins", 0), item.get("games_played", 0)),
            reverse=True,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.build_leaderboard_embed(metric, LEADERBOARD_LABELS[metric], entries),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(MetaCog(bot))
