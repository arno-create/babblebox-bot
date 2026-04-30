from __future__ import annotations

import contextlib
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import HybridPanelSendResult, require_channel_permissions, send_hybrid_panel_response, send_hybrid_response
from babblebox.official_links import OFFICIAL_LINKS


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
HELP_VIEW_TIMEOUT_SECONDS = 900
EMBED_FIELD_VALUE_LIMIT = 1024
SELECT_DESCRIPTION_LIMIT = 100
LOGGER = logging.getLogger(__name__)


def official_links_markdown() -> str:
    return "\n".join(f"[{label}]({url})" for label, url in OFFICIAL_LINKS)


HELP_PAGES: list[dict[str, str]] = [
    {
        "title": "Babblebox Guide",
        "emoji": "\U0001f3e0",
        "description": "Seven clear lanes: Party Games, Question Drops, Daily Arcade, Utilities, Vote Bonus, Buddy/Profile, and Shield.",
        "body": (
            "Use the arrows to browse the product by lane.\n"
            "Public-friendly surfaces lean public by default so they feel native in chat.\n"
            "Personal utilities, setup, and sensitive admin work stay private."
        ),
        "try": "`/play`, `/drops status`, `/daily`, `/profile`, `/watch`, `/vote`",
    },
    {
        "title": "Party Games",
        "emoji": "\U0001f389",
        "description": "Short multiplayer rooms for live server energy.",
        "body": (
            "`/play` opens the lobby for Broken Telephone, Exquisite Corpse, Spyfall, Word Bomb, and Pattern Hunt.\n"
            "Broken Telephone runs as a DM relay: original clip, panic mimics, then one final typed guess.\n"
            "Exquisite Corpse keeps six hidden prompts for 3-6 players so every counted player gets a turn.\n"
            "Spyfall gives everyone a secret DM role, rotates the spotlight through the room, supports `/spyfall target @member`, and lets any player call the vote.\n"
            "Word Bomb stays fast: one compact turn line, live syllable, no repeats, and the fuse keeps shrinking.\n"
            "Pattern Hunt: one guesser asks named pattern holders normal questions, holders answer while following a hidden rule, and natural private guesses with `/hunt guess` keep the theory offstage. Pattern holders need server DMs open before start. `Contains Digits` means digits `0-9` only.\n"
            "`/hunt status` mirrors the live state card privately, and `/hunt guess contains a number` is the prefix shape for natural theories."
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
            "Admins can use `/dropsadmin` to run 1-10 drops a day, pick channels and categories, set `/dropsadmin ping` for one safe live role mention, and opt into Guild Pro AI celebration copy while the smarter offline question rotation stays core."
        ),
        "fields": [
            (
                "Guild Lane",
                (
                    "`/drops status` shows the guild knowledge lane clearly.\n"
                    "`/drops stats` and `/drops leaderboard` stay guild-first, while Buddy and Profile surfaces fold the knowledge lane into identity cleanly.\n"
                    "`/drops roles status`, `/drops roles remove`, and `/drops roles preference` give members a private way to remove current Babblebox roles or stop future grants without touching achievement history.\n"
                    "Babblebox now keeps a wider offline pool in rotation so one busy server sees fewer near-clones and fewer same-family repeats."
                ),
            ),
            (
                "Config / Cadence",
                (
                    "`/dropsadmin config` also controls the difficulty profile: Standard stays welcoming, Smart leans medium/hard, and Hard makes the lane noticeably tougher without changing point values.\n"
                    "Admins can use `/dropsadmin` to run 1-10 drops a day, pick channels and categories, set `/dropsadmin ping` for one safe live role mention, and opt into rare AI celebration copy without turning the lane into spam.\n"
                    "Optional Question Drops AI celebration copy can run on Babblebox Guild Pro when celebration policy and provider/runtime readiness allow live copy; the offline content upgrade and smarter repeat resistance stay available on the core lane."
                ),
            ),
            (
                "Mastery / Scholar",
                (
                    "`/dropsadmin mastery category ...` and `/dropsadmin mastery scholar ...` configure category mastery roles, the guild scholar ladder, and custom mastery announcement copy in one place.\n"
                    "Template editing stays inside those mastery commands with `template_action`, supports a default template plus optional tier overrides, and falls back as: tier override -> scope default -> Babblebox default.\n"
                    "Category tokens: `{user.mention}` `{user.name}` `{user.display_name}` `{role.name}` `{tier.label}` `{threshold}` `{category.name}`. Scholar tokens: `{user.mention}` `{user.name}` `{user.display_name}` `{role.name}` `{tier.label}` `{threshold}`."
                ),
            ),
        ],
        "try": "`/drops status`, `/dropsadmin mastery category`, `/drops leaderboard`",
    },
    {
        "title": "Everyday Utilities",
        "emoji": "\U0001f9f0",
        "description": "Low-noise tools built for real server life.",
        "body": (
            "`/watch mentions`, `/watch replies`, and `/watch keyword ...` keep private DM alerts tidy.\n"
            "`/later`, `/capture`, `/remind`, `/afk`, `/afktimezone`, and `/afkschedule` stay personal by default.\n"
            "Capture sends a private recent-message snapshot whose transcript file may include currently available attachment URLs at send time, Later keeps compact attachment labels instead of raw attachment URLs, and Watch stays quiet until something relevant happens.\n"
            "Babblebox Plus raises saved-vs-active headroom for Watch, reminders, and recurring AFK, while downgrades keep saved config intact and only pause extra active headroom until you trim it or Plus returns."
        ),
        "try": "`/watch settings`, `/later mark`, `/capture`, `/remind set`",
    },
    {
        "title": "Vote Bonus",
        "emoji": "\U0001f5f3\ufe0f",
        "description": "A temporary Top.gg utility boost that stays separate from paid premium.",
        "body": (
            "`/vote` opens a private Top.gg status panel with a direct vote link, live refresh, and reminder toggle.\n"
            "Free and Supporter users can unlock a temporary utility boost after a Top.gg vote: Watch keywords `10 -> 15`, Watch filters `8 -> 12`, active reminders `3 -> 5`, active public reminders `1 -> 2`, and recurring AFK schedules `6 -> 10`.\n"
            "Babblebox Plus and Guild Pro stay above this lane already, so votes never change those paid tiers.\n"
            "Vote reminders stay opt-in, and reminder DMs tell you how to turn them back off from `/vote`."
        ),
        "try": "`/vote`",
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
        "title": "Premium / Plans",
        "emoji": "\U0001f48e",
        "description": "Buy on Patreon, compare the three combined tiers, link privately in Discord, and claim Guild Pro only where you want it.",
        "fields": [
            (
                "Choose a Plan",
                (
                    "`Supporter` is the paid support tier: it includes Inevitable Friendship Discord benefits while Babblebox stays at Free limits.\n"
                    "`Babblebox Plus` maps to IF Epic Patron and raises personal Watch, reminder, and recurring AFK limits.\n"
                    "`Babblebox Guild Pro` maps to IF Legendary Patron and is for server admins who want higher bounded server caps, Shield AI's higher model tiers when owner policy and provider/runtime readiness allow review, optional Question Drops AI celebrations when celebration policy and provider/runtime readiness allow live copy, and the larger safe Confessions image ceiling."
                ),
            ),
            (
                "How Premium Activates",
                (
                    "1. Use `/premium plans` to compare the three combined Patreon tiers.\n"
                    "2. Use `/premium subscribe` to open Patreon and buy Supporter, Babblebox Plus, or Babblebox Guild Pro.\n"
                    "3. Use `/premium link` in Discord so Babblebox can connect that Patreon account to your Discord user.\n"
                    "4. Use `/premium status` to confirm the linked personal plan and any Guild Pro claim-ready state.\n"
                    "5. If you bought Guild Pro, use `/premium guild claim` inside the server you want to upgrade, then confirm with `/premium guild status`."
                ),
            ),
            (
                "Patreon Tier Mapping",
                (
                    "Patreon now has three combined tiers: Supporter, Babblebox Plus, and Babblebox Guild Pro.\n"
                    "Babblebox Plus maps to IF Epic Patron and Babblebox Guild Pro maps to IF Legendary Patron.\n"
                    "Every paid tier includes both Babblebox and Inevitable Friendship benefits. If you are linked but still see Free after a tier change, run `/premium status` or `/premium refresh`, then use `/support` if the mapped tier still does not appear."
                ),
            ),
            (
                "Trust / Downgrade",
                (
                    "Free keeps core safety, privacy, and the baseline utility lane. Babblebox does not process cards or reverse Patreon or Apple charges directly.\n"
                    "Unlinking deletes Babblebox's local encrypted Patreon tokens only.\n"
                    "Downgrades or Guild Pro release do not delete saved Watch, reminder, AFK, Shield, or Confessions settings; extra runtime headroom simply pauses while saved config stays preserved, and future expansion stays blocked until you trim it or premium returns."
                ),
            ),
            (
                "Payment / Billing",
                (
                    "Start payment, billing, duplicate-charge, unauthorized-charge, or refund issues with Patreon, or with Apple for iOS purchases.\n"
                    "Use `/support` for Babblebox linking, resolved-tier, stale-entitlement, or Guild Pro claim issues.\n"
                    "Refund outcomes follow Patreon or Apple policy and applicable law, not a separate Babblebox guarantee.\n"
                    "Patreon policy: https://support.patreon.com/hc/en-us/articles/205032045-Patreon-s-refund-policy\n"
                    "Patreon refund help: https://support.patreon.com/hc/en-us/articles/360021113811-How-do-I-request-a-refund\n"
                    "Apple billing help: https://reportaproblem.apple.com/\n"
                    "Premium promises are the documented entitlement surfaces and limits, not guaranteed uptime or automatic AI activity."
                ),
            ),
        ],
        "try": "`/premium plans`, `/premium status`, `/premium subscribe`, `/premium guild status`",
    },
    {
        "title": "Support / Links",
        "emoji": "\U0001f6df\ufe0f",
        "description": "Official places to get help, report issues, and inspect the product.",
        "body": (
            "Use `/support` for the standalone support card whenever you need the official links quickly.\n"
            "If something breaks, feels confusing, or could be better, reporting it is genuinely appreciated and helps shape the next fix or polish pass.\n"
            "Use `/premium plans`, `/premium subscribe`, and `/premium status` when the question is specifically about buying, linking, or checking Babblebox premium.\n"
            "Start payment, billing, duplicate-charge, unauthorized-charge, or refund issues with Patreon, or with Apple for iOS purchases.\n"
            "Refund outcomes follow Patreon or Apple policy and applicable law, not a separate Babblebox guarantee.\n"
            "Use the Babblebox support routes when the linked tier, stale entitlement state, or Guild Pro claim still looks wrong."
        ),
        "links": official_links_markdown(),
        "try": "`/support`, `/premium plans`, `/premium status`, or use the link buttons below.",
    },
    {
        "title": "Shield / Admin Safety",
        "emoji": "\U0001f6e1\ufe0f",
        "description": "Optional server-side protection and compact admin automations with conservative defaults.",
        "body": (
            "Shield can watch for privacy leaks, invite or promo abuse, explicit Anti-Spam rules for fast bursts and near-duplicate floods, optional emote, excessive-capitals, and low-value chatter lanes, a separate GIF Flood / Media Pressure lane for disruptive GIF streaks and channel pressure with lightweight meaningful-text balance, malicious or scam links, no-link DM-lure bait, adult-domain intel plus optional solicitation / DM-ad text, and a separate Severe Harm / Hate pack for real-harm abuse only.\n"
            "`/shield panel`, `/shield rules`, `/shield links`, `/shield trusted`, `/shield filters`, `/shield exemptions`, `/shield allowlist`, `/shield logs`, `/shield severe category`, `/shield severe term`, `/shield ai`, and `/shield test` cover the shipped admin flow.\n"
            "`/shield panel` is the panel-first editor: pick one pack, then use Actions, Options, or Exemptions so Babblebox only shows relevant controls and keeps global scope separate from pack-local exemptions.\n"
            "`/lock channel` and `/lock remove` are Babblebox's direct emergency moderator/admin lane for calm channel lockdowns. `/lock settings` lets admins tune the default notice and optionally limit the lane to admins only.\n"
            "If a channel is already fully denied, Babblebox can track that lock without reopening it later and only clears its own timer or marker on unlock.\n"
            "`/timeout remove` is the direct moderator/admin lane for safely clearing active member timeouts.\n"
            "`/admin panel`, `/admin followup`, `/admin logs`, `/admin exclusions`, and `/admin permissions` cover returned-after-ban follow-up roles, permission diagnostics, and compact admin operations.\n"
            "`/admin panel` is now the interactive control surface: use its overview quick-config row or section buttons to open focused editors for follow-up, exclusions, and logs, then keep the slash commands as the precise fallback path.\n"
            "Live moderation stays off until an admin enables it, and the first enable applies a recommended non-AI baseline while Shield AI access stays owner-managed.\n"
            "Each live pack can inherit the global timeout or keep a dedicated timeout profile, and the trusted-link lane can do the same without turning the UI into a giant matrix.\n"
            "Trusted Links Only now exposes its built-in trusted families and domains under `/shield trusted`, keeps Confessions link mode separate, and still lets malicious, impersonation, adult, or suspicious-link intel override local trust exceptions.\n"
            "Configured anti-spam stays explainable: admins set message and near-duplicate thresholds, optional emote, capitals, and low-value chatter lanes stay off until enabled, moderators are exempt from Anti-Spam by default unless you opt into delete-only or full enforcement, pack-specific exemptions stay separate from global filters, GIF incidents are grouped per offender and window, spam and GIF delete actions remove the matched burst, Shield alert buttons stay reversible-first with false-positive recovery instead of kick, ban, or timeout-punishment shortcuts, and bot or webhook posts stay on a more conservative scam threshold unless the evidence is genuinely strong.\n"
            "Shield AI is second-pass only, live-message-only, and owner-managed. Owner policy controls whether review runs, `/shield ai` only configures review scope, runtime routes between gpt-5-nano, gpt-5-mini, and gpt-5, the owner-managed model policy can keep all three configured by default, effective higher-tier use still needs Guild Pro plus provider/runtime readiness, diagnostics report the effective lane plus local readiness, entitlement state, and provider gates, and AI never punishes on its own.\n"
            "Admin lifecycle helpers stay compact: no giant control-plane dashboard, no giant case archive, and no per-member task explosion."
        ),
        "try": "`/shield panel`, `/shield rules`, `/shield exemptions`, `/shield trusted`, `/shield filters`, `/admin panel`",
    },
    {
        "title": "Setup / Tips",
        "emoji": "\u2728",
        "description": "A few quick habits make Babblebox feel much better.",
        "body": (
            "Keep DMs open for Watch, Later, Capture, reminders, Pattern Hunt, and other DM-based party moments.\n"
            "Daily Arcade, Buddy, and Profile fit public channels best, while utilities and setup flows work better privately.\n"
            "Let a live Question Drop finish before opening `/play` in that same channel.\n"
            "If you run Shield, start with log-only or low-confidence logging and tune filters or pack-specific exemptions before enabling deletes or timeouts."
        ),
        "try": "Open DMs, start with `/help`, then pick one lane to try first.",
    },
]


def _truncate_select_description(text: str) -> str:
    if len(text) <= SELECT_DESCRIPTION_LIMIT:
        return text
    return text[: SELECT_DESCRIPTION_LIMIT - 3].rstrip() + "..."


def _split_help_field(name: str, value: str) -> list[tuple[str, str]]:
    text = value.strip()
    if len(text) <= EMBED_FIELD_VALUE_LIMIT:
        return [(name, text)]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_length = 0

    def flush():
        nonlocal current_lines, current_length
        if current_lines:
            chunks.append("\n".join(current_lines))
            current_lines = []
            current_length = 0

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        line_length = len(line)
        projected = line_length if not current_lines else current_length + 1 + line_length
        if projected <= EMBED_FIELD_VALUE_LIMIT:
            current_lines.append(line)
            current_length = projected
            continue

        flush()
        while line:
            chunk = line[:EMBED_FIELD_VALUE_LIMIT]
            chunks.append(chunk)
            line = line[EMBED_FIELD_VALUE_LIMIT:]

    flush()
    if not chunks:
        return [(name, text[:EMBED_FIELD_VALUE_LIMIT])]
    if len(chunks) == 1:
        return [(name, chunks[0])]
    return [
        (name if index == 0 else f"{name} (cont. {index + 1})", chunk)
        for index, chunk in enumerate(chunks)
    ]


def _help_content_fields(page: dict[str, str]) -> list[tuple[str, str]]:
    raw_fields = page.get("fields")
    if raw_fields:
        fields: list[tuple[str, str]] = []
        for field_name, field_value in raw_fields:
            fields.extend(_split_help_field(field_name, field_value))
        return fields
    return _split_help_field("Overview", page["body"])


def build_help_page_embed(page_index: int) -> discord.Embed:
    page = HELP_PAGES[page_index]
    embed = discord.Embed(
        title=f"{page['emoji']} {page['title']}",
        description=page["description"],
        color=ge.EMBED_THEME["accent"] if page_index else discord.Color.gold(),
    )
    for field_name, field_value in _help_content_fields(page):
        embed.add_field(name=field_name, value=field_value, inline=False)
    if page.get("links"):
        for field_name, field_value in _split_help_field("Links", page["links"]):
            embed.add_field(name=field_name, value=field_value, inline=False)
    embed.add_field(name="Try", value=page.get("try", "`/help`"), inline=False)
    embed.add_field(name="Page", value=f"{page_index + 1}/{len(HELP_PAGES)}", inline=True)
    embed.add_field(name="Visibility", value="Showable cards default public. Sensitive utilities stay private.", inline=True)
    return ge.style_embed(embed, footer="Babblebox Manual | Use the arrows to browse")


def build_help_embed() -> discord.Embed:
    embed = discord.Embed(
        title="\U0001f3e0 Babblebox Help",
        description=(
            "Babblebox is organized into clear lanes so it stays easy to learn in a live server. "
            "Start with the lane you need, use `/premium plans` when you want the upgrade path, and use `/support` any time you want the official links or bug-report routes."
        ),
        color=discord.Color.gold(),
    )
    embed.add_field(
        name="Party Games",
        value=(
            "`/play` opens Broken Telephone, Exquisite Corpse, Spyfall, Word Bomb, and Pattern Hunt. "
            "Use `/hunt status` for the private Pattern Hunt state card."
        ),
        inline=False,
    )
    embed.add_field(
        name="Question Drops",
        value=(
            "`/drops status`, `/drops stats`, `/drops leaderboard`, and `/drops roles ...` cover the guild knowledge lane. "
            "Admin setup and mastery configuration live under `/dropsadmin`."
        ),
        inline=False,
    )
    embed.add_field(
        name="Daily / Utilities",
        value=(
            "`/daily` handles Shuffle, Emoji, and Signal. Personal tools include `/watch`, `/later`, `/capture`, "
            "`/remind`, and `/afk`."
        ),
        inline=False,
    )
    embed.add_field(
        name="Buddy / Profile / Vault",
        value=(
            "`/buddy`, `/profile`, and `/vault` show identity, streaks, and highlights without turning the bot "
            "into a giant economy system."
        ),
        inline=False,
    )
    embed.add_field(
        name="Premium",
        value=(
            "`/premium plans`, `/premium status`, `/premium subscribe`, and `/premium guild status` "
            "cover plan comparison, Patreon purchase, private linking, and Guild Pro server claims."
        ),
        inline=False,
    )
    embed.add_field(
        name="Shield / Admin",
        value=(
            "`/shield panel`, `/shield rules`, `/shield exemptions`, `/shield links`, `/shield filters`, `/shield severe category`, "
            "`/shield severe term`, `/shield ai`, `/lock channel`, `/lock remove`, `/timeout remove`, `/admin panel`, `/admin followup`, `/admin logs`, "
            "`/admin exclusions`, and `/admin permissions` "
            "cover focused safety setup and compact admin automation, with `/admin panel` now acting as the interactive control surface for the common admin settings."
        ),
        inline=False,
    )
    embed.add_field(
        name="Support / Links",
        value=(
            "Use `/support` for the official support card, and `/premium plans` or `/premium status` if you are buying, linking, or checking Babblebox premium.\n"
            "Patreon or Apple handle payment and refund workflows; Babblebox support handles entitlement, linking, and Guild Pro claim issues.\n"
            "Patreon refund help: https://support.patreon.com/hc/en-us/articles/360021113811-How-do-I-request-a-refund\n"
            "Apple billing help: https://reportaproblem.apple.com/\n"
            f"{official_links_markdown()}"
        ),
        inline=False,
    )
    embed.add_field(
        name="Visibility",
        value="Daily Arcade, Buddy, and Profile work well in public channels. Personal utilities and setup flows work better privately.",
        inline=False,
    )
    return ge.style_embed(embed, footer="Babblebox Help | Start here, then open the lane you need")


def build_support_embed() -> discord.Embed:
    embed = discord.Embed(
        title="\U0001f6df\ufe0f Babblebox Support",
        description=(
            "If something breaks, feels confusing, or could be better, please report it. "
            "This is also the quickest official route for buying Babblebox premium, checking combined Patreon tier confusion, or getting live help when a link or claim looks wrong."
        ),
        color=discord.Color.gold(),
    )
    embed.add_field(name="Official Links", value=official_links_markdown(), inline=False)
    embed.add_field(
        name="Premium",
        value=(
            "`Patreon Membership` is where the three combined Babblebox + Inevitable Friendship tiers are purchased.\n"
            "After you buy Supporter, Babblebox Plus, or Babblebox Guild Pro, use `/premium link` in Discord.\n"
            "If you bought Guild Pro, finish with `/premium guild claim` in the server you want to upgrade.\n"
            "Start payment, billing, duplicate-charge, unauthorized-charge, or refund issues with Patreon, or with Apple for iOS purchases.\n"
            "Refund outcomes follow Patreon or Apple policy and applicable law, not a separate Babblebox guarantee.\n"
            "If your Patreon link, resolved tier, or Guild Pro claim still looks wrong, check `/premium status` or `/premium refresh`, then use the support server for live help."
        ),
        inline=False,
    )
    embed.add_field(
        name="Best Route",
        value=(
            "`Support Server` for live help, combined-tier questions, and stale premium or Guild Pro claim states.\n"
            "`Patreon or Apple` first for payment, billing, duplicate-charge, unauthorized-charge, or refund issues.\n"
            "`Patreon Membership` to buy Babblebox premium before linking it in Discord.\n"
            "`Official Website` for the public premium guide, help page, and policies.\n"
            "`GitHub Repository` for bug reports, issues, and the open-source code."
        ),
        inline=False,
    )
    return ge.style_embed(embed, footer="Babblebox Support | Thanks for helping improve the bot")


def add_official_link_buttons(view: discord.ui.View, *, row: int):
    for label, url in OFFICIAL_LINKS:
        view.add_item(discord.ui.Button(label=label, style=discord.ButtonStyle.link, url=url, row=row))


class SupportLinksView(discord.ui.View):
    def __init__(self, *, timeout: float | None = None, button_row: int = 0):
        super().__init__(timeout=timeout)
        add_official_link_buttons(self, row=button_row)


class HelpPageSelect(discord.ui.Select):
    def __init__(self, view: "HelpPanelView"):
        self.help_view = view
        options = [
            discord.SelectOption(
                label=page["title"],
                description=_truncate_select_description(page["description"]),
                emoji=page["emoji"],
                value=str(index),
                default=index == view.page_index,
            )
            for index, page in enumerate(HELP_PAGES)
        ]
        super().__init__(
            placeholder="Jump to a help section...",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    def refresh_state(self):
        self.placeholder = f"Jump to: {HELP_PAGES[self.help_view.page_index]['title']}"
        current_value = str(self.help_view.page_index)
        for option in self.options:
            option.default = option.value == current_value

    async def callback(self, interaction: discord.Interaction):
        try:
            page_index = int(self.values[0])
        except (TypeError, ValueError, IndexError):
            await ge.safe_send_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Help Panel Error",
                    "That help section could not be opened. Run `/help` again if this keeps happening.",
                    tone="warning",
                    footer="Babblebox Manual",
                ),
                ephemeral=True,
            )
            return
        self.help_view.page_index = max(0, min(len(HELP_PAGES) - 1, page_index))
        await self.help_view._render(interaction)


class HelpPanelView(SupportLinksView):
    def __init__(self, *, author_id: int, start_index: int = 0):
        super().__init__(timeout=HELP_VIEW_TIMEOUT_SECONDS, button_row=2)
        self.author_id = author_id
        self.page_index = start_index
        self.message: discord.Message | None = None
        self._delivery_bot: commands.Bot | None = None
        self._delivery_channel = None
        self._delivery_channel_id: int | None = None
        self._delivery_message_id: int | None = None
        self._delivery_interaction: discord.Interaction | None = None
        self._delivery_private = False
        self.page_select = HelpPageSelect(self)
        self.add_item(self.page_select)
        self._refresh_controls()

    def current_embed(self) -> discord.Embed:
        return build_help_page_embed(self.page_index)

    def _refresh_controls(self):
        self.previous_button.disabled = self.page_index <= 0
        self.home_button.disabled = self.page_index == 0
        self.next_button.disabled = self.page_index >= len(HELP_PAGES) - 1
        self.page_select.refresh_state()

    def bind_delivery(
        self,
        *,
        bot: commands.Bot,
        channel,
        interaction: discord.Interaction | None,
        visibility: str,
        result: HybridPanelSendResult,
    ):
        if result.message is not None:
            self.message = result.message
        self._delivery_bot = bot
        self._delivery_channel = channel
        self._delivery_channel_id = getattr(channel, "id", None)
        self._delivery_message_id = result.message_id or getattr(result.message, "id", None)
        self._delivery_interaction = interaction
        self._delivery_private = visibility == "private"

    async def _timeout_edit_message(self) -> bool:
        if self.message is not None:
            with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                updated = await self.message.edit(view=self)
                self.message = updated
                return True

        if not self._delivery_private and self._delivery_message_id is not None:
            channel = self._delivery_channel
            if channel is None and self._delivery_bot is not None and self._delivery_channel_id is not None:
                channel = self._delivery_bot.get_channel(self._delivery_channel_id)
            if channel is not None and hasattr(channel, "get_partial_message"):
                partial_message = channel.get_partial_message(self._delivery_message_id)
                with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                    updated = await partial_message.edit(view=self)
                    self.message = updated
                    return True

        interaction = self._delivery_interaction
        if interaction is not None and not interaction.is_expired():
            with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                updated = await interaction.edit_original_response(view=self)
                self.message = updated
                return True

        return False

    async def _render(self, interaction: discord.Interaction):
        self.message = self.message or getattr(interaction, "message", None)
        self._refresh_controls()
        edited = await ge.safe_edit_interaction_message(interaction, embed=self.current_embed(), view=self)
        if edited:
            return
        await ge.safe_send_interaction(
            interaction,
            embed=ge.make_status_embed(
                "Help Panel Expired",
                "This help panel expired or could not be refreshed. Run `/help` again for a fresh panel.",
                tone="warning",
                footer="Babblebox Manual",
            ),
            ephemeral=True,
        )

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
            if getattr(child, "style", None) != discord.ButtonStyle.link:
                child.disabled = True
        await self._timeout_edit_message()

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="\u2b05\ufe0f", row=1)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page_index = max(0, self.page_index - 1)
        await self._render(interaction)

    @discord.ui.button(label="Home", style=discord.ButtonStyle.primary, emoji="\U0001f3e0", row=1)
    async def home_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page_index = 0
        await self._render(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="\u27a1\ufe0f", row=1)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page_index = min(len(HELP_PAGES) - 1, self.page_index + 1)
        await self._render(interaction)


class MetaCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._help_user_cooldowns: dict[int, float] = {}
        self._help_channel_cooldowns: dict[int, float] = {}
        self._support_user_cooldowns: dict[int, float] = {}
        self._support_channel_cooldowns: dict[int, float] = {}

    def _is_private(self, visibility: str) -> bool:
        return visibility == "private"

    def _should_require_channel_permissions(self, ctx: commands.Context, *, visibility: str) -> bool:
        interaction = getattr(ctx, "interaction", None)
        return not (self._is_private(visibility) and interaction is not None and not interaction.is_expired())

    def _public_panel_cooldown_error(
        self,
        ctx: commands.Context,
        *,
        visibility: str,
        user_cooldowns: dict[int, float],
        channel_cooldowns: dict[int, float],
        panel_label: str,
    ) -> str | None:
        if self._is_private(visibility):
            return None
        now = self.bot.loop.time()
        user_remaining = 15.0 - (now - user_cooldowns.get(ctx.author.id, 0.0))
        channel_key = ctx.channel.id if ctx.channel is not None else 0
        channel_remaining = 8.0 - (now - channel_cooldowns.get(channel_key, 0.0))
        if user_remaining > 0 or channel_remaining > 0:
            wait_for = int(max(user_remaining, channel_remaining)) + 1
            return f"The public {panel_label} is on cooldown. Try again in about {wait_for} seconds, or switch visibility to private."
        return None

    def _record_public_panel_cooldown(
        self,
        ctx: commands.Context,
        *,
        visibility: str,
        user_cooldowns: dict[int, float],
        channel_cooldowns: dict[int, float],
    ):
        if self._is_private(visibility):
            return
        now = self.bot.loop.time()
        user_cooldowns[ctx.author.id] = now
        channel_key = ctx.channel.id if ctx.channel is not None else 0
        if channel_key:
            channel_cooldowns[channel_key] = now

    def _help_cooldown_error(self, ctx: commands.Context, *, visibility: str) -> str | None:
        return self._public_panel_cooldown_error(
            ctx,
            visibility=visibility,
            user_cooldowns=self._help_user_cooldowns,
            channel_cooldowns=self._help_channel_cooldowns,
            panel_label="manual",
        )

    def _support_cooldown_error(self, ctx: commands.Context, *, visibility: str) -> str | None:
        return self._public_panel_cooldown_error(
            ctx,
            visibility=visibility,
            user_cooldowns=self._support_user_cooldowns,
            channel_cooldowns=self._support_channel_cooldowns,
            panel_label="support panel",
        )

    def _record_help_cooldown(self, ctx: commands.Context, *, visibility: str):
        self._record_public_panel_cooldown(
            ctx,
            visibility=visibility,
            user_cooldowns=self._help_user_cooldowns,
            channel_cooldowns=self._help_channel_cooldowns,
        )

    def _record_support_cooldown(self, ctx: commands.Context, *, visibility: str):
        self._record_public_panel_cooldown(
            ctx,
            visibility=visibility,
            user_cooldowns=self._support_user_cooldowns,
            channel_cooldowns=self._support_channel_cooldowns,
        )

    def _log_panel_delivery_without_message_handle(
        self,
        *,
        event: str,
        command_name: str,
        visibility: str,
        ctx: commands.Context,
        result: HybridPanelSendResult,
    ):
        LOGGER.info(
            "%s command=%s visibility=%s path=%s handle_status=%s guild_id=%s channel_id=%s message_id=%s",
            event,
            command_name,
            visibility,
            result.path,
            result.handle_status,
            getattr(getattr(ctx, "guild", None), "id", None),
            getattr(getattr(ctx, "channel", None), "id", None),
            result.message_id,
        )

    def _log_panel_delivery_failure(
        self,
        *,
        event: str,
        command_name: str,
        visibility: str,
        ctx: commands.Context,
        result: HybridPanelSendResult,
    ):
        LOGGER.warning(
            "%s command=%s visibility=%s path=%s guild_id=%s channel_id=%s exc_class=%s",
            event,
            command_name,
            visibility,
            result.path,
            getattr(getattr(ctx, "guild", None), "id", None),
            getattr(getattr(ctx, "channel", None), "id", None),
            type(result.error).__name__ if result.error is not None else "UnknownError",
        )

    def _log_panel_delivery_fallback(
        self,
        *,
        event: str,
        command_name: str,
        visibility: str,
        ctx: commands.Context,
        primary_result: HybridPanelSendResult,
        fallback_result: HybridPanelSendResult,
    ):
        LOGGER.info(
            "%s command=%s visibility=%s primary_path=%s fallback_path=%s guild_id=%s channel_id=%s exc_class=%s",
            event,
            command_name,
            visibility,
            primary_result.path,
            fallback_result.path,
            getattr(getattr(ctx, "guild", None), "id", None),
            getattr(getattr(ctx, "channel", None), "id", None),
            type(primary_result.error).__name__ if primary_result.error is not None else "UnknownError",
        )

    async def _send_panel_response(
        self,
        ctx: commands.Context,
        *,
        command_name: str,
        visibility: str,
        cooldown_error: str | None,
        cooldown_title: str,
        cooldown_footer: str,
        embed: discord.Embed,
        view: discord.ui.View | None = None,
        recovery_title: str,
        recovery_description: str,
        recovery_footer: str,
        record_cooldown,
        send_failure_event: str,
        success_without_message_event: str,
        retry_without_view_on_failure: bool = False,
        fallback_success_event: str | None = None,
    ) -> HybridPanelSendResult:
        if self._should_require_channel_permissions(ctx, visibility=visibility):
            if not await require_channel_permissions(ctx, ge.HELP_REQUIRED_PERMS, command_name):
                return HybridPanelSendResult(delivered=False, path="permission_gate")
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(cooldown_title, cooldown_error, tone="warning", footer=cooldown_footer),
                ephemeral=True,
            )
            return HybridPanelSendResult(delivered=False)

        result = await send_hybrid_panel_response(
            ctx,
            embed=embed,
            view=view,
            ephemeral=self._is_private(visibility),
        )
        if not result.delivered and retry_without_view_on_failure and view is not None:
            fallback_result = await send_hybrid_panel_response(
                ctx,
                embed=embed,
                ephemeral=self._is_private(visibility),
            )
            if fallback_result.delivered:
                record_cooldown(ctx, visibility=visibility)
                if fallback_success_event is not None:
                    self._log_panel_delivery_fallback(
                        event=fallback_success_event,
                        command_name=command_name,
                        visibility=visibility,
                        ctx=ctx,
                        primary_result=result,
                        fallback_result=fallback_result,
                    )
                if fallback_result.message is None:
                    self._log_panel_delivery_without_message_handle(
                        event=success_without_message_event,
                        command_name=command_name,
                        visibility=visibility,
                        ctx=ctx,
                        result=fallback_result,
                    )
                return fallback_result
            result = HybridPanelSendResult(
                delivered=False,
                path=fallback_result.path,
                error=fallback_result.error or result.error,
            )
        if result.delivered:
            record_cooldown(ctx, visibility=visibility)
            if result.message is None:
                self._log_panel_delivery_without_message_handle(
                    event=success_without_message_event,
                    command_name=command_name,
                    visibility=visibility,
                    ctx=ctx,
                    result=result,
                )
            return result

        self._log_panel_delivery_failure(
            event=send_failure_event,
            command_name=command_name,
            visibility=visibility,
            ctx=ctx,
            result=result,
        )

        recovery_embed = ge.make_status_embed(
            recovery_title,
            recovery_description,
            tone="warning",
            footer=recovery_footer,
        )
        with contextlib.suppress(discord.ClientException, discord.HTTPException, discord.NotFound, TypeError, ValueError):
            await send_hybrid_response(ctx, embed=recovery_embed, ephemeral=True)
        return result

    @commands.hybrid_command(name="help", with_app_command=True, description="View the Babblebox manual, categories, and command guide")
    @app_commands.describe(visibility="Show the manual publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def help_command(self, ctx: commands.Context, visibility: str = "public"):
        await self._send_panel_response(
            ctx,
            command_name="/help",
            visibility=visibility,
            cooldown_error=self._help_cooldown_error(ctx, visibility=visibility),
            cooldown_title="Help Cooldown",
            cooldown_footer="Babblebox Manual",
            embed=build_help_embed(),
            recovery_title="Help Unavailable",
            recovery_description="Babblebox could not open help just now. Please try `/help` again in a moment.",
            recovery_footer="Babblebox Manual",
            record_cooldown=self._record_help_cooldown,
            send_failure_event="help_panel_send_failure",
            success_without_message_event="help_panel_send_success_without_message_handle",
        )

    @commands.hybrid_command(name="support", with_app_command=True, description="Open Babblebox support links and bug-report info")
    @app_commands.describe(visibility="Show support links publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def support_command(self, ctx: commands.Context, visibility: str = "public"):
        await self._send_panel_response(
            ctx,
            command_name="/support",
            visibility=visibility,
            cooldown_error=self._support_cooldown_error(ctx, visibility=visibility),
            cooldown_title="Support Cooldown",
            cooldown_footer="Babblebox Support",
            embed=build_support_embed(),
            view=SupportLinksView(),
            recovery_title="Support Panel Unavailable",
            recovery_description="Babblebox could not open the support panel just now. Please try `/support` again in a moment.",
            recovery_footer="Babblebox Support",
            record_cooldown=self._record_support_cooldown,
            send_failure_event="support_panel_send_failure",
            success_without_message_event="support_panel_send_success_without_message_handle",
            retry_without_view_on_failure=True,
            fallback_success_event="support_panel_send_fallback_without_view",
        )

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
