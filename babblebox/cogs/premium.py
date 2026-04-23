from __future__ import annotations

import logging
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import send_hybrid_response
from babblebox.official_links import PATREON_MEMBERSHIP_URL, PREMIUM_HELP_URL, SUPPORT_SERVER_URL
from babblebox.premium_limits import (
    CAPABILITY_QUESTION_DROPS_AI_CELEBRATIONS,
    CAPABILITY_SHIELD_AI_REVIEW,
    LIMIT_AFK_SCHEDULES,
    LIMIT_BUMP_DETECTION_CHANNELS,
    LIMIT_CONFESSIONS_MAX_IMAGES,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_SHIELD_CUSTOM_PATTERNS,
    LIMIT_WATCH_FILTERS,
    LIMIT_WATCH_KEYWORDS,
)
from babblebox.premium_models import LINK_STATUS_ACTIVE, LINK_STATUS_BROKEN, LINK_STATUS_REVOKED, MANUAL_KIND_BLOCK, MANUAL_KIND_GRANT, PLAN_GUILD_PRO, PROVIDER_PATREON, SCOPE_GUILD, SCOPE_USER
from babblebox.runtime_health import bind_started_service, runtime_service_lines
from babblebox.premium_store import PremiumStorageUnavailable
from babblebox.premium_service import PremiumService, format_saved_state_status, preserved_over_limit_note


LOGGER = logging.getLogger(__name__)

PATREON_REFUND_POLICY_URL = "https://support.patreon.com/hc/en-us/articles/205032045-Patreon-s-refund-policy"
PATREON_REFUND_REQUEST_URL = "https://support.patreon.com/hc/en-us/articles/360021113811-How-do-I-request-a-refund"
APPLE_REFUND_URL = "https://reportaproblem.apple.com/"


class PremiumCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = PremiumService(bot)

    async def cog_load(self):
        await bind_started_service(self.bot, attr_name="premium_service", service=self.service, label="Premium")

    def cog_unload(self):
        if getattr(self.bot, "premium_service", None) is self.service:
            delattr(self.bot, "premium_service")
        self.bot.loop.create_task(self.service.close())

    def _is_override_owner(self, user_id: int) -> bool:
        return self.service.is_system_owner(user_id)

    async def _send_private(self, ctx: commands.Context, *, embed: discord.Embed, view: discord.ui.View | None = None):
        await send_hybrid_response(ctx, embed=embed, view=view, ephemeral=True)

    async def _send_result(self, ctx: commands.Context, *, title: str, message: str, ok: bool = True):
        await self._send_private(
            ctx,
            embed=ge.make_status_embed(
                title,
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Premium",
            ),
        )

    def _mixed_campaign_note(self) -> str:
        return (
            "Patreon now has three combined tiers: Supporter, Babblebox Plus, and Babblebox Guild Pro. "
            "Babblebox Plus maps to IF Epic Patron, Babblebox Guild Pro maps to IF Legendary Patron, "
            "and every paid tier includes both Babblebox and Inevitable Friendship benefits."
        )

    def _premium_actions_view(
        self,
        *,
        auth_url: str | None = None,
        show_patreon: bool = False,
        show_compare: bool = True,
        show_support: bool = True,
    ) -> discord.ui.View:
        view = discord.ui.View()
        if auth_url:
            view.add_item(discord.ui.Button(label="Link Patreon", url=auth_url))
        elif show_patreon:
            view.add_item(discord.ui.Button(label="View Patreon", url=PATREON_MEMBERSHIP_URL))
        if show_compare:
            view.add_item(discord.ui.Button(label="Compare Plans", url=PREMIUM_HELP_URL))
        if show_support:
            view.add_item(discord.ui.Button(label="Support Server", url=SUPPORT_SERVER_URL))
        return view

    def _subscribe_view(self) -> discord.ui.View:
        return self._premium_actions_view(show_patreon=True)

    async def _require_storage(self, ctx: commands.Context) -> bool:
        if self.service.storage_ready:
            return True
        await self._send_private(
            ctx,
            embed=ge.make_status_embed(
                "Premium Unavailable",
                self.service.storage_message(),
                tone="warning",
                footer="Babblebox Premium",
            ),
        )
        return False

    async def _guild_admin_guard(self, ctx: commands.Context) -> bool:
        guild = getattr(ctx, "guild", None)
        if guild is None:
            await self._send_private(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "That premium command only works inside a server.",
                    tone="warning",
                    footer="Babblebox Premium",
                ),
            )
            return False
        perms = getattr(ctx.author, "guild_permissions", None)
        if not (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
            await self._send_private(
                ctx,
                embed=ge.make_status_embed(
                    "Manage Server Required",
                    "Only administrators or members with Manage Server can claim or release Guild Pro.",
                    tone="warning",
                    footer="Babblebox Premium",
                ),
            )
            return False
        return True

    def _active_plan_text(self, snapshot: dict[str, Any]) -> str:
        active = tuple(snapshot.get("active_plans", ()))
        if not active:
            return "Free"
        return ", ".join(self.service.plan_title(plan) for plan in active)

    def _personal_plan_text(self, snapshot: dict[str, Any]) -> str:
        active = tuple(plan for plan in snapshot.get("active_plans", ()) if plan != PLAN_GUILD_PRO)
        if not active:
            return "None"
        return ", ".join(self.service.plan_title(plan) for plan in active)

    def _utility_counts(self, user_id: int) -> dict[str, dict[str, int] | None]:
        utility_service = getattr(self.bot, "utility_service", None)
        if utility_service is None or not getattr(utility_service, "storage_ready", False):
            return {
                "watch_keywords": None,
                "watch_filters": None,
                "reminders": None,
                "public_reminders": None,
                "afk_schedules": None,
            }
        summary = utility_service.get_watch_summary(user_id, guild_id=None)
        reminder_summary = utility_service.get_reminder_summary(user_id)
        afk_summary = utility_service.get_afk_schedule_summary(user_id)
        return {
            "watch_keywords": {
                "saved": int(summary.get("total_keywords", 0)),
                "active": int(summary.get("active_keyword_count", 0)),
            },
            "watch_filters": {
                "saved": int(summary.get("saved_filter_total", 0)),
                "active": int(summary.get("active_filter_total", 0)),
            },
            "reminders": {
                "saved": int(reminder_summary.get("saved", 0)),
                "active": int(reminder_summary.get("active", 0)),
            },
            "public_reminders": {
                "saved": int(reminder_summary.get("saved_public", 0)),
                "active": int(reminder_summary.get("active_public", 0)),
            },
            "afk_schedules": {
                "saved": int(afk_summary.get("saved", 0)),
                "active": int(afk_summary.get("active", 0)),
            },
        }

    def _guild_feature_counts(self, guild_id: int) -> dict[str, dict[str, int] | None]:
        utility_service = getattr(self.bot, "utility_service", None)
        shield_service = getattr(self.bot, "shield_service", None)
        confessions_service = getattr(self.bot, "confessions_service", None)
        bump_channels = None
        if utility_service is not None and getattr(utility_service, "storage_ready", False):
            bump_summary = utility_service.get_bump_summary(guild_id)
            bump_channels = {
                "saved": int(bump_summary.get("saved_detection_channels", 0)),
                "active": int(bump_summary.get("active_detection_channels", 0)),
            }
        custom_patterns = None
        if shield_service is not None and getattr(shield_service, "storage_ready", False):
            saved_patterns = len(shield_service.get_config(guild_id).get("custom_patterns", []))
            custom_patterns = {
                "saved": int(saved_patterns),
                "active": min(int(saved_patterns), int(shield_service.custom_pattern_limit(guild_id))),
            }
        max_images = None
        if confessions_service is not None and getattr(confessions_service, "storage_ready", False):
            max_images = {
                "saved": int(confessions_service.get_config(guild_id).get("max_images", 3)),
                "active": int(confessions_service.get_compiled_config(guild_id).get("effective_max_images", 3)),
            }
        return {
            "bump_channels": bump_channels,
            "custom_patterns": custom_patterns,
            "max_images": max_images,
        }

    def _limit_line(
        self,
        *,
        label: str,
        current_count: dict[str, int] | None,
        limit_value: int,
        per_bucket: bool = False,
    ) -> str:
        if current_count is None:
            if per_bucket:
                return f"{label}: up to **{limit_value}** per bucket"
            return f"{label}: up to **{limit_value}**"
        saved_count = int(current_count.get("saved", 0))
        active_count = int(current_count.get("active", 0))
        line = f"{label}: {format_saved_state_status(saved_count=saved_count, active_count=active_count, limit_value=limit_value, per_bucket=per_bucket)}"
        over_limit = preserved_over_limit_note(saved_count=saved_count, active_count=active_count)
        if over_limit:
            line += f"\n{over_limit}"
        return line

    def _user_next_step(
        self,
        *,
        snapshot: dict[str, Any],
        link: dict[str, Any] | None,
        link_status: str,
        claimable: int,
        claim_count: int,
    ) -> str:
        if snapshot.get("blocked"):
            return "Premium is currently suspended on this Discord user. Use `/support` if that looks incorrect."
        if snapshot.get("system_access"):
            return "Internal operator access is already active. Use `/premium guild claim` only in servers that should consume an internal claim."
        if link is None and not snapshot.get("active_plans"):
            return (
                "Use `/premium subscribe` to open Patreon and choose Supporter, Babblebox Plus, "
                "or Babblebox Guild Pro. Then run `/premium link` here to connect that Patreon account. "
                "If the payment, billing, or refund itself looks wrong, start with Patreon or Apple first."
            )
        if link_status in {LINK_STATUS_REVOKED, LINK_STATUS_BROKEN}:
            return (
                "Use `/premium link` again with the same Patreon account that owns the Babblebox tier. "
                "If your Patreon tier changed recently and still looks wrong, refresh it first, then re-link."
            )
        if snapshot.get("stale"):
            return (
                "Use `/premium refresh` to ask Patreon for a fresh entitlement check. "
                "If it still looks wrong after refresh, use `/support` for live help."
            )
        if link is not None and not snapshot.get("active_plans") and claimable <= 0:
            return (
                "Your Patreon account is connected, but Babblebox did not find one of the three combined tiers on it yet. "
                "If you changed tiers recently, run `/premium refresh`, then use `/support` if it still looks wrong. "
                "If the charge, refund, or billing record looks wrong, start with Patreon or Apple first."
            )
        if claimable > 0:
            return (
                "If this was a Babblebox Guild Pro purchase, your personal lane can still read Free because Guild Pro is a server claim. "
                "Use `/premium guild claim` in the server you want to upgrade, then verify it with `/premium guild status` there."
            )
        if claim_count > 0:
            suffix = "" if claim_count == 1 else "s"
            return f"Your Guild Pro claim is already active on {claim_count} server{suffix}. Use `/premium guild status` there if you want to verify or release it."
        return "Everything looks active. Use `/premium refresh` if Patreon changed recently or `/premium plans` if you want to review the plan differences."

    def _guild_next_step(self, *, snapshot: dict[str, Any], guild_id: int) -> str:
        claim = snapshot.get("claim")
        if snapshot.get("blocked"):
            return "Guild premium is currently suspended on this server. Use `/support` if that looks incorrect."
        if snapshot.get("system_access"):
            return "This is the permanent Babblebox support server. No Guild Pro claim is needed here, and you can save your claim for another server."
        if claim is None and snapshot.get("plan_code") == PLAN_GUILD_PRO:
            return "This server is already covered by a direct Guild Pro grant. Use `/support` only if that looks incorrect."
        if claim is None:
            return (
                "If this server should use Guild Pro, buy Babblebox Guild Pro on Patreon, link Patreon on the owner account, "
                "then run `/premium guild claim` here."
            )
        if snapshot.get("stale"):
            return (
                "Guild Pro is still riding the last verified entitlement while the provider state is stale. "
                "Ask the claim owner to run `/premium refresh`, and use `/premium guild release` only if you want to move the claim."
            )
        owner_user_id = int(claim.get("owner_user_id", 0) or 0)
        return (
            f"Guild Pro is active here under <@{owner_user_id}>. "
            "Use `/premium guild release` only when you intentionally want to move this claim to another server."
        )

    def _status_view(self, *, link: dict[str, Any] | None, snapshot: dict[str, Any]) -> discord.ui.View:
        show_patreon = link is None or not snapshot.get("active_plans")
        return self._premium_actions_view(show_patreon=show_patreon)

    def _user_status_embed(self, user: discord.abc.User) -> discord.Embed:
        snapshot = self.service.get_user_snapshot(user.id)
        counts = self._utility_counts(user.id)
        link = self.service.get_link(user.id, provider=PROVIDER_PATREON)
        active_claims = self.service.list_cached_claims_for_user(user.id)
        link_status = str((link or {}).get("link_status") or "").strip().lower()
        link_reason = str(((link or {}).get("metadata") or {}).get("last_link_status_reason") or "").strip().lower()
        if link is None:
            link_label = "Not linked"
        elif link_status == LINK_STATUS_ACTIVE:
            link_label = "Connected"
        elif link_status == LINK_STATUS_REVOKED:
            link_label = "Reconnect required"
        elif link_status == LINK_STATUS_BROKEN:
            link_label = "Link needs repair"
        else:
            link_label = "Unavailable"
        embed = discord.Embed(
            title="Premium Status",
            description=(
                f"Personal plan: **{self.service.plan_title(snapshot['plan_code'])}**\n"
                f"Patreon link: **{link_label}**"
            ),
            color=ge.EMBED_THEME["accent"],
        )
        claimable = len(snapshot.get("claimable_sources", ()))
        access_lines = [f"Paid personal tier: **{self._personal_plan_text(snapshot)}**"]
        if snapshot.get("system_guild_claims") == "unlimited":
            access_lines.append("Guild Pro claim access: **Unlimited internal operator claims**")
        else:
            access_lines.append(f"Guild Pro claim sources ready: **{claimable}**")
        if active_claims:
            suffix = "" if len(active_claims) == 1 else "s"
            access_lines.append(f"Guild Pro already assigned: **{len(active_claims)} server{suffix}**")
        if link is not None and not snapshot.get("active_plans") and not snapshot.get("system_access") and claimable <= 0:
            access_lines.append("Resolved Babblebox tier: **No mapped Babblebox tier detected yet**")
        elif claimable > 0 and not snapshot.get("active_plans") and not snapshot.get("system_access"):
            access_lines.append("Resolved Babblebox Guild Pro access: **Available to claim in a server**")
        embed.add_field(name="Current Access", value="\n".join(access_lines), inline=False)
        notes: list[str] = []
        if snapshot.get("stale"):
            notes.append("Patreon data is stale. Babblebox is preserving the last verified entitlement until the grace window expires.")
        if snapshot.get("blocked"):
            notes.append("A manual premium suspension is active on this user.")
        if snapshot.get("system_access"):
            notes.append("This Discord user has permanent Babblebox operator premium access, including internal Guild Pro claim power.")
        if claimable > 0 and not snapshot.get("active_plans") and not snapshot.get("system_access"):
            notes.append("Babblebox Guild Pro is a server claim, so your personal Babblebox limits stay at Free until that claim is attached to a server.")
        if link is not None and not snapshot.get("active_plans") and not snapshot.get("system_access") and claimable <= 0:
            notes.append(self._mixed_campaign_note())
            notes.append("If the payment, refund, or billing record looks wrong, start with Patreon or Apple first. Use `/support` if the Babblebox tier still looks wrong after `/premium refresh`.")
        if link_status in {LINK_STATUS_REVOKED, LINK_STATUS_BROKEN}:
            if link_reason == "identity_provider_user_mismatch":
                notes.append("Patreon returned a different account than the one Babblebox previously linked. Re-link Patreon before provider-backed premium access can be trusted again.")
            else:
                notes.append("Patreon needs to be linked again before Babblebox can trust provider-backed premium access.")
        if notes:
            embed.add_field(name="Status Notes", value="\n".join(notes), inline=False)
        embed.add_field(
            name="Resolved Personal Limits",
            value="\n".join(
                (
                    self._limit_line(
                        label="Watch keywords",
                        current_count=counts["watch_keywords"],
                        limit_value=self.service.resolve_user_limit(user.id, LIMIT_WATCH_KEYWORDS),
                    ),
                    self._limit_line(
                        label="Watch filters",
                        current_count=counts["watch_filters"],
                        limit_value=self.service.resolve_user_limit(user.id, LIMIT_WATCH_FILTERS),
                        per_bucket=True,
                    ),
                    self._limit_line(
                        label="Active reminders",
                        current_count=counts["reminders"],
                        limit_value=self.service.resolve_user_limit(user.id, LIMIT_REMINDERS_ACTIVE),
                    ),
                    self._limit_line(
                        label="Channel reminders",
                        current_count=counts["public_reminders"],
                        limit_value=self.service.resolve_user_limit(user.id, LIMIT_REMINDERS_PUBLIC_ACTIVE),
                    ),
                    self._limit_line(
                        label="Recurring AFK schedules",
                        current_count=counts["afk_schedules"],
                        limit_value=self.service.resolve_user_limit(user.id, LIMIT_AFK_SCHEDULES),
                    ),
                )
            ),
            inline=False,
        )
        if link is not None:
            display_name = link.get("display_name") or "Linked Patreon account"
            email = link.get("email") or "No email returned"
            embed.add_field(name="Linked Account", value=f"{display_name}\n{email}", inline=False)
        embed.add_field(
            name="Next Step",
            value=self._user_next_step(
                snapshot=snapshot,
                link=link,
                link_status=link_status,
                claimable=claimable,
                claim_count=len(active_claims),
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Premium | /premium plans, link, refresh, support")

    def _guild_status_embed(self, guild: discord.Guild) -> discord.Embed:
        snapshot = self.service.get_guild_snapshot(guild.id)
        counts = self._guild_feature_counts(guild.id)
        claim = snapshot.get("claim")
        if snapshot.get("blocked"):
            claim_state = "Blocked"
        elif snapshot.get("system_access"):
            claim_state = "Permanent support-guild access"
        elif claim is None and snapshot.get("plan_code") == PLAN_GUILD_PRO:
            claim_state = "Manual Guild Pro grant"
        elif claim is None:
            claim_state = "Unclaimed"
        elif snapshot.get("stale"):
            claim_state = "Claim active, provider stale"
        else:
            claim_state = "Claim active"
        embed = discord.Embed(
            title="Guild Premium Status",
            description=(
                f"Current server plan: **{self.service.plan_title(snapshot['plan_code'])}**\n"
                f"Claim state: **{claim_state}**"
            ),
            color=ge.EMBED_THEME["accent"],
        )
        claim_lines: list[str] = [f"Active plans: **{self._active_plan_text(snapshot)}**"]
        if snapshot.get("system_access"):
            claim_lines.append("This server has permanent Babblebox operator premium.")
            if claim is not None:
                claim_lines.append(f"Stored claim owner: <@{int(claim.get('owner_user_id', 0))}>")
                claim_lines.append("Stored claim is not required for support-guild premium.")
        elif snapshot.get("blocked"):
            claim_lines.append("A manual premium suspension is active on this guild.")
        elif claim is None and snapshot.get("plan_code") == PLAN_GUILD_PRO:
            claim_lines.append("This server is covered by a direct Guild Pro grant instead of a user-owned claim.")
        elif claim is None:
            claim_lines.append("No Guild Pro claim is attached to this server.")
        else:
            claim_lines.append(f"Claim owner: <@{int(claim.get('owner_user_id', 0))}>")
            claim_lines.append(f"Source: `{claim.get('source_kind')}`")
            claim_lines.append(f"Claimed at: {claim.get('claimed_at') or 'Unknown'}")
            if claim.get("note"):
                claim_lines.append(f"Note: {claim.get('note')}")
        if snapshot.get("stale"):
            claim_lines.append("The provider-backed source is stale, so Babblebox is holding the last verified claim inside its grace window.")
        embed.add_field(name="Claim Summary", value="\n".join(claim_lines), inline=False)
        embed.add_field(
            name="What Guild Pro Changes Here",
            value="\n".join(
                (
                    self._limit_line(
                        label="Bump detection channels",
                        current_count=counts["bump_channels"],
                        limit_value=self.service.resolve_guild_limit(guild.id, LIMIT_BUMP_DETECTION_CHANNELS),
                    ),
                    self._limit_line(
                        label="Shield advanced patterns",
                        current_count=counts["custom_patterns"],
                        limit_value=self.service.resolve_guild_limit(guild.id, LIMIT_SHIELD_CUSTOM_PATTERNS),
                    ),
                    self._limit_line(
                        label="Confession images",
                        current_count=counts["max_images"],
                        limit_value=self.service.resolve_guild_limit(guild.id, LIMIT_CONFESSIONS_MAX_IMAGES),
                    ),
                    "Shield filters, allowlists, exemptions, and severe-term ceilings also rise on Guild Pro.",
                    (
                        "Higher Shield AI tiers: Available on this server when owner policy and provider/runtime readiness allow review"
                        if self.service.guild_has_capability(guild.id, CAPABILITY_SHIELD_AI_REVIEW)
                        else "Higher Shield AI tiers: Require Babblebox Guild Pro"
                    ),
                    (
                        "Question Drops AI celebrations: Available on this server when celebration policy and provider/runtime readiness allow live copy"
                        if self.service.guild_has_capability(guild.id, CAPABILITY_QUESTION_DROPS_AI_CELEBRATIONS)
                        else "Question Drops AI celebrations: Require Babblebox Guild Pro"
                    ),
                )
            ),
            inline=False,
        )
        embed.add_field(name="Next Step", value=self._guild_next_step(snapshot=snapshot, guild_id=guild.id), inline=False)
        return ge.style_embed(embed, footer="Babblebox Premium | /premium guild claim, release, support")

    def _plans_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Premium Plans",
            description="Free stays genuinely useful. Premium adds clearer headroom and explicit server power without paywalling Babblebox's core trust surfaces.",
            color=ge.EMBED_THEME["info"],
        )
        for plan in self.service.plan_catalog():
            lines = [
                f"Who it's for: {plan['audience']}",
                f"What it unlocks: {plan['summary']}",
                "Unlocks:",
                *(f"- {entry}" for entry in plan.get("unlocks", ())),
                "Does not unlock:",
                *(f"- {entry}" for entry in plan.get("does_not_unlock", ())),
                f"Best for: {plan['best_for']}",
            ]
            embed.add_field(name=plan["title"], value="\n".join(lines), inline=False)
        embed.add_field(
            name="Free Stays Useful",
            value=(
                "Free keeps the baseline utility lane, core Shield privacy and safety behavior, and the current bounded Confessions baseline.\n"
                "Supporter keeps the same Babblebox product limits as Free, but it still includes Supporter-tier Inevitable Friendship Discord benefits.\n"
                "Downgrade never deletes saved Watch, reminder, AFK, Shield, or Confessions state; premium-only runtime capacity simply pauses until the saved state is reduced or premium returns."
            ),
            inline=False,
        )
        embed.add_field(
            name="How Premium Works",
            value=(
                "1. Use `/premium subscribe` to open Patreon and choose Supporter, Babblebox Plus, or Babblebox Guild Pro.\n"
                "2. Run `/premium link` in Discord so Babblebox can connect that Patreon account to your Discord user.\n"
                "3. Use `/premium status` to confirm the linked personal plan and any Guild Pro claim-ready state.\n"
                "4. If you bought Guild Pro, use `/premium guild claim` in the server you want to upgrade, then verify it with `/premium guild status`."
            ),
            inline=False,
        )
        embed.add_field(
            name="Patreon Tier Mapping",
            value=(
                f"{self._mixed_campaign_note()}\n"
                "If a linked Patreon account still shows Free after a recent tier change, the membership probably has not refreshed into Discord yet. "
                "Run `/premium refresh` or use `/support` if it still looks wrong."
            ),
            inline=False,
        )
        embed.add_field(
            name="Payment / Refund Routing",
            value=(
                "Babblebox does not process cards or reverse Patreon or Apple charges directly. "
                "Start payment, billing, duplicate-charge, unauthorized-charge, or refund issues with Patreon, or with Apple for iOS purchases. "
                "Refund outcomes follow Patreon or Apple policy and applicable law, not a separate Babblebox guarantee. "
                f"Use `/support` for Babblebox entitlement, linking, or Guild Pro claim issues. Patreon: {PATREON_REFUND_POLICY_URL} Apple: {APPLE_REFUND_URL}"
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Premium | Buy on Patreon, link in Discord, claim Guild Pro where needed")

    def _refresh_result_embed(self, user: discord.abc.User, *, ok: bool, message: str) -> discord.Embed:
        if ok:
            embed = self._user_status_embed(user)
            embed.title = "Premium Refresh"
            embed.insert_field_at(
                0,
                name="Refresh Result",
                value="Babblebox rechecked the linked Patreon account, refreshed the cached entitlement snapshot, and rebuilt the resolved premium state.",
                inline=False,
            )
            return embed
        if "No Patreon account is linked" in message:
            return ge.make_status_embed(
                "No Patreon Link",
                "Babblebox cannot refresh Patreon until this Discord user is linked. Buy the Babblebox tier on Patreon first if needed, then run `/premium link`.",
                tone="warning",
                footer="Babblebox Premium",
            )
        if "linked again" in message:
            return ge.make_status_embed(
                "Re-link Required",
                "Babblebox cannot safely trust the saved Patreon link anymore. Use `/premium link` again with the same Patreon account that owns the Babblebox tier.",
                tone="warning",
                footer="Babblebox Premium",
            )
        return ge.make_status_embed(
            "Premium Refresh Review",
            (
                f"{message} Use `/support` for Babblebox entitlement help if the linked account still looks wrong after refresh. "
                "If the payment, billing, or refund itself looks wrong, start with Patreon or Apple first."
            ),
            tone="warning",
            footer="Babblebox Premium",
        )

    def _unlink_result_embed(self, *, ok: bool, message: str) -> discord.Embed:
        if ok:
            return ge.make_status_embed(
                "Patreon Unlinked",
                (
                    "Babblebox deleted the local encrypted Patreon tokens for this Discord user and withdrew provider-backed premium access. "
                    "Saved Watch, reminder, AFK, Shield, and Confessions configuration stays preserved. "
                    "The Patreon or Apple billing relationship, if any, still lives with that provider, and you can use `/premium link` again if needed."
                ),
                tone="success",
                footer="Babblebox Premium",
            )
        if "No Patreon account is linked" in message:
            return ge.make_status_embed(
                "Nothing To Unlink",
                "There is no Patreon link on this Discord user right now. Use `/premium status` to confirm the current premium state.",
                tone="warning",
                footer="Babblebox Premium",
            )
        return ge.make_status_embed("Premium Unlink", message, tone="warning", footer="Babblebox Premium")

    def _claim_result_embed(self, guild: discord.Guild, *, ok: bool, message: str) -> discord.Embed:
        if ok:
            embed = self._guild_status_embed(guild)
            claim = self.service.get_guild_snapshot(guild.id).get("claim") or {}
            source_kind = str(claim.get("source_kind") or "unknown")
            embed.title = "Guild Pro Claimed"
            embed.insert_field_at(
                0,
                name="Claim Result",
                value=(
                    "Guild Pro is now attached to this server.\n"
                    f"Claim source: `{source_kind}`\n"
                    "Use `/premium guild status` any time you want to verify the active claim and the unlocked server surfaces."
                ),
                inline=False,
            )
            return embed
        if "No unclaimed Guild Pro entitlement" in message:
            detail = "Babblebox could not find a free Guild Pro source on this user. Buy Babblebox Guild Pro on Patreon, link Patreon, or release an existing claim first."
        elif "already uses one of your Guild Pro claims" in message:
            detail = "This server is already using one of your Guild Pro claims. Use `/premium guild status` to verify the current claim."
        elif "already has an active Guild Pro claim" in message:
            detail = "This server already has a Guild Pro claim under another owner. Only that claim owner can release it."
        elif "support server" in message.casefold():
            detail = "The Babblebox support server keeps permanent premium and never needs a normal Guild Pro claim."
        else:
            detail = message
        return ge.make_status_embed("Guild Pro Claim", detail, tone="warning", footer="Babblebox Premium")

    def _release_result_embed(self, guild: discord.Guild, *, ok: bool, message: str) -> discord.Embed:
        if ok:
            return ge.make_status_embed(
                "Guild Pro Released",
                (
                    "Guild Pro stopped for this server, but Babblebox preserved the saved server configuration. "
                    "That claim can now be used somewhere else. Use `/premium guild status` to confirm the current server state before moving on."
                ),
                tone="success",
                footer="Babblebox Premium",
            )
        if "does not have an active Guild Pro claim" in message:
            detail = "This server does not currently have an active Guild Pro claim to release."
        elif "Only the claim owner can release" in message:
            detail = "Only the person who claimed this Guild Pro slot can release it from the server."
        elif "support server" in message.casefold():
            detail = "The Babblebox support server keeps permanent premium. Releasing a stored claim there never removes the support server's own access."
        else:
            detail = message
        return ge.make_status_embed("Guild Pro Release", detail, tone="warning", footer="Babblebox Premium")

    def _admin_status_embed(self, *, title: str, note: str, user_id: int | None = None, guild_id: int | None = None) -> discord.Embed:
        diagnostics = self.service.provider_diagnostics()
        embed = discord.Embed(title=title, description=note, color=ge.EMBED_THEME["info"])
        embed.add_field(
            name="Runtime",
            value=(
                f"Storage ready: {'Yes' if diagnostics['storage_ready'] else 'No'}\n"
                f"Backend: `{diagnostics['storage_backend']}`\n"
                f"Patreon configured: {'Yes' if diagnostics['patreon_configured'] else 'No'}\n"
                f"Sync-ready: {'Yes' if diagnostics['patreon_sync_ready'] else 'No'}\n"
                f"Crypto source: `{diagnostics['crypto_source']}`"
            ),
            inline=False,
        )
        config_errors = tuple(diagnostics.get("patreon_config_errors", ()))
        if config_errors:
            embed.add_field(name="Patreon Config", value="\n".join(config_errors[:4]), inline=False)
        embed.add_field(
            name="Cache",
            value=(
                f"Links: **{diagnostics['link_count']}**\n"
                f"Entitlements: **{diagnostics['entitlement_count']}**\n"
                f"Active guild claims: **{diagnostics['active_claim_count']}**"
            ),
            inline=False,
        )
        if diagnostics.get("storage_error"):
            embed.add_field(name="Storage Error", value=str(diagnostics["storage_error"]), inline=False)
        runtime_lines = runtime_service_lines(self.bot)
        if runtime_lines:
            embed.add_field(name="Bot Services", value="\n".join(runtime_lines), inline=False)
        webhook_stats = diagnostics.get("provider_monitor") or {}
        if webhook_stats:
            embed.add_field(
                name="Patreon Webhooks",
                value=(
                    f"Observed invalid signatures: **{webhook_stats['invalid_signature_count']}**\n"
                    f"Stored unresolved review items: **{webhook_stats['unresolved_issue_count']}**\n"
                    f"Observed 503 responses: **{webhook_stats['recent_unavailable_count']}**\n"
                    f"Observed 5xx responses: **{webhook_stats['recent_server_error_count']}**\n"
                    f"Last: `{webhook_stats['last_webhook_status'] or 'none'}` / `{webhook_stats['last_webhook_http_status'] or 'n/a'}`"
                ),
                inline=False,
            )
        if user_id is not None:
            snapshot = self.service.get_user_snapshot(user_id)
            embed.add_field(
                name="User Snapshot",
                value=(
                    f"User: `{user_id}`\n"
                    f"Plan: `{snapshot['plan_code']}`\n"
                    f"Active plans: {', '.join(snapshot['active_plans']) or 'none'}\n"
                    f"Claimable sources: {len(snapshot.get('claimable_sources', ()))}\n"
                    f"Blocked: {'Yes' if snapshot.get('blocked') else 'No'}"
                ),
                inline=False,
            )
        if guild_id is not None:
            snapshot = self.service.get_guild_snapshot(guild_id)
            embed.add_field(
                name="Guild Snapshot",
                value=(
                    f"Guild: `{guild_id}`\n"
                    f"Plan: `{snapshot['plan_code']}`\n"
                    f"Active plans: {', '.join(snapshot['active_plans']) or 'none'}\n"
                    f"Blocked: {'Yes' if snapshot.get('blocked') else 'No'}\n"
                    f"Claim attached: {'Yes' if snapshot.get('claim') else 'No'}"
                ),
                inline=False,
            )
        return ge.style_embed(embed, footer="Babblebox Premium | DM-only maintainer control")

    @commands.hybrid_group(
        name="premium",
        with_app_command=True,
        description="View premium status, plans, linking, and server claims",
        invoke_without_command=True,
    )
    async def premium_group(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        snapshot = self.service.get_user_snapshot(ctx.author.id)
        link = self.service.get_link(ctx.author.id, provider=PROVIDER_PATREON)
        await self._send_private(ctx, embed=self._user_status_embed(ctx.author), view=self._status_view(link=link, snapshot=snapshot))

    @premium_group.command(name="status", with_app_command=True, description="View your current premium status")
    async def premium_status_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        snapshot = self.service.get_user_snapshot(ctx.author.id)
        link = self.service.get_link(ctx.author.id, provider=PROVIDER_PATREON)
        await self._send_private(ctx, embed=self._user_status_embed(ctx.author), view=self._status_view(link=link, snapshot=snapshot))

    @premium_group.command(name="plans", with_app_command=True, description="See what each Babblebox premium plan unlocks")
    async def premium_plans_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        await self._send_private(ctx, embed=self._plans_embed(), view=self._subscribe_view())

    @premium_group.command(name="subscribe", with_app_command=True, description="Open the Patreon page where Babblebox premium is purchased")
    async def premium_subscribe_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        embed = discord.Embed(
            title="Subscribe on Patreon",
            description=(
                "Patreon is where Babblebox premium is purchased. Discord linking is the second step, not the purchase step."
            ),
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Choose The Right Tier",
            value=(
                "`Supporter` backs the project, includes the Supporter-tier Inevitable Friendship Discord benefits, and keeps Babblebox at Free limits.\n"
                "`Babblebox Plus` maps to IF Epic Patron and raises personal Watch, reminder, and recurring AFK limits.\n"
                "`Babblebox Guild Pro` maps to IF Legendary Patron and is the server plan for higher caps, higher Shield AI tiers when owner policy and provider/runtime readiness allow review, optional Question Drops AI celebrations when celebration policy and provider/runtime readiness allow live copy, and the larger safe Confessions image ceiling."
            ),
            inline=False,
        )
        embed.add_field(
            name="Before You Buy",
            value=(
                "Babblebox does not process cards and cannot reverse Patreon or Apple charges directly. "
                "Start payment, billing, duplicate-charge, unauthorized-charge, or refund issues with Patreon, or with Apple for iOS purchases. "
                "Refund outcomes follow Patreon or Apple policy and applicable law, not a separate Babblebox guarantee. "
                f"Patreon policy: {PATREON_REFUND_POLICY_URL} Patreon refund help: {PATREON_REFUND_REQUEST_URL} Apple: {APPLE_REFUND_URL} "
                "Terms: https://arno-create.github.io/babblebox-bot/terms.html"
            ),
            inline=False,
        )
        embed.add_field(
            name="After You Buy",
            value=(
                "1. Run `/premium link` in Discord with the same Patreon account that owns the Babblebox tier.\n"
                "2. Run `/premium status` to confirm the linked personal plan and any Guild Pro claim-ready state.\n"
                "3. If you bought Guild Pro, run `/premium guild claim` inside the server you want to upgrade."
            ),
            inline=False,
        )
        embed.add_field(
            name="Need Help?",
            value=(
                "Use `/premium plans` to compare the plans again. Use `/support` if the Babblebox link, resolved tier, or Guild Pro claim still feels wrong after purchase. "
                f"For payment or refund issues, start with Patreon or Apple first. Patreon: {PATREON_REFUND_REQUEST_URL} Apple: {APPLE_REFUND_URL}"
            ),
            inline=False,
        )
        embed = ge.style_embed(embed, footer="Babblebox Premium | Buy first, then link in Discord")
        await self._send_private(ctx, embed=embed, view=self._subscribe_view())

    @premium_group.command(name="link", with_app_command=True, description="Start Patreon linking for this Discord user")
    async def premium_link_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        ok, result = await self.service.create_link_url(ctx.author.id)
        if not ok:
            await self._send_private(
                ctx,
                embed=ge.make_status_embed("Premium Link", result, tone="warning", footer="Babblebox Premium"),
                view=self._premium_actions_view(show_patreon=True),
            )
            return
        embed = discord.Embed(
            title="Link Patreon to Babblebox",
            description="This step connects a Patreon purchase to your Discord user. It does not buy the tier for you.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="What Linking Does",
            value=(
                "Babblebox opens Patreon authorization, finishes the bind on the callback page, and then refreshes your Babblebox premium entitlements."
            ),
            inline=False,
        )
        embed.add_field(
            name="Payment Boundary",
            value=(
                "Linking does not buy the tier, cancel the subscription, or reverse charges. "
                "Payment, billing, and refund issues start with Patreon, or with Apple for iOS purchases. "
                "Refund outcomes follow Patreon or Apple policy and applicable law, not a separate Babblebox guarantee. "
                "Babblebox support helps with entitlement, linking, and Guild Pro claim questions."
            ),
            inline=False,
        )
        embed.add_field(
            name="Use The Right Patreon Account",
            value=(
                "Use the same Patreon account that owns Supporter, Babblebox Plus, or Babblebox Guild Pro."
            ),
            inline=False,
        )
        embed.add_field(
            name="After Success",
            value=(
                "Run `/premium status` to confirm the linked personal plan and any Guild Pro claim-ready state. If you bought Guild Pro, continue in the target server with `/premium guild claim`."
            ),
            inline=False,
        )
        embed.add_field(
            name="Privacy Boundary",
            value=(
                "Babblebox stores local encrypted Patreon tokens so it can refresh the link later. `/premium unlink` deletes those local tokens without pretending it revoked Patreon-side app access for you."
            ),
            inline=False,
        )
        embed.add_field(name="Link Session", value="The Patreon authorization link expires in about 15 minutes.", inline=False)
        embed = ge.style_embed(embed, footer="Babblebox Premium | Link Patreon, then verify with /premium status")
        await self._send_private(ctx, embed=embed, view=self._premium_actions_view(auth_url=result))

    @premium_group.command(name="refresh", with_app_command=True, description="Refresh Patreon-backed premium entitlements")
    async def premium_refresh_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        ok, message = await self.service.refresh_user_link(ctx.author.id)
        snapshot = self.service.get_user_snapshot(ctx.author.id)
        link = self.service.get_link(ctx.author.id, provider=PROVIDER_PATREON)
        await self._send_private(
            ctx,
            embed=self._refresh_result_embed(ctx.author, ok=ok, message=message),
            view=self._status_view(link=link, snapshot=snapshot),
        )

    @premium_group.command(name="unlink", with_app_command=True, description="Unlink Patreon from this Discord user")
    async def premium_unlink_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        ok, message = await self.service.unlink_user(ctx.author.id)
        await self._send_private(
            ctx,
            embed=self._unlink_result_embed(ok=ok, message=message),
            view=self._premium_actions_view(show_patreon=True),
        )

    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
    @premium_group.group(name="guild", with_app_command=True, description="Manage Guild Pro for this server", invoke_without_command=True)
    async def premium_guild_group(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        await self._send_private(ctx, embed=self._guild_status_embed(ctx.guild), view=self._premium_actions_view(show_patreon=True))

    @premium_guild_group.command(name="status", with_app_command=True, description="See this server's Guild Pro status")
    async def premium_guild_status_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        await self._send_private(ctx, embed=self._guild_status_embed(ctx.guild), view=self._premium_actions_view(show_patreon=True))

    @premium_guild_group.command(name="claim", with_app_command=True, description="Assign one of your Guild Pro claims to this server")
    async def premium_guild_claim_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        ok, message = await self.service.claim_guild(guild=ctx.guild, actor=ctx.author)
        await self._send_private(
            ctx,
            embed=self._claim_result_embed(ctx.guild, ok=ok, message=message),
            view=self._premium_actions_view(show_patreon=not ok),
        )

    @premium_guild_group.command(name="release", with_app_command=True, description="Release Guild Pro from this server")
    async def premium_guild_release_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        ok, message = await self.service.release_guild(guild=ctx.guild, actor=ctx.author)
        await self._send_private(
            ctx,
            embed=self._release_result_embed(ctx.guild, ok=ok, message=message),
            view=self._premium_actions_view(show_patreon=not ok),
        )

    @commands.command(name="premiumadmin", hidden=True)
    async def premium_admin_command(self, ctx: commands.Context, *parts: str):
        if ctx.guild is not None:
            await ctx.send(content="That command is only available in DM.")
            return
        author_id = getattr(ctx.author, "id", 0)
        if not self._is_override_owner(author_id):
            LOGGER.warning(
                "Premium owner command denied: unauthorized_dm_user_id=%s",
                author_id,
            )
            await ctx.send(content="That command is unavailable.")
            return
        tokens = [str(part).strip() for part in parts if str(part).strip()]
        if not tokens:
            tokens = ["status"]
        root = tokens[0].casefold()
        usage = (
            "Use `status`, `status user <id>`, `status guild <id>`, "
            "`grant user|guild <id> <supporter|plus|guild_pro> [reason]`, "
            "`block user|guild <id> [reason]`, `unblock user|guild <id>`, "
            "`revoke <override_id>`, or `refresh <user_id>`."
        )
        if root == "status":
            if len(tokens) == 1:
                await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note="Private maintainer diagnostics for premium storage, Patreon readiness, and cached claims."))
                return
            if len(tokens) >= 3 and tokens[1].casefold() in {"user", "guild"}:
                try:
                    target_id = int(tokens[2])
                except ValueError:
                    await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note="Target IDs must be numeric."))
                    return
                if tokens[1].casefold() == "user":
                    await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note="User premium snapshot.", user_id=target_id))
                    return
                await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note="Guild premium snapshot.", guild_id=target_id))
                return
            await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note=usage))
            return
        if root == "grant":
            if len(tokens) < 4:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=usage))
                return
            target_type = tokens[1].casefold()
            try:
                target_id = int(tokens[2])
            except ValueError:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note="Target IDs must be numeric."))
                return
            plan_code = tokens[3].casefold()
            if target_type not in {SCOPE_USER, SCOPE_GUILD}:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note="Grant targets must be `user` or `guild`."))
                return
            if plan_code not in {"supporter", "plus", "guild_pro"}:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note="Plan codes must be supporter, plus, or guild_pro."))
                return
            reason = " ".join(tokens[4:]) or None
            try:
                record = await self.service.create_manual_override(
                    target_type=target_type,
                    target_id=target_id,
                    kind=MANUAL_KIND_GRANT,
                    plan_code=plan_code,
                    actor_user_id=author_id,
                    reason=reason,
                )
            except (PremiumStorageUnavailable, ValueError) as exc:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=f"Grant failed: {exc}"))
                return
            await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=f"Manual grant `{record['override_id']}` saved for {target_type} `{target_id}` at `{plan_code}`."))
            return
        if root == "block":
            if len(tokens) < 3:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=usage))
                return
            target_type = tokens[1].casefold()
            try:
                target_id = int(tokens[2])
            except ValueError:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note="Target IDs must be numeric."))
                return
            if target_type not in {SCOPE_USER, SCOPE_GUILD}:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note="Block targets must be `user` or `guild`."))
                return
            reason = " ".join(tokens[3:]) or None
            try:
                record = await self.service.create_manual_override(
                    target_type=target_type,
                    target_id=target_id,
                    kind=MANUAL_KIND_BLOCK,
                    plan_code=None,
                    actor_user_id=author_id,
                    reason=reason,
                )
            except (PremiumStorageUnavailable, ValueError) as exc:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=f"Block failed: {exc}"))
                return
            await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=f"Premium suspension `{record['override_id']}` is active for {target_type} `{target_id}`."))
            return
        if root == "unblock":
            if len(tokens) < 3:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=usage))
                return
            target_type = tokens[1].casefold()
            try:
                target_id = int(tokens[2])
            except ValueError:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note="Target IDs must be numeric."))
                return
            ok, message = await self.service.clear_block_overrides(target_type=target_type, target_id=target_id, actor_user_id=author_id)
            await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=message))
            return
        if root == "revoke":
            if len(tokens) < 2:
                await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=usage))
                return
            ok, message = await self.service.deactivate_override(tokens[1], actor_user_id=author_id)
            await ctx.send(embed=self._admin_status_embed(title="Premium Override", note=message if ok else f"Update failed: {message}"))
            return
        if root == "refresh":
            if len(tokens) < 2:
                await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note=usage))
                return
            try:
                user_id = int(tokens[1])
            except ValueError:
                await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note="User IDs must be numeric."))
                return
            ok, message = await self.service.refresh_user_link(user_id)
            await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note=message if ok else f"Refresh failed: {message}", user_id=user_id))
            return
        await ctx.send(embed=self._admin_status_embed(title="Premium Owner Status", note=usage))


async def setup(bot: commands.Bot):
    await bot.add_cog(PremiumCog(bot))
