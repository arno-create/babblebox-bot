from __future__ import annotations

from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import send_hybrid_response
from babblebox.premium_limits import (
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
from babblebox.premium_models import MANUAL_KIND_BLOCK, MANUAL_KIND_GRANT, PLAN_GUILD_PRO, PROVIDER_PATREON, SCOPE_GUILD, SCOPE_USER
from babblebox.premium_service import PremiumService


PREMIUM_OVERRIDE_OWNER_IDS = {1266444952779620413, 1345860619836063754}


class PremiumCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = PremiumService(bot)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "premium_service", self.service)

    def cog_unload(self):
        if getattr(self.bot, "premium_service", None) is self.service:
            delattr(self.bot, "premium_service")
        self.bot.loop.create_task(self.service.close())

    def _is_override_owner(self, user_id: int) -> bool:
        return user_id in PREMIUM_OVERRIDE_OWNER_IDS

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

    def _utility_counts(self, user_id: int) -> dict[str, int | None]:
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
        reminders = utility_service.list_reminders(user_id)
        watch_filter_count = sum(
            len(summary.get(key, ()))
            for key in ("mention_channel_ids", "reply_channel_ids", "ignored_channel_ids", "ignored_user_ids")
        )
        return {
            "watch_keywords": int(summary.get("total_keywords", 0)),
            "watch_filters": int(watch_filter_count),
            "reminders": len(reminders),
            "public_reminders": len([item for item in reminders if item.get("delivery") == "here"]),
            "afk_schedules": len(utility_service.list_afk_schedules(user_id)),
        }

    def _guild_feature_counts(self, guild_id: int) -> dict[str, int | None]:
        utility_service = getattr(self.bot, "utility_service", None)
        shield_service = getattr(self.bot, "shield_service", None)
        confessions_service = getattr(self.bot, "confessions_service", None)
        bump_channels = None
        if utility_service is not None and getattr(utility_service, "storage_ready", False):
            bump_channels = len(utility_service.get_bump_config(guild_id).get("detection_channel_ids", []))
        custom_patterns = None
        if shield_service is not None and getattr(shield_service, "storage_ready", False):
            custom_patterns = len(shield_service.get_config(guild_id).get("custom_patterns", []))
        max_images = None
        if confessions_service is not None and getattr(confessions_service, "storage_ready", False):
            max_images = int(confessions_service.get_config(guild_id).get("max_images", 3))
        return {
            "bump_channels": bump_channels,
            "custom_patterns": custom_patterns,
            "max_images": max_images,
        }

    def _limit_line(self, *, label: str, current_count: int | None, limit_value: int) -> str:
        if current_count is None:
            return f"{label}: up to **{limit_value}**"
        line = f"{label}: **{current_count} / {limit_value}**"
        over_limit = self.service.over_limit_label(current_count=current_count, limit_value=limit_value)
        if over_limit:
            line += f"\n{over_limit}"
        return line

    def _user_status_embed(self, user: discord.abc.User) -> discord.Embed:
        snapshot = self.service.get_user_snapshot(user.id)
        counts = self._utility_counts(user.id)
        link = self.service.get_link(user.id, provider=PROVIDER_PATREON)
        embed = discord.Embed(
            title="Premium Status",
            description=(
                f"Plan: **{self.service.plan_title(snapshot['plan_code'])}**\n"
                f"Active plans: {self._active_plan_text(snapshot)}\n"
                f"Patreon link: {'Connected' if link is not None else 'Not linked'}"
            ),
            color=ge.EMBED_THEME["accent"],
        )
        notes: list[str] = []
        if snapshot.get("stale"):
            notes.append("Patreon data is stale. Babblebox is preserving the last verified entitlement until grace expires.")
        if snapshot.get("blocked"):
            notes.append("A manual premium suspension is active on this user.")
        if notes:
            embed.add_field(name="State", value="\n".join(notes), inline=False)
        embed.add_field(
            name="Plus Utility Limits",
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
        claimable = len(snapshot.get("claimable_sources", ()))
        embed.add_field(
            name="Guild Pro Claims",
            value=f"Available claim units: **{claimable}**",
            inline=False,
        )
        if link is not None:
            display_name = link.get("display_name") or "Linked Patreon account"
            email = link.get("email") or "No email returned"
            embed.add_field(name="Linked Account", value=f"{display_name}\n{email}", inline=False)
        return ge.style_embed(embed, footer="Babblebox Premium | /premium link, refresh, unlink")

    def _guild_status_embed(self, guild: discord.Guild) -> discord.Embed:
        snapshot = self.service.get_guild_snapshot(guild.id)
        counts = self._guild_feature_counts(guild.id)
        embed = discord.Embed(
            title="Guild Premium Status",
            description=(
                f"Server plan: **{self.service.plan_title(snapshot['plan_code'])}**\n"
                f"Active plans: {self._active_plan_text(snapshot)}"
            ),
            color=ge.EMBED_THEME["accent"],
        )
        claim = snapshot.get("claim")
        claim_lines = []
        if claim is None:
            claim_lines.append("No Guild Pro claim is attached to this server.")
        else:
            claim_lines.append(f"Claim owner: <@{int(claim.get('owner_user_id', 0))}>")
            claim_lines.append(f"Source: `{claim.get('source_kind')}`")
            claim_lines.append(f"Claimed at: {claim.get('claimed_at') or 'Unknown'}")
        if snapshot.get("stale"):
            claim_lines.append("Provider-backed status is stale and currently riding the grace window.")
        if snapshot.get("blocked"):
            claim_lines.append("A manual premium suspension is active on this guild.")
        embed.add_field(name="Claim", value="\n".join(claim_lines), inline=False)
        embed.add_field(
            name="Guild Pro Surfaces",
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
                    f"Shield AI review: {'Unlocked' if self.service.guild_has_capability(guild.id, CAPABILITY_SHIELD_AI_REVIEW) else 'Requires Guild Pro'}",
                )
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Premium | /premium guild claim or release")

    def _plans_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Premium Plans",
            description="Babblebox keeps core safety, privacy, and baseline utilities free. Premium expands limits and advanced admin power.",
            color=ge.EMBED_THEME["info"],
        )
        for plan in self.service.plan_catalog():
            embed.add_field(name=plan["title"], value=plan["summary"], inline=False)
        embed.add_field(
            name="Current Premium Hooks",
            value=(
                "Plus: higher Watch, reminder, and recurring AFK limits.\n"
                "Guild Pro: more bump detection channels, larger Shield caps, Shield AI review eligibility, and a higher Confessions image ceiling."
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Premium | Patreon-backed entitlements with safe manual overrides")

    def _admin_status_embed(self, *, title: str, note: str, user_id: int | None = None, guild_id: int | None = None) -> discord.Embed:
        diagnostics = self.service.provider_diagnostics()
        embed = discord.Embed(title=title, description=note, color=ge.EMBED_THEME["info"])
        embed.add_field(
            name="Runtime",
            value=(
                f"Storage ready: {'Yes' if diagnostics['storage_ready'] else 'No'}\n"
                f"Backend: `{diagnostics['storage_backend']}`\n"
                f"Patreon configured: {'Yes' if diagnostics['patreon_configured'] else 'No'}\n"
                f"Automation-ready: {'Yes' if diagnostics['patreon_automation_ready'] else 'No'}\n"
                f"Crypto source: `{diagnostics['crypto_source']}`"
            ),
            inline=False,
        )
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
        await self._send_private(ctx, embed=self._user_status_embed(ctx.author))

    @premium_group.command(name="status", with_app_command=True, description="View your current premium status")
    async def premium_status_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        await self._send_private(ctx, embed=self._user_status_embed(ctx.author))

    @premium_group.command(name="plans", with_app_command=True, description="See what each Babblebox premium plan unlocks")
    async def premium_plans_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        await self._send_private(ctx, embed=self._plans_embed())

    @premium_group.command(name="link", with_app_command=True, description="Start Patreon linking for this Discord user")
    async def premium_link_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        ok, result = await self.service.create_link_url(ctx.author.id)
        if not ok:
            await self._send_result(ctx, title="Premium Link", message=result, ok=False)
            return
        view = discord.ui.View()
        view.add_item(discord.ui.Button(label="Open Patreon", url=result))
        embed = ge.make_status_embed(
            "Premium Link Ready",
            "Open Patreon to authorize Babblebox. The link expires in about 15 minutes, and Babblebox will finish the bind on the callback page.",
            tone="info",
            footer="Babblebox Premium",
        )
        await self._send_private(ctx, embed=embed, view=view)

    @premium_group.command(name="refresh", with_app_command=True, description="Refresh Patreon-backed premium entitlements")
    async def premium_refresh_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        ok, message = await self.service.refresh_user_link(ctx.author.id)
        await self._send_result(ctx, title="Premium Refresh", message=message, ok=ok)

    @premium_group.command(name="unlink", with_app_command=True, description="Unlink Patreon from this Discord user")
    async def premium_unlink_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx):
            return
        ok, message = await self.service.unlink_user(ctx.author.id)
        await self._send_result(ctx, title="Premium Unlink", message=message, ok=ok)

    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
    @premium_group.group(name="guild", with_app_command=True, description="Manage Guild Pro for this server", invoke_without_command=True)
    async def premium_guild_group(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        await self._send_private(ctx, embed=self._guild_status_embed(ctx.guild))

    @premium_guild_group.command(name="status", with_app_command=True, description="See this server's Guild Pro status")
    async def premium_guild_status_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        await self._send_private(ctx, embed=self._guild_status_embed(ctx.guild))

    @premium_guild_group.command(name="claim", with_app_command=True, description="Assign one of your Guild Pro claims to this server")
    async def premium_guild_claim_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        ok, message = await self.service.claim_guild(guild_id=ctx.guild.id, user_id=ctx.author.id)
        await self._send_result(ctx, title="Guild Pro Claim", message=message, ok=ok)

    @premium_guild_group.command(name="release", with_app_command=True, description="Release Guild Pro from this server")
    async def premium_guild_release_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx) or not await self._guild_admin_guard(ctx):
            return
        ok, message = await self.service.release_guild(guild_id=ctx.guild.id, user_id=ctx.author.id)
        await self._send_result(ctx, title="Guild Pro Release", message=message, ok=ok)

    @commands.command(name="premiumadmin", hidden=True)
    async def premium_admin_command(self, ctx: commands.Context, *parts: str):
        if ctx.guild is not None:
            await ctx.send(content="That command is only available in DM.")
            return
        author_id = getattr(ctx.author, "id", 0)
        if not self._is_override_owner(author_id):
            print(f"Premium owner command denied: unauthorized_dm_user_id={author_id}")
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
            record = await self.service.create_manual_override(
                target_type=target_type,
                target_id=target_id,
                kind=MANUAL_KIND_GRANT,
                plan_code=plan_code,
                actor_user_id=author_id,
                reason=reason,
            )
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
            record = await self.service.create_manual_override(
                target_type=target_type,
                target_id=target_id,
                kind=MANUAL_KIND_BLOCK,
                plan_code=None,
                actor_user_id=author_id,
                reason=reason,
            )
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
