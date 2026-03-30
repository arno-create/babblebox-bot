from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import send_hybrid_response
from babblebox.only16_game import manually_arm_only16_message
from babblebox.pattern_hunt_game import PATTERN_HUNT_RULE_FAMILIES, build_pattern_hunt_status_embed, parse_guess_atom, submit_pattern_guess_locked


ATOM_FAMILY_CHOICES = [app_commands.Choice(name=family.replace("_", " ").title(), value=family) for family in PATTERN_HUNT_RULE_FAMILIES]


class PartyGamesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._arm_16_menu = app_commands.ContextMenu(name="Arm 16 Trap", callback=self.arm_only16_context_menu)

    async def cog_load(self):
        self.bot.tree.add_command(self._arm_16_menu)

    def cog_unload(self):
        self.bot.tree.remove_command(self._arm_16_menu.name, type=self._arm_16_menu.type)

    @commands.hybrid_group(name="hunt", with_app_command=True, description="Pattern Hunt guess tools", invoke_without_command=True)
    async def hunt_group(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "Pattern Hunt only works inside a server.", tone="warning", footer="Babblebox Pattern Hunt"),
                ephemeral=True,
            )
            return
        game = ge.games.get(ctx.guild.id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("No Active Hunt", "There is no live Pattern Hunt round right now.", tone="info", footer="Babblebox Pattern Hunt"),
                ephemeral=True,
            )
            return
        await send_hybrid_response(ctx, embed=build_pattern_hunt_status_embed(game, public=False), ephemeral=True)

    @hunt_group.command(name="status", with_app_command=True, description="Show the current Pattern Hunt state")
    async def hunt_status_command(self, ctx: commands.Context):
        await PartyGamesCog.hunt_group.callback(self, ctx)

    @hunt_group.command(name="guess", with_app_command=True, description="Submit a structured Pattern Hunt guess")
    @app_commands.describe(
        atom_one="First rule family",
        value_one="Value for the first family, if needed",
        atom_two="Second rule family",
        value_two="Value for the second family, if needed",
        atom_three="Third rule family",
        value_three="Value for the third family, if needed",
        note="Optional free-text note; this does not affect correctness",
    )
    @app_commands.choices(atom_one=ATOM_FAMILY_CHOICES, atom_two=ATOM_FAMILY_CHOICES, atom_three=ATOM_FAMILY_CHOICES)
    async def hunt_guess_command(
        self,
        ctx: commands.Context,
        atom_one: str,
        value_one: Optional[str] = None,
        atom_two: Optional[str] = None,
        value_two: Optional[str] = None,
        atom_three: Optional[str] = None,
        value_three: Optional[str] = None,
        note: Optional[str] = None,
    ):
        del note
        if ctx.guild is None:
            await send_hybrid_response(ctx, embed=ge.make_status_embed("Server Only", "Pattern Hunt only works inside a server.", tone="warning", footer="Babblebox Pattern Hunt"), ephemeral=True)
            return
        game = ge.games.get(ctx.guild.id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
            await send_hybrid_response(ctx, embed=ge.make_status_embed("No Active Hunt", "There is no live Pattern Hunt round right now.", tone="info", footer="Babblebox Pattern Hunt"), ephemeral=True)
            return
        guessed_atoms = []
        for family, value in ((atom_one, value_one), (atom_two, value_two), (atom_three, value_three)):
            if family is None:
                continue
            ok, atom_or_message = parse_guess_atom(family, value)
            if not ok:
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed("Bad Guess", str(atom_or_message), tone="warning", footer="Babblebox Pattern Hunt"),
                    ephemeral=True,
                )
                return
            guessed_atoms.append(atom_or_message)
        async with game["lock"]:
            game = ge.games.get(ctx.guild.id)
            if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
                await send_hybrid_response(ctx, embed=ge.make_status_embed("No Active Hunt", "That Pattern Hunt round is already closed.", tone="warning", footer="Babblebox Pattern Hunt"), ephemeral=True)
                return
            ok, message = await submit_pattern_guess_locked(ctx.guild.id, game, ctx.author, guessed_atoms)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Pattern Guess",
                message,
                tone="success" if ok and message == "Correct" else "warning",
                footer="Babblebox Pattern Hunt",
            ),
            ephemeral=True,
        )

    async def arm_only16_context_menu(self, interaction: discord.Interaction, message: discord.Message):
        if interaction.guild is None:
            await interaction.response.send_message("Only 16 traps can only be armed inside a server.", ephemeral=True)
            return
        game = ge.games.get(interaction.guild.id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "only16":
            await interaction.response.send_message("There is no active Only 16 round here.", ephemeral=True)
            return
        async with game["lock"]:
            game = ge.games.get(interaction.guild.id)
            if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "only16":
                await interaction.response.send_message("That Only 16 round is already closed.", ephemeral=True)
                return
            ok, reply = await manually_arm_only16_message(message, interaction.guild.id, game, interaction.user)
        await interaction.response.send_message(reply, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(PartyGamesCog(bot))
