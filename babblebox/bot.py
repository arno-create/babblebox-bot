from __future__ import annotations

import os
import traceback
from pathlib import Path

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge


COGS = (
    "babblebox.cogs.meta",
    "babblebox.cogs.afk",
    "babblebox.cogs.gameplay",
    "babblebox.cogs.utilities",
    "babblebox.cogs.identity",
    "babblebox.cogs.shield",
    "babblebox.cogs.events",
)


class BabbleBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix="bb!", intents=intents, help_command=None, case_insensitive=True)
        self.dictionary_ready = False
        self.dev_guild_id: int | None = None

        dev_guild_raw = os.getenv("DEV_GUILD_ID", "").strip()
        if dev_guild_raw:
            try:
                self.dev_guild_id = int(dev_guild_raw)
            except ValueError:
                print(f"Invalid DEV_GUILD_ID '{dev_guild_raw}'. Ignoring dev guild sync.")

    async def setup_hook(self):
        ge.set_runtime_bot(self)
        self.tree.on_error = self.on_app_command_error

        await self._load_dictionary()

        for extension in COGS:
            await self.load_extension(extension)

        try:
            if self.dev_guild_id:
                dev_guild = discord.Object(id=self.dev_guild_id)
                self.tree.copy_global_to(guild=dev_guild)
                guild_synced = await self.tree.sync(guild=dev_guild)
                print(f"Commands synced to dev guild {self.dev_guild_id}: {len(guild_synced)}")

            global_synced = await self.tree.sync()
            print(f"Commands synced globally: {len(global_synced)}")
        except Exception as exc:
            print(f"Command sync failed: {exc}")
            traceback.print_exc()

    async def _load_dictionary(self):
        if ge.VALID_WORDS:
            self.dictionary_ready = True
            return

        cache_path = Path(__file__).resolve().parent.parent / ".cache" / "words_alpha.txt"
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        if not cache_path.exists():
            timeout = aiohttp.ClientTimeout(total=30)
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(ge.DICTIONARY_URL) as response:
                        response.raise_for_status()
                        with cache_path.open("wb") as cache_file:
                            async for chunk in response.content.iter_chunked(65536):
                                cache_file.write(chunk)
            except Exception as exc:
                self.dictionary_ready = False
                print(f"Failed to download dictionary: {exc}")
                return

        try:
            with cache_path.open("r", encoding="utf-8") as handle:
                ge.VALID_WORDS.update(line.strip().lower() for line in handle if line.strip())
            self.dictionary_ready = True
            print(f"Dictionary loaded: {len(ge.VALID_WORDS)} words ready for Word Bomb.")
        except Exception as exc:
            self.dictionary_ready = False
            print(f"Failed to load cached dictionary: {exc}")

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        print(f"App command error: {error}")
        traceback.print_exception(type(error), error, error.__traceback__)

        if isinstance(error, app_commands.errors.CheckFailure):
            message = "This command can only be used in a server."
        else:
            message = "Something went wrong while running that command."

        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)


def create_bot() -> BabbleBot:
    return BabbleBot()
