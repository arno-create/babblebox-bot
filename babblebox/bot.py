from __future__ import annotations

import os
import traceback
from pathlib import Path
from typing import Any

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge


COGS = (
    "babblebox.cogs.meta",
    "babblebox.cogs.afk",
    "babblebox.cogs.gameplay",
    "babblebox.cogs.party_games",
    "babblebox.cogs.utilities",
    "babblebox.cogs.question_drops",
    "babblebox.cogs.identity",
    "babblebox.cogs.shield",
    "babblebox.cogs.admin",
    "babblebox.cogs.confessions",
    "babblebox.cogs.events",
)

REQUIRED_SLASH_CONTRACT: dict[str, dict[str, object]] = {
    # A prefix-only emergency lock lane is not a releasable state.
    "lock": {
        "children": frozenset({"channel", "remove", "settings"}),
        "default_member_permissions": None,
    },
}

DISCORD_MAX_COMMAND_OPTIONS = 25
DISCORD_MAX_COMMAND_CHOICES = 25


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

    def _required_slash_contract(self) -> dict[str, dict[str, object]]:
        return REQUIRED_SLASH_CONTRACT

    @staticmethod
    def _command_children(command: object) -> set[str]:
        children = getattr(command, "commands", None)
        if children is None:
            children = getattr(command, "options", ())
        return {name for child in children if (name := getattr(child, "name", None))}

    @staticmethod
    def _command_default_permissions(command: object) -> Any:
        permissions = getattr(command, "default_member_permissions", None)
        if permissions is None and hasattr(command, "default_permissions"):
            permissions = getattr(command, "default_permissions")
        return permissions

    def _verify_required_slash_contract(self, commands_to_check: list[object], *, target_label: str) -> None:
        by_name = {
            name: command
            for command in commands_to_check
            if (name := getattr(command, "name", None))
        }
        failures: list[str] = []

        for root_name, contract in self._required_slash_contract().items():
            command = by_name.get(root_name)
            if command is None:
                failures.append(f"{target_label} is missing `/{root_name}`.")
                continue

            expected_permissions = contract.get("default_member_permissions")
            actual_permissions = self._command_default_permissions(command)
            if expected_permissions is None:
                if actual_permissions is not None:
                    rendered_permissions = getattr(actual_permissions, "value", actual_permissions)
                    failures.append(
                        f"{target_label} published `/{root_name}` with default_member_permissions="
                        f"{rendered_permissions}, which hides the slash root from Babblebox's runtime moderator lane."
                    )
            elif actual_permissions != expected_permissions:
                failures.append(
                    f"{target_label} published `/{root_name}` with default_member_permissions="
                    f"{getattr(actual_permissions, 'value', actual_permissions)} instead of {expected_permissions}."
                )

            expected_children = set(contract.get("children", ()))
            actual_children = self._command_children(command)
            missing_children = sorted(expected_children - actual_children)
            if missing_children:
                rendered = ", ".join(f"`/{root_name} {child}`" for child in missing_children)
                failures.append(f"{target_label} is missing required slash subcommands: {rendered}.")

        if failures:
            raise RuntimeError("Required slash-command contract failed: " + " ".join(failures))

    def _command_schema_payload(self, command: object) -> dict[str, Any]:
        to_dict = getattr(command, "to_dict", None)
        if not callable(to_dict):
            return {}
        try:
            payload = to_dict(self.tree)
        except TypeError:
            payload = to_dict()
        return payload if isinstance(payload, dict) else {}

    def _collect_slash_schema_failures(
        self,
        payload: dict[str, Any],
        *,
        path: str,
        target_label: str,
        failures: list[str],
    ) -> None:
        options = payload.get("options", [])
        if isinstance(options, list):
            if len(options) > DISCORD_MAX_COMMAND_OPTIONS:
                failures.append(
                    f"{target_label} exposes {len(options)} options on `{path}` "
                    f"(Discord max {DISCORD_MAX_COMMAND_OPTIONS})."
                )
            for option in options:
                if not isinstance(option, dict):
                    continue
                option_name = option.get("name")
                next_path = f"{path} {option_name}" if option_name else path
                self._collect_slash_schema_failures(
                    option,
                    path=next_path,
                    target_label=target_label,
                    failures=failures,
                )

        choices = payload.get("choices", [])
        if isinstance(choices, list) and len(choices) > DISCORD_MAX_COMMAND_CHOICES:
            failures.append(
                f"{target_label} exposes {len(choices)} choices on `{path}` "
                f"(Discord max {DISCORD_MAX_COMMAND_CHOICES})."
            )

    def _verify_slash_schema_limits(self, commands_to_check: list[object], *, target_label: str) -> None:
        failures: list[str] = []
        for command in commands_to_check:
            name = getattr(command, "name", None)
            if not name:
                continue
            payload = self._command_schema_payload(command)
            if not payload:
                continue
            self._collect_slash_schema_failures(
                payload,
                path=f"/{name}",
                target_label=target_label,
                failures=failures,
            )
        if failures:
            raise RuntimeError("Slash-command schema failed: " + " ".join(failures))

    def _verify_loaded_tree(self, commands_to_check: list[object], *, target_label: str) -> None:
        self._verify_required_slash_contract(commands_to_check, target_label=target_label)
        self._verify_slash_schema_limits(commands_to_check, target_label=target_label)

    async def _sync_commands_for_target(
        self,
        *,
        guild: discord.abc.Snowflake | None,
        target_label: str,
    ) -> list[app_commands.AppCommand]:
        try:
            synced = await self.tree.sync(guild=guild)
        except Exception as exc:
            raise RuntimeError(f"Command sync failed for {target_label}: {exc}") from exc

        self._verify_required_slash_contract(synced, target_label=f"{target_label} sync result")
        print(f"Commands synced to {target_label}: {len(synced)}")
        return synced

    async def setup_hook(self):
        ge.set_runtime_bot(self)
        self.tree.on_error = self.on_app_command_error

        await self._load_dictionary()

        for extension in COGS:
            await self.load_extension(extension)

        self._verify_loaded_tree(self.tree.get_commands(), target_label="loaded global tree")

        if self.dev_guild_id:
            dev_guild = discord.Object(id=self.dev_guild_id)
            self.tree.copy_global_to(guild=dev_guild)
            self._verify_loaded_tree(
                self.tree.get_commands(guild=dev_guild),
                target_label=f"loaded dev guild tree {self.dev_guild_id}",
            )
            await self._sync_commands_for_target(guild=dev_guild, target_label=f"dev guild {self.dev_guild_id}")

        await self._sync_commands_for_target(guild=None, target_label="global commands")

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
