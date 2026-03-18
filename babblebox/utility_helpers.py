from __future__ import annotations

import io
import re
from datetime import datetime, timezone

import discord

from babblebox import game_engine as ge


DURATION_PATTERN = re.compile(
    r"(?ix)"
    r"(\d+)\s*"
    r"(w|week|weeks|d|day|days|h|hr|hrs|hour|hours|m|min|mins|minute|minutes|s|sec|secs|second|seconds)"
)

DURATION_UNITS = {
    "w": 7 * 24 * 3600,
    "week": 7 * 24 * 3600,
    "weeks": 7 * 24 * 3600,
    "d": 24 * 3600,
    "day": 24 * 3600,
    "days": 24 * 3600,
    "h": 3600,
    "hr": 3600,
    "hrs": 3600,
    "hour": 3600,
    "hours": 3600,
    "m": 60,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
    "s": 1,
    "sec": 1,
    "secs": 1,
    "second": 1,
    "seconds": 1,
}


def parse_duration_string(raw: str | None) -> int | None:
    if raw is None:
        return None

    text = raw.strip().lower()
    if not text:
        return None

    matches = list(DURATION_PATTERN.finditer(text))
    if not matches:
        return None

    consumed = "".join(match.group(0) for match in matches)
    remainder = re.sub(r"[\s,]+", "", text.replace(consumed, "", 1))
    if remainder:
        total_text = "".join(match.group(0) for match in matches)
        if re.sub(r"[\s,]+", "", total_text) != re.sub(r"[\s,]+", "", text):
            return None

    total_seconds = 0
    covered = []
    position = 0
    for match in matches:
        between = text[position:match.start()]
        if between.strip(", "):
            return None
        amount = int(match.group(1))
        unit = match.group(2).lower()
        total_seconds += amount * DURATION_UNITS[unit]
        covered.append(match.group(0))
        position = match.end()

    if text[position:].strip(", "):
        return None

    return total_seconds


def format_duration_brief(total_seconds: int) -> str:
    seconds = max(0, int(total_seconds))
    if seconds == 0:
        return "0 seconds"

    parts = []
    for unit_seconds, label in (
        (7 * 24 * 3600, "week"),
        (24 * 3600, "day"),
        (3600, "hour"),
        (60, "minute"),
        (1, "second"),
    ):
        if seconds < unit_seconds:
            continue
        count, seconds = divmod(seconds, unit_seconds)
        suffix = "" if count == 1 else "s"
        parts.append(f"{count} {label}{suffix}")
        if len(parts) == 2:
            break
    return " ".join(parts)


def serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def deserialize_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_jump_view(url: str, *, label: str = "Jump to Message") -> discord.ui.View:
    view = discord.ui.View(timeout=120)
    view.add_item(discord.ui.Button(label=label, url=url))
    return view


def make_message_preview(content: str | None, *, attachments: list[str] | None = None, limit: int = 260) -> str:
    preview = (content or "").strip()
    if not preview:
        preview = "[no text content]"

    if attachments:
        attachment_text = ", ".join(attachments[:3])
        if len(attachments) > 3:
            attachment_text += f", +{len(attachments) - 3} more"
        preview = f"{preview}\nAttachments: {attachment_text}"

    return ge.safe_field_text(preview, limit=limit)


def make_attachment_labels(message: discord.Message) -> list[str]:
    labels = []
    for attachment in message.attachments:
        if attachment.url:
            labels.append(f"{attachment.filename} ({attachment.url})")
        else:
            labels.append(attachment.filename)
    return labels


def build_watch_alert_embed(
    message: discord.Message,
    *,
    trigger_labels: list[str],
    matched_keywords: list[str],
) -> discord.Embed:
    guild_name = message.guild.name if message.guild else "Direct Messages"
    channel_name = getattr(message.channel, "mention", "#unknown")
    embed = discord.Embed(
        title="Babblebox Watch Alert",
        description=f"Something you watch for showed up in **{guild_name}**.",
        color=ge.EMBED_THEME["accent"],
        timestamp=message.created_at or ge.now_utc(),
    )
    embed.add_field(name="From", value=ge.display_name_of(message.author), inline=True)
    embed.add_field(name="Channel", value=channel_name, inline=True)
    embed.add_field(name="Triggered By", value=", ".join(trigger_labels), inline=False)
    if matched_keywords:
        rendered = ", ".join(f"`{keyword}`" for keyword in matched_keywords[:6])
        if len(matched_keywords) > 6:
            rendered += f" and {len(matched_keywords) - 6} more"
        embed.add_field(name="Matched Keywords", value=rendered, inline=False)
    embed.add_field(
        name="Preview",
        value=make_message_preview(message.content, attachments=make_attachment_labels(message)),
        inline=False,
    )
    return ge.style_embed(embed, footer="Babblebox Watch | Link buttons do not ping anyone.")


def build_later_marker_embed(marker: dict) -> discord.Embed:
    embed = discord.Embed(
        title="Babblebox Later Marker",
        description=f"Your reading marker for **{marker['guild_name']}** in **#{marker['channel_name']}** is saved.",
        color=ge.EMBED_THEME["info"],
        timestamp=deserialize_datetime(marker.get("message_created_at")) or ge.now_utc(),
    )
    embed.add_field(name="Author", value=marker.get("author_name", "Unknown"), inline=True)
    embed.add_field(name="Saved", value=ge.format_timestamp(deserialize_datetime(marker.get("saved_at")), "R"), inline=True)
    embed.add_field(name="Preview", value=marker.get("preview", "[no preview available]"), inline=False)
    return ge.style_embed(embed, footer="Babblebox Later | Pick up where you left off.")


def build_capture_delivery_embed(
    *,
    guild_name: str,
    channel_name: str,
    captured_count: int,
    requested_count: int,
    preview_lines: list[str],
    jump_url: str | None,
) -> tuple[discord.Embed, discord.ui.View | None]:
    embed = discord.Embed(
        title="Babblebox Capture",
        description=f"Captured **{captured_count}** recent message(s) from **{guild_name} / #{channel_name}**.",
        color=ge.EMBED_THEME["info"],
    )
    embed.add_field(name="Requested", value=str(requested_count), inline=True)
    embed.add_field(name="Captured", value=str(captured_count), inline=True)
    if preview_lines:
        embed.add_field(name="Preview", value=ge.join_limited_lines(preview_lines[:6]), inline=False)
    view = build_jump_view(jump_url, label="Jump Back to Channel") if jump_url else None
    return ge.style_embed(embed, footer="Babblebox Capture | Full transcript attached."), view


def build_capture_transcript_file(
    *,
    guild_name: str,
    channel_name: str,
    messages: list[discord.Message],
) -> discord.File:
    lines = [f"Babblebox Capture", f"Server: {guild_name}", f"Channel: #{channel_name}", ""]
    for message in reversed(messages):
        timestamp = message.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if message.created_at else "Unknown time"
        author = ge.display_name_of(message.author)
        content = message.content.strip() or "[no text content]"
        lines.append(f"[{timestamp}] {author}")
        lines.append(content)
        for attachment in message.attachments:
            lines.append(f"Attachment: {attachment.filename} - {attachment.url}")
        lines.append("")

    buffer = io.BytesIO("\n".join(lines).encode("utf-8"))
    return discord.File(buffer, filename=f"babblebox-capture-{channel_name}.txt")


def build_reminder_delivery_embed(reminder: dict, *, delayed: bool = False) -> discord.Embed:
    due_at = deserialize_datetime(reminder.get("due_at"))
    created_at = deserialize_datetime(reminder.get("created_at"))
    title = "Babblebox Reminder"
    if delayed:
        title = "Babblebox Reminder (Delayed Delivery)"

    embed = discord.Embed(
        title=title,
        description=reminder.get("text", "[missing reminder text]"),
        color=ge.EMBED_THEME["success"],
        timestamp=due_at or ge.now_utc(),
    )
    if created_at is not None:
        embed.add_field(name="Set", value=ge.format_timestamp(created_at, "f"), inline=True)
    if due_at is not None:
        embed.add_field(name="Due", value=ge.format_timestamp(due_at, "f"), inline=True)
    context_parts = []
    if reminder.get("guild_name"):
        context_parts.append(reminder["guild_name"])
    if reminder.get("channel_name"):
        context_parts.append(f"#{reminder['channel_name']}")
    if context_parts:
        embed.add_field(name="Original Context", value=" / ".join(context_parts), inline=False)
    return ge.style_embed(embed, footer="Babblebox Remind | One-time reminders only.")


def build_brb_status_embed(user: discord.abc.User, record: dict) -> discord.Embed:
    ends_at = deserialize_datetime(record.get("ends_at"))
    created_at = deserialize_datetime(record.get("created_at"))
    embed = discord.Embed(
        title="Babblebox BRB",
        description=f"**{ge.display_name_of(user)}** is temporarily away.",
        color=ge.EMBED_THEME["warning"],
        timestamp=ends_at or ge.now_utc(),
    )
    if record.get("reason"):
        embed.add_field(name="Reason", value=ge.safe_field_text(record["reason"], limit=512), inline=False)
    if created_at is not None:
        embed.add_field(name="Started", value=ge.format_timestamp(created_at, "R"), inline=True)
    if ends_at is not None:
        embed.add_field(name="Returns", value=ge.format_timestamp(ends_at, "R"), inline=True)
    return ge.style_embed(embed, footer="Babblebox BRB | Timed away notice, not a Discord profile status.")
