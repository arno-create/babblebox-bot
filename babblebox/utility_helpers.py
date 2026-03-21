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

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".mkv", ".avi"}
AUDIO_EXTENSIONS = {".ogg", ".mp3", ".wav", ".m4a", ".flac"}
MESSAGE_LINK_RE = re.compile(r"https?://(?:canary\.|ptb\.)?discord(?:app)?\.com/channels/(?P<guild>\d+|@me)/(?P<channel>\d+)/(?P<message>\d+)")


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


def _attachment_kind(attachment) -> str:
    content_type = (getattr(attachment, "content_type", "") or "").lower()
    filename = getattr(attachment, "filename", "") or ""
    lowered = filename.lower()
    for suffix in IMAGE_EXTENSIONS:
        if lowered.endswith(suffix):
            return "image"
    for suffix in VIDEO_EXTENSIONS:
        if lowered.endswith(suffix):
            return "video"
    for suffix in AUDIO_EXTENSIONS:
        if lowered.endswith(suffix):
            return "audio"
    if content_type.startswith("image/"):
        return "image"
    if content_type.startswith("video/"):
        return "video"
    if content_type.startswith("audio/"):
        return "audio"
    return "attachment"


def _attachment_display_name(attachment) -> str:
    filename = (getattr(attachment, "filename", "") or "").strip()
    if filename:
        return filename
    return _attachment_kind(attachment)


def make_attachment_labels(message: discord.Message, *, include_urls: bool = True) -> list[str]:
    labels = []
    for attachment in message.attachments:
        label = _attachment_display_name(attachment)
        if include_urls and getattr(attachment, "url", None):
            label = f"{label} ({attachment.url})"
        labels.append(label)
    return labels


def build_attachment_summary(attachments, *, include_names: bool = True) -> str | None:
    if not attachments:
        return None

    counts = {"image": 0, "video": 0, "audio": 0, "attachment": 0}
    names: list[str] = []
    for attachment in attachments:
        kind = _attachment_kind(attachment)
        counts[kind] += 1
        if include_names:
            names.append(_attachment_display_name(attachment))

    if len(attachments) == 1:
        kind = _attachment_kind(attachments[0])
        label = _attachment_display_name(attachments[0])
        if kind == "attachment":
            return f"[attachment: {label}]"
        return f"[{kind}: {label}]"

    parts = []
    for kind, label in (("image", "image"), ("video", "video"), ("audio", "audio"), ("attachment", "file")):
        count = counts[kind]
        if count <= 0:
            continue
        suffix = "" if count == 1 else "s"
        parts.append(f"{count} {label}{suffix}")
    summary = "[media: " + ", ".join(parts) + "]"

    if include_names and names:
        rendered_names = ", ".join(names[:3])
        if len(names) > 3:
            rendered_names += f", +{len(names) - 3} more"
        summary += f" {rendered_names}"
    return summary


def make_message_preview(content: str | None, *, attachments=None, limit: int = 260) -> str:
    preview = (content or "").strip()
    media_summary = build_attachment_summary(list(attachments or []), include_names=True)

    if preview and media_summary:
        preview = f"{preview}\nMedia: {media_summary}"
    elif not preview:
        preview = media_summary or "[message preview unavailable]"

    return ge.safe_field_text(preview, limit=limit)


def parse_message_link(raw: str | None) -> tuple[int | None, int, int] | None:
    if not raw:
        return None
    match = MESSAGE_LINK_RE.search(raw.strip())
    if match is None:
        return None
    guild_value = match.group("guild")
    guild_id = None if guild_value == "@me" else int(guild_value)
    return guild_id, int(match.group("channel")), int(match.group("message"))


def _quote_block(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return "> [moment unavailable]"
    return "\n".join(f"> {line}" for line in lines[:4])


def _message_color(message: discord.Message) -> discord.Color:
    author_color = getattr(message.author, "color", None)
    if isinstance(author_color, discord.Color) and author_color.value:
        return author_color
    return ge.EMBED_THEME["accent"]


def build_moment_card_embed(
    message: discord.Message,
    *,
    followup: discord.Message | None = None,
    title: str | None = None,
    requested_by: discord.abc.User | None = None,
) -> discord.Embed:
    preview = make_message_preview(message.content, attachments=message.attachments, limit=500)
    embed = discord.Embed(
        title=title or "Babblebox Moment Card",
        description=_quote_block(preview),
        color=_message_color(message),
        timestamp=message.created_at or ge.now_utc(),
    )

    author_icon = None
    display_avatar = getattr(message.author, "display_avatar", None)
    if display_avatar is not None:
        author_icon = getattr(display_avatar, "url", None)
    embed.set_author(name=f"{ge.display_name_of(message.author)} said:", icon_url=author_icon)

    channel_name = getattr(message.channel, "mention", "#unknown")
    guild_name = message.guild.name if message.guild else "Direct Messages"
    scene = f"{guild_name} • {channel_name}"
    if message.created_at is not None:
        scene += f"\n{ge.format_timestamp(message.created_at, 'f')}"
    embed.add_field(name="Scene", value=scene, inline=False)
    corrected_scene = f"{guild_name} | {channel_name}"
    if message.created_at is not None:
        corrected_scene += f"\n{ge.format_timestamp(message.created_at, 'f')}"
    embed.set_field_at(len(embed.fields) - 1, name="Scene", value=corrected_scene, inline=False)

    if followup is not None:
        echo_preview = make_message_preview(followup.content, attachments=followup.attachments, limit=320)
        embed.add_field(
            name=f"Echo • {ge.display_name_of(followup.author)}",
            value=_quote_block(echo_preview),
            inline=False,
        )

    if followup is not None:
        embed.set_field_at(
            len(embed.fields) - 1,
            name=f"Echo | {ge.display_name_of(followup.author)}",
            value=_quote_block(echo_preview),
            inline=False,
        )

    if requested_by is not None and requested_by.id != getattr(message.author, "id", None):
        embed.add_field(name="Pinned By", value=ge.display_name_of(requested_by), inline=True)

    return ge.style_embed(embed, footer="Babblebox Moment | Live link attached, nothing archived")


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
        value=make_message_preview(message.content, attachments=message.attachments),
        inline=False,
    )
    return ge.style_embed(embed, footer="Babblebox Watch | Quiet DM alert with a jump link")


def build_later_marker_embed(marker: dict) -> discord.Embed:
    embed = discord.Embed(
        title="Later Marker Saved",
        description=f"Your reading spot in **{marker['guild_name']} / #{marker['channel_name']}** is tucked away.",
        color=ge.EMBED_THEME["info"],
        timestamp=deserialize_datetime(marker.get("message_created_at")) or ge.now_utc(),
    )
    embed.add_field(name="From", value=marker.get("author_name", "Unknown"), inline=True)
    embed.add_field(name="Saved", value=ge.format_timestamp(deserialize_datetime(marker.get("saved_at")), "R"), inline=True)
    embed.add_field(name="Preview", value=marker.get("preview", "[no preview available]"), inline=False)
    attachment_lines = marker.get("attachment_labels") or []
    if attachment_lines:
        embed.add_field(name="Attachments", value=ge.join_limited_lines(attachment_lines[:4]), inline=False)
    return ge.style_embed(embed, footer="Babblebox Later | Pick up exactly where you left off")


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
        title="Capture Delivered",
        description=f"Packed **{captured_count}** recent message(s) from **{guild_name} / #{channel_name}** into your DMs.",
        color=ge.EMBED_THEME["info"],
    )
    embed.add_field(name="Requested", value=str(requested_count), inline=True)
    embed.add_field(name="Captured", value=str(captured_count), inline=True)
    if preview_lines:
        embed.add_field(name="Peek", value=ge.join_limited_lines(preview_lines[:6]), inline=False)
    view = build_jump_view(jump_url, label="Jump Back to Channel") if jump_url else None
    return ge.style_embed(embed, footer="Babblebox Capture | Full transcript attached, no long-term archive"), view


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
        content = make_message_preview(message.content, attachments=message.attachments, limit=500)
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


def build_reminder_delivery_view(reminder: dict) -> discord.ui.View | None:
    jump_url = reminder.get("origin_jump_url")
    if not jump_url:
        return None
    if reminder.get("delivery") != "dm":
        return None
    if reminder.get("guild_id") is None:
        return None
    return build_jump_view(jump_url)


def build_afk_status_embed(user: discord.abc.User, record: dict, *, title: str | None = None) -> discord.Embed:
    status = record.get("status", "active")
    created_at = deserialize_datetime(record.get("created_at"))
    set_at = deserialize_datetime(record.get("set_at")) or deserialize_datetime(record.get("starts_at")) or created_at
    starts_at = deserialize_datetime(record.get("starts_at")) or set_at
    ends_at = deserialize_datetime(record.get("ends_at"))
    embed = discord.Embed(
        title=title or ("Babblebox AFK Scheduled" if status == "scheduled" else "Babblebox AFK"),
        description=f"**{ge.display_name_of(user)}**",
        color=ge.EMBED_THEME["warning"],
        timestamp=ends_at or starts_at or ge.now_utc(),
    )
    if record.get("reason"):
        embed.add_field(name="Reason", value=ge.safe_field_text(record["reason"], limit=512), inline=False)
    timing_lines = []
    if status == "scheduled" and starts_at is not None:
        timing_lines.append(f"Starts: {ge.format_timestamp(starts_at, 'R')} ({ge.format_timestamp(starts_at, 'f')})")
    elif set_at is not None:
        timing_lines.append(f"Away Since: {ge.format_timestamp(set_at, 'R')} ({ge.format_timestamp(set_at, 'f')})")
    if ends_at is not None:
        timing_lines.append(f"Returns: {ge.format_timestamp(ends_at, 'R')} ({ge.format_timestamp(ends_at, 'f')})")
    if timing_lines:
        embed.add_field(name="Timing", value="\n".join(timing_lines), inline=False)
    return ge.style_embed(embed, footer="Babblebox AFK | Away notices show elapsed time and return ETA when available.")


def build_afk_notice_line(user: discord.abc.User, record: dict) -> str:
    parts = [f"**{ge.display_name_of(user)}** is AFK"]
    set_at = deserialize_datetime(record.get("set_at")) or deserialize_datetime(record.get("starts_at")) or deserialize_datetime(record.get("created_at"))
    ends_at = deserialize_datetime(record.get("ends_at"))
    if set_at is not None:
        parts.append(f"away since {ge.format_timestamp(set_at, 'R')}")
    if ends_at is not None:
        parts.append(f"back {ge.format_timestamp(ends_at, 'R')}")
    if record.get("reason"):
        parts.append(ge.safe_field_text(record["reason"], limit=120))
    return " - ".join(parts)
