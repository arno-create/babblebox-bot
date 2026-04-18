from __future__ import annotations

import io
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
AFK_CLOCK_RE = re.compile(r"^(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>am|pm)?$")

AFK_QUICK_REASONS = {
    "sleeping": {
        "emoji": "💤",
        "label": "Sleeping",
        "color": discord.Color.from_rgb(86, 119, 173),
    },
    "studying": {
        "emoji": "📚",
        "label": "Studying",
        "color": discord.Color.from_rgb(86, 146, 111),
    },
    "working": {
        "emoji": "💼",
        "label": "Working",
        "color": discord.Color.from_rgb(90, 132, 176),
    },
    "gaming": {
        "emoji": "🎮",
        "label": "Gaming",
        "color": discord.Color.from_rgb(89, 161, 193),
    },
    "busy": {
        "emoji": "⏳",
        "label": "Busy",
        "color": discord.Color.from_rgb(207, 140, 78),
    },
    "eating": {
        "emoji": "🍽️",
        "label": "Eating",
        "color": discord.Color.from_rgb(208, 126, 88),
    },
    "outside": {
        "emoji": "🌿",
        "label": "Outside",
        "color": discord.Color.from_rgb(83, 160, 130),
    },
    "resting": {
        "emoji": "🛋️",
        "label": "Resting",
        "color": discord.Color.from_rgb(163, 122, 133),
    },
}

AFK_UTC_OFFSET_RE = re.compile(r"^(?:(?:utc|gmt)\s*)?(?P<sign>[+-])\s*(?P<hours>\d{1,2})(?::?(?P<minutes>\d{2}))?$", re.IGNORECASE)
AFK_WEEKDAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
AFK_WEEKDAY_SHORT_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
AFK_WEEKDAY_ALIASES = {
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "wed": 2,
    "weds": 2,
    "wednesday": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
    "sun": 6,
    "sunday": 6,
}
AFK_REPEAT_LABELS = {
    "daily": "Every day",
    "weekdays": "Weekdays",
    "weekly": "Every week",
    "custom": "Selected weekdays",
}

AFK_QUICK_REASONS = {
    "sleeping": {
        "emoji": "💤",
        "label": "Sleeping",
        "color": discord.Color.from_rgb(86, 119, 173),
        "default_duration_seconds": 8 * 3600,
    },
    "studying": {
        "emoji": "📚",
        "label": "Studying",
        "color": discord.Color.from_rgb(86, 146, 111),
        "default_duration_seconds": 2 * 3600,
    },
    "working": {
        "emoji": "💼",
        "label": "Working",
        "color": discord.Color.from_rgb(90, 132, 176),
        "default_duration_seconds": 8 * 3600,
    },
    "gaming": {
        "emoji": "🎮",
        "label": "Gaming",
        "color": discord.Color.from_rgb(89, 161, 193),
        "default_duration_seconds": 2 * 3600,
    },
    "busy": {
        "emoji": "⏳",
        "label": "Busy",
        "color": discord.Color.from_rgb(207, 140, 78),
        "default_duration_seconds": 3600,
    },
    "eating": {
        "emoji": "🍽️",
        "label": "Eating",
        "color": discord.Color.from_rgb(208, 126, 88),
        "default_duration_seconds": 30 * 60,
    },
    "outside": {
        "emoji": "🌿",
        "label": "Outside",
        "color": discord.Color.from_rgb(83, 160, 130),
        "default_duration_seconds": 2 * 3600,
    },
    "resting": {
        "emoji": "🛋️",
        "label": "Resting",
        "color": discord.Color.from_rgb(163, 122, 133),
        "default_duration_seconds": 3600,
    },
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


def build_afk_reason_text(*, preset: str | None = None, custom_reason: str | None = None) -> str | None:
    details = (custom_reason or "").strip()
    quick_reason = get_afk_quick_reason(preset)
    if quick_reason is None:
        return details or None
    base = f"{quick_reason['emoji']} {quick_reason['label']}"
    return f"{base} - {details}" if details else base


def resolve_afk_reason_style(reason: str | None) -> dict | None:
    cleaned = (reason or "").strip().casefold()
    if not cleaned:
        return None
    for payload in AFK_QUICK_REASONS.values():
        emoji_prefix = f"{payload['emoji']} {payload['label']}".casefold()
        label_prefix = payload["label"].casefold()
        if cleaned == emoji_prefix or cleaned.startswith(f"{emoji_prefix} - ") or cleaned == label_prefix or cleaned.startswith(f"{label_prefix} - "):
            return payload
    return None


def _parse_afk_clock(text: str) -> tuple[int, int] | None:
    match = AFK_CLOCK_RE.fullmatch(text.strip().lower())
    if match is None:
        return None
    hour = int(match.group("hour"))
    minute = int(match.group("minute") or "0")
    ampm = match.group("ampm")
    if minute > 59:
        return None
    if ampm is None:
        if hour > 23:
            return None
        return hour, minute
    if not (1 <= hour <= 12):
        return None
    if ampm == "am":
        return 0 if hour == 12 else hour, minute
    return (12 if hour == 12 else hour + 12), minute


def normalize_afk_preset_key(key: str | None) -> str | None:
    if key is None:
        return None
    normalized = str(key).strip().casefold()
    return normalized if normalized in AFK_QUICK_REASONS else None


def get_afk_quick_reason(key: str | None) -> dict | None:
    normalized = normalize_afk_preset_key(key)
    if normalized is None:
        return None
    return AFK_QUICK_REASONS.get(normalized)


def get_afk_preset_default_duration(key: str | None) -> int | None:
    quick_reason = get_afk_quick_reason(key)
    if quick_reason is None:
        return None
    default_duration = quick_reason.get("default_duration_seconds")
    return int(default_duration) if isinstance(default_duration, int) and default_duration > 0 else None


def resolve_afk_reason_style(reason: str | None, *, preset: str | None = None) -> dict | None:
    preset_style = get_afk_quick_reason(preset)
    if preset_style is not None:
        return preset_style
    cleaned = (reason or "").strip().casefold()
    if not cleaned:
        return None
    for payload in AFK_QUICK_REASONS.values():
        emoji_prefix = f"{payload['emoji']} {payload['label']}".casefold()
        label_prefix = payload["label"].casefold()
        if cleaned == emoji_prefix or cleaned.startswith(f"{emoji_prefix} - ") or cleaned == label_prefix or cleaned.startswith(f"{label_prefix} - "):
            return payload
    return None


def parse_afk_clock_input(raw: str | None) -> tuple[bool, tuple[int, int] | str | None]:
    if raw is None:
        return False, "Use a time like `23:30`, `08:00`, or `11:30pm`."
    parsed = _parse_afk_clock(raw)
    if parsed is None:
        return False, "Use a time like `23:30`, `08:00`, or `11:30pm`."
    return True, parsed


def parse_afk_weekday(raw: str | None) -> tuple[bool, int | str | None]:
    if raw is None:
        return False, "Choose a weekday like `Monday`."
    weekday = AFK_WEEKDAY_ALIASES.get(str(raw).strip().casefold())
    if weekday is None:
        return False, "Choose a weekday like `Monday`."
    return True, weekday


def format_afk_weekday(weekday: int) -> str:
    if 0 <= weekday < len(AFK_WEEKDAY_NAMES):
        return AFK_WEEKDAY_NAMES[weekday]
    return "Unknown"


def format_afk_clock(hour: int, minute: int) -> str:
    return f"{int(hour):02d}:{int(minute):02d}"


def format_afk_timezone_label(spec: str | None) -> str:
    cleaned = (spec or "").strip()
    return cleaned or "Timezone not set"


def format_afk_weekday_mask(mask: int, *, short: bool = True) -> str:
    names = AFK_WEEKDAY_SHORT_NAMES if short else AFK_WEEKDAY_NAMES
    values = [names[index] for index in range(7) if mask & (1 << index)]
    return ", ".join(values)


def format_afk_repeat_label(repeat: str | None, weekday_mask: int = 0) -> str:
    normalized = str(repeat or "").strip().casefold()
    if normalized == "weekly":
        weekdays = format_afk_weekday_mask(weekday_mask, short=False)
        if weekdays:
            return f"Every {weekdays}"
    if normalized == "custom":
        weekdays = format_afk_weekday_mask(weekday_mask, short=True)
        if weekdays:
            return weekdays
    return AFK_REPEAT_LABELS.get(normalized, "Repeating")


def build_afk_weekday_mask(*weekdays: int) -> int:
    mask = 0
    for weekday in weekdays:
        if isinstance(weekday, int) and 0 <= weekday <= 6:
            mask |= 1 << weekday
    return mask


def default_afk_weekday_mask(repeat: str, *, weekday: int | None = None) -> int:
    normalized = str(repeat).strip().casefold()
    if normalized == "daily":
        return build_afk_weekday_mask(*range(7))
    if normalized == "weekdays":
        return build_afk_weekday_mask(0, 1, 2, 3, 4)
    if normalized == "weekly" and isinstance(weekday, int):
        return build_afk_weekday_mask(weekday)
    return 0


def _parse_afk_utc_offset(raw: str | None) -> tuple[timezone | None, str | None]:
    if raw is None:
        return None, None
    match = AFK_UTC_OFFSET_RE.fullmatch(raw.strip())
    if match is None:
        return None, None
    hours = int(match.group("hours"))
    minutes = int(match.group("minutes") or "0")
    if hours > 14 or minutes > 59:
        return None, None
    total_minutes = hours * 60 + minutes
    if total_minutes > 14 * 60:
        return None, None
    sign = -1 if match.group("sign") == "-" else 1
    offset = timedelta(minutes=sign * total_minutes)
    return timezone(offset), f"UTC{match.group('sign')}{hours:02d}:{minutes:02d}"


def canonicalize_afk_timezone(raw: str | None) -> tuple[bool, str | None, str | None]:
    text = (raw or "").strip()
    if not text:
        return False, None, "Use an IANA timezone like `Asia/Yerevan` or `America/New_York`, or a fixed offset like `UTC+04:00`."
    try:
        ZoneInfo(text)
        return True, text, None
    except ZoneInfoNotFoundError:
        pass
    _, canonical = _parse_afk_utc_offset(text)
    if canonical is not None:
        return True, canonical, None
    return False, None, "Use an IANA timezone like `Asia/Yerevan` or `America/New_York`, or a fixed offset like `UTC+04:00`."


def load_afk_timezone(spec: str | None):
    cleaned = (spec or "").strip()
    if not cleaned:
        return None
    fixed_offset, _ = _parse_afk_utc_offset(cleaned)
    if fixed_offset is not None:
        return fixed_offset
    try:
        return ZoneInfo(cleaned)
    except ZoneInfoNotFoundError:
        return None


def _resolve_afk_local_datetime(tz_spec: str, *, year: int, month: int, day: int, hour: int, minute: int) -> datetime | None:
    tzinfo = load_afk_timezone(tz_spec)
    if tzinfo is None:
        return None
    naive = datetime(year, month, day, hour, minute)
    for fold in (0, 1):
        local_value = naive.replace(tzinfo=tzinfo, fold=fold)
        roundtrip = local_value.astimezone(timezone.utc).astimezone(tzinfo)
        if roundtrip.replace(tzinfo=None) == naive:
            return local_value
    return None


def _schedule_weekday_matches(mask: int, weekday: int) -> bool:
    return bool(mask & (1 << weekday))


def parse_afk_start_at(raw: str | None, *, timezone_name: str | None, now: datetime | None = None) -> tuple[bool, datetime | str | None]:
    if raw is None:
        return True, None
    text = raw.strip()
    if not text:
        return False, "Use `start_at` like `23:00`, `tomorrow 08:30`, or `2026-03-22 23:00` in your saved timezone."
    if timezone_name is None:
        return False, "Set your AFK timezone first with `/afktimezone set`, or use `start_in` for a relative delay."
    tzinfo = load_afk_timezone(timezone_name)
    if tzinfo is None:
        return False, "Your saved AFK timezone is invalid. Set it again with `/afktimezone set`."
    now_utc = (now or ge.now_utc()).astimezone(timezone.utc)
    now_local = now_utc.astimezone(tzinfo)
    lowered = text.casefold()

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        local_value = _resolve_afk_local_datetime(
            timezone_name,
            year=parsed.year,
            month=parsed.month,
            day=parsed.day,
            hour=parsed.hour,
            minute=parsed.minute,
        )
        if local_value is None:
            return False, f"`start_at` lands on a skipped DST time in **{format_afk_timezone_label(timezone_name)}**. Try a nearby time."
        parsed_utc = local_value.astimezone(timezone.utc)
        if parsed_utc <= now_utc:
            return False, "`start_at` must be in the future."
        return True, parsed_utc

    for prefix, day_offset in (("today ", 0), ("tomorrow ", 1)):
        if lowered.startswith(prefix):
            clock = _parse_afk_clock(lowered[len(prefix):].strip())
            if clock is None:
                return False, "Use `start_at` like `23:00`, `tomorrow 08:30`, or `2026-03-22 23:00` in your saved timezone."
            target_date = (now_local + timedelta(days=day_offset)).date()
            local_value = _resolve_afk_local_datetime(
                timezone_name,
                year=target_date.year,
                month=target_date.month,
                day=target_date.day,
                hour=clock[0],
                minute=clock[1],
            )
            if local_value is None:
                return False, f"`start_at` lands on a skipped DST time in **{format_afk_timezone_label(timezone_name)}**. Try a nearby time."
            parsed_utc = local_value.astimezone(timezone.utc)
            if parsed_utc <= now_utc:
                return False, "`start_at` must be in the future."
            return True, parsed_utc

    clock = _parse_afk_clock(lowered)
    if clock is not None:
        candidate_date = now_local.date()
        local_value = _resolve_afk_local_datetime(
            timezone_name,
            year=candidate_date.year,
            month=candidate_date.month,
            day=candidate_date.day,
            hour=clock[0],
            minute=clock[1],
        )
        if local_value is None:
            return False, f"`start_at` lands on a skipped DST time in **{format_afk_timezone_label(timezone_name)}**. Try a nearby time."
        parsed_utc = local_value.astimezone(timezone.utc)
        if parsed_utc <= now_utc:
            tomorrow = candidate_date + timedelta(days=1)
            local_value = _resolve_afk_local_datetime(
                timezone_name,
                year=tomorrow.year,
                month=tomorrow.month,
                day=tomorrow.day,
                hour=clock[0],
                minute=clock[1],
            )
            if local_value is None:
                return False, f"`start_at` lands on a skipped DST time in **{format_afk_timezone_label(timezone_name)}**. Try a nearby time."
            parsed_utc = local_value.astimezone(timezone.utc)
        return True, parsed_utc

    return False, "Use `start_at` like `23:00`, `tomorrow 08:30`, or `2026-03-22 23:00` in your saved timezone."


def compute_next_afk_schedule_start(schedule: dict, *, after: datetime | None = None) -> datetime | None:
    tz_spec = schedule.get("timezone")
    tzinfo = load_afk_timezone(tz_spec)
    if tzinfo is None:
        return None
    try:
        hour = int(schedule.get("local_hour"))
        minute = int(schedule.get("local_minute"))
        weekday_mask = int(schedule.get("weekday_mask", 0))
    except (TypeError, ValueError):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59) or weekday_mask <= 0:
        return None
    created_at = deserialize_datetime(schedule.get("created_at"))
    after_utc = (after or ge.now_utc()).astimezone(timezone.utc)
    if created_at is not None and after_utc < created_at:
        after_utc = created_at
    local_anchor = after_utc.astimezone(tzinfo)
    for day_offset in range(0, 14):
        candidate_date = (local_anchor + timedelta(days=day_offset)).date()
        if not _schedule_weekday_matches(weekday_mask, candidate_date.weekday()):
            continue
        local_value = _resolve_afk_local_datetime(
            tz_spec,
            year=candidate_date.year,
            month=candidate_date.month,
            day=candidate_date.day,
            hour=hour,
            minute=minute,
        )
        if local_value is None:
            continue
        candidate_utc = local_value.astimezone(timezone.utc)
        if candidate_utc <= after_utc:
            continue
        if created_at is not None and candidate_utc < created_at:
            continue
        return candidate_utc
    return None


def compute_latest_afk_schedule_start(schedule: dict, *, at_or_before: datetime | None = None) -> datetime | None:
    tz_spec = schedule.get("timezone")
    tzinfo = load_afk_timezone(tz_spec)
    if tzinfo is None:
        return None
    try:
        hour = int(schedule.get("local_hour"))
        minute = int(schedule.get("local_minute"))
        weekday_mask = int(schedule.get("weekday_mask", 0))
    except (TypeError, ValueError):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59) or weekday_mask <= 0:
        return None
    created_at = deserialize_datetime(schedule.get("created_at"))
    anchor_utc = (at_or_before or ge.now_utc()).astimezone(timezone.utc)
    local_anchor = anchor_utc.astimezone(tzinfo)
    for day_offset in range(0, 14):
        candidate_date = (local_anchor - timedelta(days=day_offset)).date()
        if not _schedule_weekday_matches(weekday_mask, candidate_date.weekday()):
            continue
        local_value = _resolve_afk_local_datetime(
            tz_spec,
            year=candidate_date.year,
            month=candidate_date.month,
            day=candidate_date.day,
            hour=hour,
            minute=minute,
        )
        if local_value is None:
            continue
        candidate_utc = local_value.astimezone(timezone.utc)
        if candidate_utc > anchor_utc:
            continue
        if created_at is not None and candidate_utc < created_at:
            continue
        return candidate_utc
    return None


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
        preview = media_summary or "[quiet message]"

    return ge.safe_field_text(preview, limit=limit)


def build_watch_alert_embed(
    message: discord.Message,
    *,
    trigger_labels: list[str],
    matched_keywords: list[str],
) -> discord.Embed:
    guild_name = message.guild.name if message.guild else "Direct Messages"
    channel_name = getattr(message.channel, "mention", "#unknown")
    embed = discord.Embed(
        title="Babblebox Watch Ping",
        description=f"Quiet alert from **{guild_name}** in {channel_name}.",
        color=ge.EMBED_THEME["accent"],
        timestamp=message.created_at or ge.now_utc(),
    )
    embed.add_field(name="From", value=ge.display_name_of(message.author), inline=True)
    embed.add_field(name="Why", value=", ".join(trigger_labels), inline=True)
    if matched_keywords:
        rendered = ", ".join(f"`{keyword}`" for keyword in matched_keywords[:6])
        if len(matched_keywords) > 6:
            rendered += f" and {len(matched_keywords) - 6} more"
        embed.add_field(name="Matched Keywords", value=rendered, inline=False)
    embed.add_field(
        name="Peek",
        value=make_message_preview(message.content, attachments=message.attachments),
        inline=False,
    )
    return ge.style_embed(embed, footer="Babblebox Watch | Quiet DM alert with a jump link")


def build_later_marker_embed(marker: dict) -> discord.Embed:
    embed = discord.Embed(
        title="Later Marker Saved",
        description=f"Saved your place in **{marker['guild_name']} / #{marker['channel_name']}**.",
        color=ge.EMBED_THEME["info"],
        timestamp=deserialize_datetime(marker.get("message_created_at")) or ge.now_utc(),
    )
    embed.add_field(name="Location", value=f"{marker['guild_name']} / #{marker['channel_name']}", inline=False)
    embed.add_field(name="Saved", value=ge.format_timestamp(deserialize_datetime(marker.get("saved_at")), "R"), inline=True)
    embed.add_field(name="Author", value=marker.get("author_name", "Unknown"), inline=True)
    embed.add_field(name="Preview", value=marker.get("preview", "[quiet message]"), inline=False)
    attachment_lines = marker.get("attachment_labels") or []
    if attachment_lines:
        shown = attachment_lines[:3]
        if len(attachment_lines) > len(shown):
            shown = [*shown, f"+{len(attachment_lines) - len(shown)} more"]
        embed.add_field(name="Attachments", value=ge.join_limited_lines(shown), inline=False)
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
        title="Capture Ready",
        description=f"I sent **{captured_count}** recent message(s) from **{guild_name} / #{channel_name}** to your DMs.",
        color=ge.EMBED_THEME["info"],
    )
    embed.add_field(name="Source", value=f"Asked for **{requested_count}**\nCaptured **{captured_count}**", inline=True)
    embed.add_field(name="Privacy", value="DM only\nNo long-term archive", inline=True)
    if preview_lines:
        embed.add_field(name="Latest Messages", value=ge.join_limited_lines(preview_lines[:5]), inline=False)
    view = build_jump_view(jump_url, label="Back to Channel") if jump_url else None
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


def build_reminder_delivery_view(reminder: dict, *, delivered_in_guild_channel: bool = False) -> discord.ui.View | None:
    jump_url = reminder.get("origin_jump_url")
    if not jump_url:
        return None
    if not delivered_in_guild_channel:
        return None
    return build_jump_view(jump_url)


def build_bump_reminder_embed(
    *,
    provider_label: str,
    reminder_text: str,
    cycle: dict | None = None,
    delayed: bool = False,
) -> discord.Embed:
    cycle = cycle or {}
    due_at = deserialize_datetime(cycle.get("due_at"))
    last_bump_at = deserialize_datetime(cycle.get("last_bump_at"))
    title = f"{provider_label} bump window is open"
    if delayed:
        title = f"{provider_label} bump window is open"
    embed = discord.Embed(
        title=title,
        description=reminder_text,
        color=ge.EMBED_THEME["accent"],
        timestamp=due_at or ge.now_utc(),
    )
    embed.add_field(name="Provider", value=provider_label, inline=True)
    if due_at is not None:
        embed.add_field(name="Window opened", value=ge.format_timestamp(due_at, "f"), inline=True)
    if delayed:
        embed.add_field(name="Delivery", value="Sent late after a temporary interruption.", inline=False)
    if last_bump_at is not None:
        embed.add_field(name="Last verified bump", value=ge.format_timestamp(last_bump_at, "R"), inline=False)
    last_bumper_user_id = cycle.get("last_bumper_user_id")
    if isinstance(last_bumper_user_id, int) and last_bumper_user_id > 0:
        embed.add_field(name="Last bumper", value=f"<@{last_bumper_user_id}>", inline=True)
    return ge.style_embed(embed, footer="Babblebox Bump Reminders | Verified provider success only")


def build_bump_thanks_embed(
    *,
    provider_label: str,
    thanks_text: str,
    bumper_name: str | None = None,
) -> discord.Embed:
    description = thanks_text
    if bumper_name:
        description = f"**{bumper_name}**\n{thanks_text}"
    embed = discord.Embed(
        title=f"{provider_label} bump confirmed",
        description=description,
        color=ge.EMBED_THEME["success"],
        timestamp=ge.now_utc(),
    )
    embed.add_field(name="Provider", value=provider_label, inline=True)
    return ge.style_embed(embed, footer="Babblebox Bump Reminders | Quiet, verified, and low-noise")


def build_afk_status_embed(user: discord.abc.User, record: dict, *, title: str | None = None) -> discord.Embed:
    status = record.get("status", "active")
    created_at = deserialize_datetime(record.get("created_at"))
    set_at = deserialize_datetime(record.get("set_at")) or deserialize_datetime(record.get("starts_at")) or created_at
    starts_at = deserialize_datetime(record.get("starts_at")) or set_at
    ends_at = deserialize_datetime(record.get("ends_at"))
    style = resolve_afk_reason_style(record.get("reason"))
    accent_emoji = style["emoji"] if style is not None else ("🗓️" if status == "scheduled" else "💤")
    embed = discord.Embed(
        title=title or (f"{accent_emoji} AFK Scheduled" if status == "scheduled" else f"{accent_emoji} AFK Enabled"),
        description=f"**{ge.display_name_of(user)}**",
        color=style["color"] if style is not None else ge.EMBED_THEME["warning"],
        timestamp=ends_at or starts_at or ge.now_utc(),
    )
    status_text = "Scheduled away status" if status == "scheduled" else "Away status is active"
    if style is not None:
        status_text = f"{style['emoji']} {style['label']}"
    embed.add_field(name="Status", value=status_text, inline=False)
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
    return ge.style_embed(embed, footer="Babblebox AFK | Away notices stay compact and show return timing when available.")


def build_afk_notice_line(user: discord.abc.User, record: dict) -> str:
    style = resolve_afk_reason_style(record.get("reason"))
    prefix = f"{style['emoji']} " if style is not None else "💤 "
    parts = [f"{prefix}**{ge.display_name_of(user)}** is AFK"]
    set_at = deserialize_datetime(record.get("set_at")) or deserialize_datetime(record.get("starts_at")) or deserialize_datetime(record.get("created_at"))
    ends_at = deserialize_datetime(record.get("ends_at"))
    if set_at is not None:
        parts.append(f"away since {ge.format_timestamp(set_at, 'R')}")
    if ends_at is not None:
        parts.append(f"back {ge.format_timestamp(ends_at, 'R')}")
    if record.get("reason"):
        parts.append(ge.safe_field_text(record["reason"], limit=120))
    return " - ".join(parts)


def build_afk_status_embed(user: discord.abc.User, record: dict, *, title: str | None = None) -> discord.Embed:
    status = record.get("status", "active")
    created_at = deserialize_datetime(record.get("created_at"))
    set_at = deserialize_datetime(record.get("set_at")) or deserialize_datetime(record.get("starts_at")) or created_at
    starts_at = deserialize_datetime(record.get("starts_at")) or set_at
    ends_at = deserialize_datetime(record.get("ends_at"))
    style = resolve_afk_reason_style(record.get("reason"), preset=record.get("preset"))
    accent_emoji = style["emoji"] if style is not None else ("🗓️" if status == "scheduled" else "💤")
    embed = discord.Embed(
        title=title or (f"{accent_emoji} AFK Scheduled" if status == "scheduled" else f"{accent_emoji} AFK Enabled"),
        description=f"**{ge.display_name_of(user)}**",
        color=style["color"] if style is not None else ge.EMBED_THEME["warning"],
        timestamp=ends_at or starts_at or ge.now_utc(),
    )
    state_label = "Scheduled" if status == "scheduled" else "Active"
    status_text = f"{state_label} away status"
    if style is not None:
        status_text = f"{style['emoji']} {style['label']} • {state_label}"
    embed.add_field(name="Status", value=status_text, inline=False)
    if record.get("reason"):
        embed.add_field(name="Reason", value=ge.safe_field_text(record["reason"], limit=512), inline=False)
    if record.get("schedule_id"):
        embed.add_field(name="Source", value="Repeating schedule", inline=True)
    timing_lines = []
    if status == "scheduled" and starts_at is not None:
        timing_lines.append(f"Starts: {ge.format_timestamp(starts_at, 'R')} ({ge.format_timestamp(starts_at, 'f')})")
    elif set_at is not None:
        timing_lines.append(f"Away Since: {ge.format_timestamp(set_at, 'R')} ({ge.format_timestamp(set_at, 'f')})")
    if ends_at is not None:
        timing_lines.append(f"Returns: {ge.format_timestamp(ends_at, 'R')} ({ge.format_timestamp(ends_at, 'f')})")
    if timing_lines:
        embed.add_field(name="Timing", value="\n".join(timing_lines), inline=False)
    return ge.style_embed(embed, footer="Babblebox AFK | Away notices stay compact and show return timing when available.")


def build_afk_notice_line(user: discord.abc.User, record: dict) -> str:
    style = resolve_afk_reason_style(record.get("reason"), preset=record.get("preset"))
    prefix = f"{style['emoji']} " if style is not None else "💤 "
    parts = [f"{prefix}**{ge.display_name_of(user)}** is AFK"]
    set_at = deserialize_datetime(record.get("set_at")) or deserialize_datetime(record.get("starts_at")) or deserialize_datetime(record.get("created_at"))
    ends_at = deserialize_datetime(record.get("ends_at"))
    if set_at is not None:
        parts.append(f"away since {ge.format_timestamp(set_at, 'R')}")
    if ends_at is not None:
        parts.append(f"back {ge.format_timestamp(ends_at, 'R')}")
    if record.get("reason"):
        parts.append(ge.safe_field_text(record["reason"], limit=120))
    return " - ".join(parts)
