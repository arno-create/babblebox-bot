# Babblebox

Babblebox is a modular Discord bot built with Python and `discord.py`.

It aims to feel cohesive in four directions at once:

- Party Games for active voice/text hangouts
- Everyday Utilities for quiet server life
- Daily Arcade for low-player-count return visits
- Buddy / Profile / Vault for identity, streaks, and showable progress

Babblebox is intentionally compact:

- no economy grind
- no lootboxes
- no production JSON persistence for durable systems
- no blob/media archives in the database
- no external AI chat or heavy third-party content APIs

## Official Links

- Official website: [https://arno-create.github.io/babblebox-bot/](https://arno-create.github.io/babblebox-bot/)
- Invite link: [https://discord.com/oauth2/authorize?client_id=1480903089518022739](https://discord.com/oauth2/authorize?client_id=1480903089518022739)
- GitHub repository: [https://github.com/arno-create/babblebox-bot](https://github.com/arno-create/babblebox-bot)
- Support server: [https://discord.com/servers/inevitable-friendship-1322933864360050688](https://discord.com/servers/inevitable-friendship-1322933864360050688)

## Product Overview

### Party Games

- Broken Telephone
- Exquisite Corpse
- Spyfall
- Word Bomb
- Chaos Cards and bomb mode variants
- hybrid slash + `bb!` prefix support
- session stats and session leaderboard commands

### Everyday Utilities

- Watch V2
  - separate mention alerts
  - separate reply alerts
  - keyword alerts
  - global, server, or channel scope
  - ignored channels
  - ignored users
  - DM-only delivery with cooldowns and dedupe
- Later
  - one saved reading marker per user per channel
  - media-aware previews
- Capture
  - DM transcript snapshots of recent messages
  - better media placeholders and attachment context
- Moment Cards
  - shareable embed-first quote or exchange cards
  - built from replies, recent messages, or message links
  - no archive table and no generated image pipeline
- Remind
  - safe one-time reminders with small active limits
- AFK
  - immediate or scheduled away status
  - elapsed and return-time messaging

### Daily Arcade

Babblebox Daily is now a small arcade instead of one booth.

Current daily modes:

- Shuffle Booth
  - unscramble the word
- Emoji Booth
  - decode the emoji clue
- Signal Booth
  - decode a shifted word

Daily Arcade design rules:

- deterministic generation from the UTC date
- one compact result row per `user + date + mode`
- small attempt limits
- shareable output
- no external content APIs
- raw rows pruned after 180 days
- streaks and lifetime totals stay in the profile row

### Buddy / Profile / Vault

- one lightweight Buddy per user
- cosmetic style + nickname + mood + title + featured badge
- XP and level progression tied to actual use
- anti-farm per-day XP caps by category
- `/profile` is public-friendly by default
- `/vault` stays the more personal snapshot

## Commands

Slash commands and the `bb!` prefix both work.

### Core

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/help` | `bb!help` | Open the in-bot manual |
| `/ping` | `bb!ping` | Health check |
| `/play` | `bb!play` | Open a game lobby |
| `/stop` | `bb!stop` | Force stop the active lobby/game |
| `/vote` | `bb!vote` | Trigger a Spyfall vote |
| `/stats` | `bb!stats` | Session stats |
| `/leaderboard` | `bb!leaderboard` | Session leaderboard |
| `/chaoscard` | `bb!chaoscard` | Cycle or inspect the lobby Chaos Card |

### Party Games

Party game flow still starts from `/play`.

- Broken Telephone: 3+ players
- Exquisite Corpse: 3+ players
- Spyfall: 3+ players
- Word Bomb: 2+ players

Babblebox now nudges solo users toward Daily Arcade, Buddy, Profile, and utilities instead of leaving them at dead ends.

### Everyday Utilities

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/watch mentions` | `bb!watch mentions` | Toggle mention alerts by channel, server, or global scope |
| `/watch replies` | `bb!watch replies` | Toggle reply alerts separately from mentions |
| `/watch keyword add` | `bb!watch keyword add channel contains camera` | Add a watched keyword in channel/server/global scope |
| `/watch keyword remove` | `bb!watch keyword remove server camera` | Remove a watched keyword |
| `/watch ignore channel` | `bb!watch ignore channel` | Exclude the current channel from Watch |
| `/watch ignore user` | `bb!watch ignore user @name` | Ignore one user's messages in Watch |
| `/watch list` | `bb!watch list` | See keyword buckets and focused channels |
| `/watch settings` | `bb!watch settings` | See mention/reply states, ignore lists, and recent counts |
| `/watch off` | `bb!watch off server` | Clear watch settings by scope |
| `/later mark` | `bb!later mark` | Save your current reading spot |
| `/later list` | `bb!later list` | List saved markers |
| `/later clear` | `bb!later clear here` | Clear markers |
| `/capture` | `bb!capture 10` | DM yourself a recent-message snapshot |
| `/moment create` | `bb!moment create <message_link>` | Create a Moment Card from a message |
| `/moment from-reply` | `bb!moment from-reply` | Create a Moment Card from the replied message |
| `/moment recent` | `bb!moment recent` | Create a Moment Card from the latest channel moment |
| `/remind set` | `bb!remind set 2h dm take a break` | Create a reminder |
| `/remind list` | `bb!remind list` | Review active reminders |
| `/remind cancel` | `bb!remind cancel <id>` | Cancel a reminder |
| `/afk` | `bb!afk` | Set, schedule, or clear AFK |
| `/afkstatus` | `bb!afkstatus` | View AFK status |

### Daily Arcade

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/daily` | `bb!daily` | Open today's arcade overview |
| `/daily play <guess>` | `bb!daily play <guess>` | Play the default Shuffle Booth |
| `/daily play emoji <guess>` | `bb!daily play emoji <guess>` | Play Emoji Booth |
| `/daily play signal <guess>` | `bb!daily play signal <guess>` | Play Signal Booth |
| `/daily stats` | `bb!daily stats` | View arcade streaks and recent runs |
| `/daily share` | `bb!daily share` | Share a completed booth result |
| `/daily leaderboard` | `bb!daily leaderboard` | View arcade standings |

### Buddy / Profile / Vault

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/buddy` | `bb!buddy` | Open your Buddy card |
| `/buddy profile` | `bb!buddy profile` | Re-open the Buddy card explicitly |
| `/buddy rename` | `bb!buddy rename Pebble` | Rename your Buddy |
| `/buddy style` | `bb!buddy style sunset` | Change Buddy style |
| `/buddy stats` | `bb!buddy stats` | View Buddy progression |
| `/profile` | `bb!profile` | View a public-friendly profile card |
| `/vault` | `bb!vault` | Open your more personal vault view |

## Watch V2 Notes

Watch now distinguishes three alert types:

- mentions
- replies
- keywords

Important rules:

- replies only trigger when someone replies to a message owned by the watched user
- bots, webhooks, and self-messages are ignored
- delivery stays DM-only
- dedupe and cooldown protection stay in place
- no message-content archive is stored
- recent counts in settings are runtime-only, not a long-term inbox table

## Capture and Later Media Handling

Capture and Later now treat attachment-only messages more cleanly.

Examples:

- `[image: cat.png]`
- `[video: clip.mp4]`
- `[attachment: notes.pdf]`
- `[media: 2 images, 1 file]`

Important rules:

- no media blobs are stored in Postgres
- attachment URLs are only used at send time when available
- Capture transcripts are delivered privately, not archived long-term

## Daily Arcade Storage Discipline

Babblebox is designed for a small free-tier Postgres budget.

### Persistent identity row

- `bb_user_profiles`
  - one row per user
  - buddy identity
  - XP
  - streaks
  - aggregate counters

### Daily raw rows

- `bb_daily_results`
  - one row per `user + date + mode`
  - attempts
  - solved flag
  - timestamps
  - solve time

Retention:

- raw Daily Arcade rows prune after 180 days
- streaks and lifetime totals remain in `bb_user_profiles`

### Utility persistence

Utility persistence remains Postgres-first:

- `utility_watch_configs`
- `utility_watch_keywords`
- `utility_later_markers`
- `utility_reminders`
- `utility_afk`

New Watch V2 storage stays compact:

- booleans for global mention/reply alerts
- small JSON arrays for guild/channel filters
- small JSON arrays for ignored channels/users
- compact keyword rows with optional guild/channel scope

### Moment Cards

Moment Cards do not introduce a durable archive table.

- no stored generated images
- no stored quote feed
- no stored full message transcripts
- cards are built live from visible Discord messages

## Architecture

```text
.
|-- babblebox/
|   |-- __init__.py
|   |-- bot.py
|   |-- command_utils.py
|   |-- daily_challenges.py
|   |-- game_engine.py
|   |-- profile_service.py
|   |-- profile_store.py
|   |-- text_safety.py
|   |-- utility_helpers.py
|   |-- utility_service.py
|   |-- utility_store.py
|   |-- web.py
|   `-- cogs/
|       |-- __init__.py
|       |-- afk.py
|       |-- events.py
|       |-- gameplay.py
|       |-- identity.py
|       |-- meta.py
|       `-- utilities.py
|-- assets/
|-- tests/
|-- index.html
|-- main.py
`-- requirements.txt
```

### Important modules

- `babblebox/bot.py`: bot bootstrap, extension loading, dictionary setup, sync
- `babblebox/game_engine.py`: lobby state, gameplay flow, recaps, help/manual, session stats
- `babblebox/utility_store.py`: Postgres-first utility persistence, including Watch V2 schema
- `babblebox/utility_service.py`: Watch, Later, Capture, Moment, Remind, AFK orchestration
- `babblebox/utility_helpers.py`: utility preview rendering, transcript formatting, and Moment Card embeds
- `babblebox/daily_challenges.py`: deterministic Daily Arcade booth generation
- `babblebox/profile_store.py`: compact profile and daily persistence
- `babblebox/profile_service.py`: Daily Arcade, Buddy, Profile, Vault, and anti-farm progression
- `babblebox/cogs/identity.py`: Daily Arcade, Buddy, Profile, and Vault commands
- `babblebox/cogs/utilities.py`: Watch V2, Later, Capture, Moment, and Remind commands

## Hosting Notes

Babblebox is designed for constrained hosting:

- no giant analytics tables
- no media/blob storage
- no giant inventory or economy loops
- no external Daily APIs
- small connection pools
- limited background scheduling
- deterministic Daily generation from local data

## Setup

### Requirements

- Python 3.9+
- a Discord bot token
- a `.env` file in the project root
- Postgres for durable storage-backed features

### Install

```bash
git clone https://github.com/arno-create/babblebox-bot.git
cd babblebox-bot
pip install -r requirements.txt
```

### Environment

```env
DISCORD_TOKEN=your_bot_token_here
DEV_GUILD_ID=your_test_server_id_here
UTILITY_DATABASE_URL=postgresql://...
# or SUPABASE_DB_URL=postgresql://...
# or DATABASE_URL=postgresql://...

# optional local/test override:
# UTILITY_STORAGE_BACKEND=memory
# PROFILE_STORAGE_BACKEND=memory
```

Environment variable notes:

- `DISCORD_TOKEN` is required
- `DEV_GUILD_ID` is optional and helps faster dev sync
- `UTILITY_DATABASE_URL` is the preferred Postgres connection string
- `SUPABASE_DB_URL` and `DATABASE_URL` are also accepted
- `UTILITY_STORAGE_BACKEND=memory` is for explicit local/test work only
- `PROFILE_STORAGE_BACKEND=memory` is optional for tests and local development

### Discord Portal Settings

Enable:

- Message Content Intent
- Server Members Intent

### Run

```bash
python main.py
```

## Recommended Permissions

- View Channels
- Send Messages
- Embed Links
- Attach Files
- Read Message History
- Add Reactions

## DM Requirement

These features rely on DMs being open:

- Broken Telephone
- Exquisite Corpse
- Spyfall role messages
- Watch alerts
- Later markers
- Capture transcripts
- DM reminders

## Screenshots

### Lobby

![Lobby](assets/lobby.png)

### Spyfall Voting

![Spyfall Voting](assets/spyfall_voting.png)

### Word Bomb

![Word Bomb](assets/wordbomb_gameplay.png)

### Exquisite Corpse

![Exquisite Corpse](assets/exquisite_corpse.png)
