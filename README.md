# Babblebox

Babblebox is a modular Discord bot built with Python and `discord.py`.
It combines:

- party games for active groups
- everyday utility tools that stay lightweight and private
- a shared Daily micro-challenge for low-activity moments
- a compact Buddy/Profile identity layer that makes the bot feel cohesive and showable

Babblebox is designed to stay useful without drifting into a bloated economy bot, moderation suite, or API-heavy novelty project.

## Official Links

- Official website: [https://arno-create.github.io/babblebox-bot/](https://arno-create.github.io/babblebox-bot/)
- Invite link: [https://discord.com/oauth2/authorize?client_id=1480903089518022739](https://discord.com/oauth2/authorize?client_id=1480903089518022739)
- GitHub repository: [https://github.com/arno-create/babblebox-bot](https://github.com/arno-create/babblebox-bot)
- Support server: [https://discord.com/servers/inevitable-friendship-1322933864360050688](https://discord.com/servers/inevitable-friendship-1322933864360050688)

## Feature Categories

### Party Games

- Broken Telephone
- Exquisite Corpse
- Spyfall
- Word Bomb
- Chaos Cards for lightweight pre-game twists
- hybrid slash + `bb!` prefix support
- session stats and leaderboard commands

### Everyday Utilities

- Watch: DM alerts for mentions and watched keywords
- Later: save one reading marker per channel
- Capture: private recent-message transcript snapshots
- Remind: safe one-time reminders with bounded limits
- AFK: scheduled or immediate away status with elapsed and return context

### Daily Play

- `Babblebox Daily`: one shared deterministic Daily Shuffle puzzle per UTC day
- compact per-user result row storage
- daily streaks
- shareable text result output
- lifetime summary stats plus bounded raw retention

### Buddy / Profile / Vault

- one lightweight Buddy per user
- cosmetic-first style and naming
- XP and level progression tied to real usage
- anti-farm daily XP caps by category
- `/profile` and `/vault` views that summarize Daily, utilities, Buddy, and multiplayer activity

## Commands

Slash commands and the `bb!` prefix both work.

### Core

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/help` | `bb!help` | Open the in-bot manual |
| `/ping` | `bb!ping` | Health check |
| `/play` | `bb!play` | Open a lobby |
| `/stop` | `bb!stop` | Force stop the active lobby/game |
| `/vote` | `bb!vote` | Trigger a Spyfall vote |
| `/stats` | `bb!stats` | Session stats |
| `/leaderboard` | `bb!leaderboard` | Session leaderboard |
| `/chaoscard` | `bb!chaoscard` | Cycle the current lobby Chaos Card |

### Everyday Utilities

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/watch settings` | `bb!watch settings` | View your watch setup |
| `/watch keyword add` | `bb!watch keyword add server contains project update` | Add a watched phrase |
| `/watch off` | `bb!watch off server` | Clear watch settings |
| `/later mark` | `bb!later mark` | Save your current reading spot |
| `/later list` | `bb!later list` | List saved markers |
| `/later clear` | `bb!later clear here` | Clear markers |
| `/capture` | `bb!capture 10` | DM yourself a private snapshot |
| `/remind set` | `bb!remind set 2h dm take a break` | Create a reminder |
| `/remind list` | `bb!remind list` | Review active reminders |
| `/remind cancel` | `bb!remind cancel <id>` | Cancel a reminder |
| `/afk` | `bb!afk` | Set, schedule, or clear AFK |
| `/afkstatus` | `bb!afkstatus` | View AFK status |

### Daily Play

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/daily` | `bb!daily` | View today's Daily |
| `/daily play <guess>` | `bb!daily play <guess>` | Submit a Daily guess |
| `/daily stats` | `bb!daily stats` | View Daily streaks and recent runs |
| `/daily share` | `bb!daily share` | Share your Daily result |
| `/daily leaderboard` | `bb!daily leaderboard` | View Daily standings |

### Buddy / Profile

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/buddy` | `bb!buddy` | Open your Buddy card |
| `/buddy rename` | `bb!buddy rename Pebble` | Rename your Buddy |
| `/buddy style` | `bb!buddy style sunset` | Change Buddy style |
| `/buddy stats` | `bb!buddy stats` | View XP, badges, and titles |
| `/profile` | `bb!profile` | View your Babblebox profile |
| `/vault` | `bb!vault` | Open your personal vault view |

## How Daily Works

Babblebox Daily currently ships with one flagship mode: `Daily Shuffle`.

- Each UTC day maps to one deterministic puzzle from local curated data.
- The challenge content is regenerated from the date, not stored in the database.
- Each user gets one result row per day and challenge.
- Users get up to 3 attempts.
- Only compact outcome data is stored: attempts, solved/failed, timestamps, and solve time.
- Raw Daily rows are pruned after 180 days.
- Lifetime streak and clear counters live in the profile row, so long-term stats survive pruning.

## How Buddy Works

Buddy is intentionally lightweight.

- One companion row per user
- Buddy species, nickname, style, mood, title slot, and featured badge slot
- XP and level progression
- No inventory
- No currency
- No lootboxes
- No blob or image storage

Progress comes from real activity:

- Daily participation and clears
- selected utility use
- game participation and wins

Anti-farm controls:

- Daily XP bucket cap
- Utility XP bucket cap
- Game XP bucket cap
- No XP for spammy repeated share loops or noisy utility abuse

## Profile / Vault Design

`/profile` and `/vault` pull together:

- Buddy identity
- Daily streaks and totals
- utility summary
- persistent multiplayer summary
- current session stats when available

Utility-sensitive details remain conservative:

- self-view can show active reminder and Later counts
- public profile views avoid exposing personal utility details

## Storage Discipline

Babblebox is built with a free-tier database budget in mind.

### Production persistence choices

- no production JSON persistence for the new systems
- no screenshot storage
- no image/blob storage
- no full watched-message archives
- no long-term capture transcript storage
- no arbitrary per-message logs

### New compact tables

- `bb_user_profiles`
  - one row per user
  - Buddy identity, streaks, XP, and compact counters
- `bb_daily_results`
  - one row per user per day per challenge
  - attempts, solved flag, timestamps, and solve time
- `bb_identity_meta`
  - small metadata such as prune bookkeeping

### Existing compact tables

Utility persistence remains in the existing Postgres-first utility tables for Watch, Later, Remind, and AFK.

### Retention

- raw Daily rows prune after 180 days
- long-term streak and clear counters stay in `bb_user_profiles`
- reminders are removed after delivery or cancel
- Later markers remain one-per-user-per-channel
- Watch state remains bounded by keyword limits

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
- `babblebox/game_engine.py`: lobby state, views, timers, game flow, session stats, help embed
- `babblebox/utility_store.py`: Postgres-first utility persistence with memory backend for tests/dev
- `babblebox/utility_service.py`: Watch, Later, Capture, Remind, AFK orchestration
- `babblebox/daily_challenges.py`: deterministic Daily puzzle generation
- `babblebox/profile_store.py`: compact profile/daily persistence backend
- `babblebox/profile_service.py`: Daily, Buddy, Profile, Vault logic and anti-farm progression
- `babblebox/cogs/identity.py`: Daily, Buddy, Profile, Vault commands

## Hosting Notes

Babblebox is intended to stay friendly to constrained free-tier hosting.

- no always-on polling loop for Daily generation
- one deterministic Daily per UTC day
- no external APIs for Daily/Buddy/Profile
- small connection pools
- no giant analytics tables
- no unnecessary user-content retention

The existing utility scheduler remains wake-on-change rather than polling every feature separately.

## Setup

### Requirements

- Python 3.9+
- A Discord bot token
- A `.env` file in the project root
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
- `PROFILE_STORAGE_BACKEND=memory` is optional if you want the Daily/Buddy/Profile layer in memory for tests/dev

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
