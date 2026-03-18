# Babblebox

Babblebox is a multiplayer Discord party game bot built with Python and `discord.py`.
It is designed around social chaos, polished Discord UX, and replayable mini-games that feel lively instead of disposable.

This version of the project includes:

- A modular backend instead of one giant monolith
- Dual slash and `bb!` prefix commands
- A new `Chaos Card` lobby system
- A utility suite with Watch, Later, Capture, Remind, and a stronger timed AFK system
- Postgres-first utility persistence in production, with explicit memory mode for tests and local dev
- Upgraded embed-driven bot responses
- A restored blue-themed website with privacy, community, and support sections

## Official Links

- Official website: [https://arno-create.github.io/babblebox-bot/](https://arno-create.github.io/babblebox-bot/)
- Invite link: [https://discord.com/oauth2/authorize?client_id=1480903089518022739](https://discord.com/oauth2/authorize?client_id=1480903089518022739)
- GitHub repository: [https://github.com/arno-create/babblebox-bot](https://github.com/arno-create/babblebox-bot)
- Support server: [https://discord.com/servers/inevitable-friendship-1322933864360050688](https://discord.com/servers/inevitable-friendship-1322933864360050688)

## What Babblebox Feels Like

Babblebox is meant to create the kinds of Discord moments people remember:

- Voice-message mimicry that spirals into nonsense
- Social deduction rounds that turn suspicious fast
- Story prompts that produce ridiculous final reveals
- Fast typing pressure that makes Word Bomb feel genuinely tense

It is a party bot, but it is built with real engineering discipline:

- guild-scoped state
- timeout cleanup
- defensive DM handling
- interactive views
- Render-friendly runtime behavior

## Games

### Broken Telephone

Players pass along a voice message by imitating what they hear. The final player types what they think the original phrase was.

### Exquisite Corpse

Players secretly contribute words and phrases to build one absurd final sentence.

### Spyfall

One player is the spy, everyone else knows the location, and the server has to question each other before voting.

### Word Bomb

Players race to type valid English words containing a required syllable before the timer expires.

## Utility Suite

Babblebox is now more than a game bot. It also includes a quiet, personal utility layer that stays aligned with Discord permissions and keeps channel clutter low.

### Watch

Receive DM alerts when someone mentions you or when watched keywords appear, with jump links back to the message. Mention watches can be server-scoped or global, and keyword watches support server or global scope with contains or whole-word matching.

### Later

Mark where you stopped reading in a channel and get a DM jump link back to that point later. One marker per user per channel is stored, so re-marking simply updates your place.

### Capture

Privately DM yourself a structured snapshot of recent channel messages for memory, moderation context, or reference. Captures respect channel visibility and only work from channels the requester can already access.

### Remind

Create safe one-time reminders using relative durations such as `10m`, `2h`, or `1d12h`. Reminders default to DMs, while channel delivery is intentionally stricter to reduce spam and clutter.

### AFK

AFK now handles both indefinite and timed away states. When someone mentions you or replies to one of your messages while you are AFK, Babblebox can show how long you have been away and, if the AFK is timed, when you are expected back.

## Dual Command System

Core commands and the new utility features work in both styles:

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/help` | `bb!help` | Show the manual |
| `/ping` | `bb!ping` | Health check |
| `/play` | `bb!play` | Open a lobby |
| `/stop` | `bb!stop` | Force stop the active lobby/game |
| `/afk` | `bb!afk` | Set, schedule, or clear AFK, including timed away status |
| `/afkstatus` | `bb!afkstatus` | View AFK status |
| `/vote` | `bb!vote` | Trigger a Spyfall vote |
| `/stats` | `bb!stats` | Show session stats |
| `/leaderboard` | `bb!leaderboard` | Show leaderboard |
| `/chaoscard` | `bb!chaoscard` | Cycle the lobby Chaos Card |
| `/watch mentions` | `bb!watch mentions on server` | Toggle mention alerts |
| `/watch keyword add` | `bb!watch keyword add server contains project update` | Add a watched keyword |
| `/watch settings` | `bb!watch settings` | View current watch configuration |
| `/later mark` | `bb!later mark` | Save a reading marker |
| `/later list` | `bb!later list` | List saved reading markers |
| `/capture` | `bb!capture 10` | DM a recent channel snapshot |
| `/remind set` | `bb!remind set 2h dm take a break` | Schedule a one-time reminder |
| `/remind list` | `bb!remind list` | List active reminders |

## Chaos Cards

Chaos Cards are lightweight pre-game lobby modifiers. They change the vibe of a round without adding background workers, polling, or storage-heavy systems.

### Included cards

- `Off`: standard rules
- `Reverse Order`: reverse the shuffled player order
- `Lightning Round`: shorter DM timers, faster vote timing, tighter Word Bomb pacing
- `Encore Reveal`: add a dramatic recap headline at the end of the game

## Embed UX Upgrade

Babblebox now leans harder into embeds across command responses and game presentation:

- Consistent color language for info, success, warning, and danger states
- Shared footer style for better orientation
- Cleaner AFK, lobby, stats, and vote messaging
- Improved Word Bomb turn presentation

## Architecture

The project now uses a package-based layout:

```text
.
|-- babblebox/
|   |-- __init__.py
|   |-- bot.py
|   |-- command_utils.py
|   |-- game_engine.py
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
|       |-- meta.py
|       `-- utilities.py
|-- assets/
|-- keep_alive.py
|-- main.py
|-- index.html
|-- requirements.txt
|-- README.md
`-- tests/
```

### File overview

- `babblebox/bot.py`: bot bootstrap, dictionary loading, extension loading, command sync
- `babblebox/game_engine.py`: shared state, views, timers, and core game flow
- `babblebox/text_safety.py`: shared short-text validation used by AFK and Remind
- `babblebox/utility_store.py`: utility persistence layer with Postgres production storage, memory-mode tests/dev, and one-time legacy JSON import support
- `babblebox/utility_service.py`: utility-state orchestration, delivery scheduling, and watch matching
- `babblebox/utility_helpers.py`: duration parsing, jump-link helpers, transcript generation, and utility embeds
- `babblebox/cogs/meta.py`: help, ping, stats, leaderboard
- `babblebox/cogs/afk.py`: AFK commands
- `babblebox/cogs/gameplay.py`: play, stop, vote, Chaos Card controls
- `babblebox/cogs/utilities.py`: Watch, Later, Capture, and Remind commands
- `babblebox/cogs/events.py`: listeners and lifecycle handling
- `babblebox/web.py`: Flask routes and keep-alive thread

## Key Fixes In This Version

### Prefix double-trigger bug

The duplicated prefix response bug came from calling `bot.process_commands(message)` manually inside a cog `on_message` listener.
`commands.Bot` already processes prefix commands through its own `on_message`, so the extra call caused every prefix command to fire twice.

That extra call has been removed.

### Resource-conscious improvements

- The keep-alive thread is daemonized
- The Word Bomb dictionary is cached to disk
- Chaos Cards are stateless lobby modifiers
- No database polling or always-on background worker system was added

## Hosting Notes

Babblebox is designed to survive on a constrained free Render instance.

### Safe design choices

- No heavy polling loops
- No background analytics workers
- One wake-on-change utility scheduler instead of many always-on workers
- Postgres storage support without per-message database queries in hot paths
- Cleanup-first handling for stale game state

### Persistence note

Babblebox now supports a Postgres-backed utility store for Watch settings, Later markers, reminders, and timed AFK state. Supabase Postgres is the recommended hosted option.

If `UTILITY_DATABASE_URL`, `SUPABASE_DB_URL`, or `DATABASE_URL` is configured, Babblebox will use Postgres, create its utility tables automatically, and optionally import a legacy JSON utility file once if one is present.

Babblebox does not keep writing user utility state to local JSON files in production. For local development and tests you can opt into a non-persistent memory backend instead, but durable runtime continuity depends on the external database.

If no Postgres URL is configured, Babblebox still starts, but storage-backed utility features stay unavailable until the database is configured again.

## Website and Community

The website was restored to the original blue visual direction and now also includes:

- a dedicated Privacy Policy section
- a Support Server section for **inevitable friendship**
- social links in the footer
- updated command and architecture content

### inevitable friendship

Babblebox lives alongside the **inevitable friendship** support server.
That space is where updates can be tested, bugs can be reported, screenshots can be shared, and the next features can be shaped with actual community feedback.

## Local Setup

### Requirements

- Python 3.9+
- A Discord bot token
- A `.env` file in the project root

### 1. Clone the repository

```bash
git clone https://github.com/arno-create/babblebox-bot.git
cd babblebox-bot
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Create `.env`

```env
DISCORD_TOKEN=your_bot_token_here
DEV_GUILD_ID=your_test_server_id_here
UTILITY_DATABASE_URL=postgresql://...
# or SUPABASE_DB_URL=postgresql://...
# or DATABASE_URL=postgresql://...
# optional:
# UTILITY_STORAGE_BACKEND=memory
# UTILITY_JSON_MIGRATION_PATH=.cache/utility_state.json
```

- `DISCORD_TOKEN` is required
- `DEV_GUILD_ID` is optional and useful for faster dev slash-command sync
- `UTILITY_DATABASE_URL` is the preferred Postgres connection string for durable utility storage
- `SUPABASE_DB_URL` and `DATABASE_URL` are also accepted
- `UTILITY_STORAGE_BACKEND=memory` is for explicit local/dev or test workflows only
- `UTILITY_JSON_MIGRATION_PATH` can point at an old JSON utility file for one-time import

### 4. Enable Discord settings

In the Discord Developer Portal, enable:

- Message Content Intent
- Server Members Intent

If you want other servers to invite the bot, make sure it is set to Public.

### 5. Run the bot

```bash
python main.py
```

## Environment Variables

| Variable | Required | Description |
| --- | --- | --- |
| `DISCORD_TOKEN` | Yes | Discord bot token |
| `DEV_GUILD_ID` | No | Optional development guild ID |
| `UTILITY_DATABASE_URL` | No | Preferred Postgres connection string for utility persistence |
| `SUPABASE_DB_URL` | No | Alternate Postgres connection string, useful for Supabase-style naming |
| `DATABASE_URL` | No | Alternate Postgres connection string used by some hosts |
| `UTILITY_STORAGE_BACKEND` | No | Use `memory` only for explicit local/dev or test runs |
| `UTILITY_JSON_MIGRATION_PATH` | No | Optional path to an old JSON utility file for one-time import |

## Recommended Permissions

- View Channels
- Send Messages
- Embed Links
- Attach Files
- Read Message History
- Add Reactions

## DM Requirement

These features rely on DMs being available:

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

## Links

- Official website: [https://arno-create.github.io/babblebox-bot/](https://arno-create.github.io/babblebox-bot/)
- Add the bot to your server: [https://discord.com/oauth2/authorize?client_id=1480903089518022739](https://discord.com/oauth2/authorize?client_id=1480903089518022739)
- Support server: [https://discord.com/servers/inevitable-friendship-1322933864360050688](https://discord.com/servers/inevitable-friendship-1322933864360050688)
