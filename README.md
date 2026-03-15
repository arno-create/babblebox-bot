# Babblebox

Babblebox is a multiplayer Discord party game bot built with Python and `discord.py`.
It now uses a modular package layout, a dual command system, and a lightweight Chaos Card feature designed to stay safe on a free Render instance.

## Official Links

- Official website: [https://arno-create.github.io/babblebox-bot/](https://arno-create.github.io/babblebox-bot/)
- Add the bot: [https://discord.com/oauth2/authorize?client_id=1480903089518022739](https://discord.com/oauth2/authorize?client_id=1480903089518022739)
- Repository: [https://github.com/arno-create/babblebox-bot](https://github.com/arno-create/babblebox-bot)

## What Changed

- The old single-file bot has been split into a package with cogs and shared services.
- Core commands now support both slash commands and the `bb!` prefix.
- A new `Chaos Card` lobby system adds low-cost twists without adding background workers.
- The Flask keep-alive server and website content were updated to match the new command system.

## Features

### Core Games

- Broken Telephone
- Exquisite Corpse
- Spyfall
- Word Bomb

### Discord UX

- Slash commands and prefix commands
- Interactive `discord.ui.View` buttons and selects
- Hybrid DM and server game flow
- Ephemeral responses where appropriate

### Safety and Reliability

- Guild-scoped runtime state
- Turn, vote, and idle timeout cleanup
- AFK scheduling and expiry support
- Defensive handling for deleted panels, DM failures, and player exits
- Render-friendly keep-alive server with no heavy polling loops

## Dual Command System

These commands work in both formats:

| Slash | Prefix | Purpose |
| --- | --- | --- |
| `/help` | `bb!help` | Show the manual |
| `/ping` | `bb!ping` | Bot health check |
| `/play` | `bb!play` | Open a game lobby |
| `/stop` | `bb!stop` | Force stop the current lobby or match |
| `/afk` | `bb!afk` | Set, schedule, or clear AFK |
| `/afkstatus` | `bb!afkstatus` | View AFK status |
| `/vote` | `bb!vote` | Trigger a Spyfall vote |
| `/stats` | `bb!stats` | View session stats |
| `/leaderboard` | `bb!leaderboard` | View leaderboard |
| `/chaoscard` | `bb!chaoscard` | Cycle the lobby Chaos Card |

## Chaos Cards

Chaos Cards are lightweight lobby modifiers that the host can set before the game starts.
They are designed to feel distinctive without adding database traffic or background processes.

### Included Cards

- `Off`: standard rules
- `Reverse Order`: shuffle the lobby, then reverse the turn order
- `Lightning Round`: shorter DM turns, tighter Spyfall vote windows, faster Word Bomb pressure
- `Encore Reveal`: adds a dramatic recap headline at the end of the game

## Architecture

The project now uses this structure:

```text
.
|-- babblebox/
|   |-- __init__.py
|   |-- bot.py
|   |-- command_utils.py
|   |-- game_engine.py
|   |-- web.py
|   `-- cogs/
|       |-- __init__.py
|       |-- afk.py
|       |-- events.py
|       |-- gameplay.py
|       `-- meta.py
|-- assets/
|-- keep_alive.py
|-- main.py
|-- index.html
|-- requirements.txt
`-- README.md
```

### Module Overview

- `babblebox/bot.py`: bot startup, dictionary loading, extension loading, slash sync
- `babblebox/game_engine.py`: shared state, views, timers, AFK logic, and game flow
- `babblebox/cogs/meta.py`: help, ping, stats, leaderboard
- `babblebox/cogs/afk.py`: AFK commands
- `babblebox/cogs/gameplay.py`: play, vote, stop, Chaos Card control
- `babblebox/cogs/events.py`: message routing and lifecycle listeners
- `babblebox/web.py`: Flask app and keep-alive thread

## Hosting Notes

Babblebox targets a constrained free Render environment.

### Design choices for low-resource hosting

- No database polling loops were introduced
- No always-running analytics or worker processes were added
- The keep-alive thread is daemonized
- The Word Bomb dictionary is cached to disk after download
- Chaos Cards are stateless modifiers stored in the active lobby only

### Visible issues from the original monolith that this refactor addresses

- The old dictionary loader pulled the full word list into memory as one large string before building the set, which created an unnecessary memory spike at startup
- The Flask keep-alive thread was non-daemon, which could interfere with clean process shutdown
- Commands, listeners, timers, web boot logic, and AFK handling all lived in one file, making regressions and duplicate logic much easier to introduce
- Prefix support was incomplete, even though part of the bot already assumed command-prefix handling existed

## Local Setup

### Requirements

- Python 3.11+
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
```

- `DISCORD_TOKEN` is required
- `DEV_GUILD_ID` is optional and useful for faster dev command sync

### 4. Enable Discord developer settings

In the Discord Developer Portal, enable:

- Message Content Intent
- Server Members Intent

If you want the bot to be inviteable by others, make sure it is set to Public.

### 5. Run the bot

```bash
python main.py
```

## Environment Variables

| Variable | Required | Description |
| --- | --- | --- |
| `DISCORD_TOKEN` | Yes | Discord bot token |
| `DEV_GUILD_ID` | No | Optional development guild ID for quicker slash sync |

## Recommended Permissions

- View Channels
- Send Messages
- Embed Links
- Attach Files
- Read Message History
- Add Reactions

## DM Requirement

These features depend on players allowing DMs from server members:

- Broken Telephone
- Exquisite Corpse
- Spyfall role messages

## Screenshots

### Lobby

![Lobby](assets/lobby.png)

### Spyfall Voting

![Spyfall Voting](assets/spyfall_voting.png)

### Word Bomb

![Word Bomb](assets/wordbomb_gameplay.png)

### Exquisite Corpse

![Exquisite Corpse](assets/exquisite_corpse.png)

## Website and Invite

- Official website: [https://arno-create.github.io/babblebox-bot/](https://arno-create.github.io/babblebox-bot/)
- Add the bot to your server: [https://discord.com/oauth2/authorize?client_id=1480903089518022739](https://discord.com/oauth2/authorize?client_id=1480903089518022739)
