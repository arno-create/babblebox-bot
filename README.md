# Babblebox

**Babblebox** is a production-style multiplayer Discord party game bot built with **Python**, **discord.py**, **Slash Commands**, and **custom UI Views**.  
It is designed for chaotic, social, replayable mini-games with a strong focus on **stability**, **user experience**, and **real-world async architecture**.

## Features

- **4 built-in multiplayer mini-games**
  - 🎙️ **Broken Telephone** — voice-message mimicry through DMs
  - 📝 **Exquisite Corpse** — collaborative absurd story building
  - 🕵️ **Spyfall** — social deduction with interactive UI voting
  - 💣 **Word Bomb** — battle-royale typing game with multiple modes

- **Modern Discord UX**
  - Slash commands via `app_commands`
  - Custom `discord.ui.View` buttons and dropdowns
  - Ephemeral responses where appropriate
  - DM + server hybrid gameplay flows

- **Reliability & safety systems**
  - Per-game lifecycle management
  - Timeout handling and cleanup logic
  - Safe AFK system with sanitized reasons
  - Protection against stale interactions and dead views
  - Defensive handling for DM failures, deleted messages, and player exits

- **Deployment**
  - Hosted on **Render**
  - Keep-alive endpoint using Flask
  - Designed to run continuously on a free-tier style setup

---

## Why this project matters

Babblebox is not just a toy bot.  
It demonstrates practical engineering skills that are directly relevant to backend and real-time systems work:

- asynchronous programming with `asyncio`
- event-driven architecture
- shared state management across guilds
- Discord API integrations
- interaction-driven UI systems
- fault tolerance and cleanup design
- multiplayer game flow orchestration
- external HTTP usage with `aiohttp`

This project was built as a portfolio-grade Discord bot to showcase both **software engineering ability** and **product thinking**.

---

## Game Overview

### 🎙️ Broken Telephone
Players pass along a voice message by mimicking what they hear.  
The final player types what they think the original phrase was.

**Highlights**
- DM-based voice flow
- attachment handling
- timeout enforcement
- end-of-round reveal

---

### 📝 Exquisite Corpse
Players secretly contribute parts of a sentence based on prompts such as adjective, noun, verb, and location.  
The final result is revealed as a funny story.

**Highlights**
- structured sequential DM prompts
- hidden contributions
- themed endings

---

### 🕵️ Spyfall
One player is secretly the spy. Everyone else knows the location.  
Players interrogate one another and eventually trigger a vote to identify the spy.

**Highlights**
- dynamic UI target selection
- live vote system
- timed vote resolution
- interaction-state synchronization

---

### 💣 Word Bomb
A fast-paced typing game where players must send a valid English word containing a required syllable before the timer runs out.

**Highlights**
- dictionary validation using `aiohttp`
- multiple bomb modes
- accelerating turn pressure
- elimination flow and session recap

---

## Tech Stack

- **Python**
- **discord.py**
- **asyncio**
- **aiohttp**
- **Flask**
- **Render**

---

## Architecture Notes

Babblebox uses a **guild-scoped game state model**:

- global `games` dictionary keyed by `guild_id`
- per-game locks for safer concurrent interaction handling
- dedicated timeout tasks for idle, turn, and vote timing
- cleanup routines that cancel tasks and disable active UI views
- DM routing to prevent cross-guild message collisions

This architecture allows multiple servers to host independent game sessions simultaneously.

---

## Commands

### Core
- `/play` — open the lobby and host a game
- `/stop` — stop the current game
- `/help` — show the game manual
- `/ping` — bot health check

### Game
- `/vote` — trigger a Spyfall vote

### AFK
- `/afk` — set or clear AFK status
- `/afkstatus` — check your AFK status

### Stats
- `/stats` — view your Babblebox stats
- `/leaderboard` — view the session leaderboard

---

## Screenshots

> Add screenshots in the `assets/` folder and link them here.

Example:

```md
![Lobby](assets/lobby.png)
![Spyfall](assets/spyfall.png)
![Word Bomb](assets/wordbomb.png)