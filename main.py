from __future__ import annotations

import os

from dotenv import load_dotenv

from babblebox.bot import create_bot
from babblebox.web import keep_alive


def main():
    load_dotenv()
    keep_alive()

    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN is missing from the environment.")

    bot = create_bot()
    bot.run(token)


if __name__ == "__main__":
    main()
