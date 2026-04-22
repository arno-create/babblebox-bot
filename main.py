from __future__ import annotations

import os

from dotenv import load_dotenv

from babblebox.bot import create_bot
from babblebox.web import set_bot_runtime, start_http_server


def main():
    load_dotenv()
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN is missing from the environment.")

    bot = create_bot()
    set_bot_runtime(bot)
    start_http_server()
    bot.run(token)


if __name__ == "__main__":
    main()
