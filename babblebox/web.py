from __future__ import annotations

import os
from pathlib import Path
from threading import Thread

from flask import Flask, abort, jsonify, send_file, send_from_directory


ROOT_DIR = Path(__file__).resolve().parent.parent
INDEX_PATH = ROOT_DIR / "index.html"

app = Flask(__name__)


@app.get("/")
def home():
    if INDEX_PATH.exists():
        return send_file(INDEX_PATH)

    return jsonify(
        {
            "bot": "Babblebox",
            "status": "online",
            "website": "https://arno-create.github.io/babblebox-bot/",
            "invite": "https://discord.com/oauth2/authorize?client_id=1480903089518022739",
        }
    )


@app.get("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "commands": [
                "/play",
                "bb!play",
                "/daily",
                "/daily play emoji",
                "/daily play signal",
                "/buddy",
                "/profile",
                "/vault",
                "/help",
                "bb!help",
                "/watch settings",
                "/watch replies",
                "/later mark",
                "/capture",
                "/remind set",
            ],
            "website": "https://arno-create.github.io/babblebox-bot/",
        }
    )


@app.get("/assets/<path:filename>")
def assets(filename: str):
    assets_dir = ROOT_DIR / "assets"
    if not assets_dir.exists():
        abort(404)
    return send_from_directory(assets_dir, filename)


@app.get("/<path:filename>")
def static_root_files(filename: str):
    file_path = ROOT_DIR / filename
    if file_path.exists() and file_path.is_file():
        return send_file(file_path)
    abort(404)


def run():
    try:
        port = int(os.getenv("PORT", "10000"))
    except ValueError:
        port = 10000
    app.run(host="0.0.0.0", port=port)


def keep_alive():
    thread = Thread(target=run, daemon=True)
    thread.start()
