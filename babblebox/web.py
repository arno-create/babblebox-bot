from __future__ import annotations

import asyncio
import html
import os
from pathlib import Path
from threading import Thread

from flask import Flask, abort, jsonify, request, send_file, send_from_directory


ROOT_DIR = Path(__file__).resolve().parent.parent
INDEX_PATH = ROOT_DIR / "index.html"

app = Flask(__name__)
_premium_runtime = None


def set_premium_runtime(service):
    global _premium_runtime
    _premium_runtime = service


def _run_premium_coroutine(coro):
    service = _premium_runtime
    loop = getattr(getattr(service, "bot", None), "loop", None)
    if service is None or loop is None:
        raise RuntimeError("Premium runtime is not attached.")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=45)


def _render_premium_page(*, title: str, message: str, tone: str = "info", status_code: int = 200):
    accent = {
        "success": "#1d7f49",
        "warning": "#a86a00",
        "danger": "#a12727",
        "info": "#275ca1",
    }.get(tone, "#275ca1")
    body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} | Babblebox Premium</title>
  <style>
    :root {{
      color-scheme: light;
      --accent: {accent};
      --bg: #f5f2ea;
      --card: #fffdf8;
      --text: #1e1b18;
      --muted: #645c53;
      --border: #ddd1c1;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", system-ui, sans-serif;
      background: radial-gradient(circle at top, #fffaf0 0%, var(--bg) 58%, #efe5d6 100%);
      color: var(--text);
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }}
    main {{
      width: min(560px, 100%);
      background: var(--card);
      border: 1px solid var(--border);
      border-top: 6px solid var(--accent);
      border-radius: 18px;
      padding: 28px;
      box-shadow: 0 18px 50px rgba(49, 35, 12, 0.08);
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 1.8rem;
      line-height: 1.15;
    }}
    p {{
      margin: 0;
      line-height: 1.55;
      color: var(--muted);
    }}
    small {{
      display: block;
      margin-top: 18px;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(title)}</h1>
    <p>{html.escape(message)}</p>
    <small>Return to Discord and open <code>/premium status</code> if you want to confirm the current entitlement state.</small>
  </main>
</body>
</html>"""
    return body, status_code, {"Content-Type": "text/html; charset=utf-8"}


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
                "/premium status",
                "/premium link",
            ],
            "website": "https://arno-create.github.io/babblebox-bot/",
        }
    )


@app.get("/premium/patreon/callback")
def premium_patreon_callback():
    service = _premium_runtime
    if service is None:
        return _render_premium_page(
            title="Premium unavailable",
            message="Babblebox premium is not attached on this deployment right now.",
            tone="warning",
            status_code=503,
        )
    state_token = str(request.args.get("state") or "").strip()
    code = str(request.args.get("code") or "").strip() or None
    error = str(request.args.get("error") or "").strip() or None
    if not state_token:
        return _render_premium_page(
            title="Link failed",
            message="Patreon did not return a valid link state. Start again from `/premium link` in Discord.",
            tone="warning",
            status_code=400,
        )
    try:
        result = _run_premium_coroutine(
            service.complete_link_callback(state_token=state_token, code=code, error=error)
        )
    except Exception as exc:
        return _render_premium_page(
            title="Link failed",
            message=f"Babblebox could not finish Patreon linking safely: {exc}",
            tone="danger",
            status_code=500,
        )
    tone = "success" if str(result.get("title", "")).casefold() == "patreon linked" else "info"
    status_code = 200 if tone == "success" else 400
    return _render_premium_page(
        title=str(result.get("title") or "Premium status"),
        message=str(result.get("message") or "No premium result was returned."),
        tone=tone,
        status_code=status_code,
    )


@app.post("/premium/patreon/webhook")
def premium_patreon_webhook():
    service = _premium_runtime
    if service is None:
        return jsonify({"status": "unavailable", "message": "Premium runtime is not attached."}), 503
    event_type = str(request.headers.get("X-Patreon-Event") or "").strip()
    signature = str(request.headers.get("X-Patreon-Signature") or "").strip()
    if not event_type or not signature:
        return jsonify({"status": "invalid", "message": "Missing Patreon webhook headers."}), 400
    body = request.get_data(cache=False)
    try:
        ok, message = _run_premium_coroutine(
            service.handle_patreon_webhook(body=body, event_type=event_type, signature=signature)
        )
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 400
    return jsonify({"status": "ok" if ok else "ignored", "message": message}), 200


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
