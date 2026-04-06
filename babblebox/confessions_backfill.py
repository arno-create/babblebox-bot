from __future__ import annotations

import argparse
import asyncio
import json
import sys

from dotenv import load_dotenv

from babblebox.confessions_store import ConfessionsStorageUnavailable, ConfessionsStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill legacy Babblebox Confessions rows into the privacy-hardened storage model."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Apply the backfill.")
    mode.add_argument("--dry-run", action="store_true", help="Count rows without modifying storage.")
    parser.add_argument("--backend", help="Override the confessions storage backend.")
    parser.add_argument("--database-url", help="Override the confessions database URL.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Maximum rows to process per category in one run.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    store = ConfessionsStore(backend=args.backend, database_url=args.database_url)
    try:
        await store.load()
        summary = await store.run_privacy_backfill(apply=bool(args.apply), batch_size=max(1, int(args.batch_size or 100)))
    finally:
        await store.close()
    print(json.dumps(summary, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except ConfessionsStorageUnavailable as exc:
        print(f"Confessions privacy backfill failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
