"""
Centralized trading-asset configuration.

Environment (one of the following is required):

  ASSET=BTC
      Single Polymarket crypto up/down slug (btc, eth, sol, xrp).

  TRADING_ASSETS=btc,eth,sol,xrp
      Comma-separated list for multi-asset deployments.

TRADING_ASSETS takes precedence when both are set.
"""

from __future__ import annotations

import os
import sys
from typing import Tuple

from dotenv import load_dotenv

load_dotenv()

# Slugs used in Polymarket market URLs: {asset}-updown-{interval}-{ts}
SUPPORTED_TRADING_ASSETS: frozenset[str] = frozenset({"btc", "eth", "sol", "xrp", "doge", "hype", "bnb"})

_ASSET_ALIASES: dict[str, str] = {
    "bitcoin": "btc",
    "ethereum": "eth",
    "solana": "sol",
    "ripple": "xrp",
}


def _fatal(message: str) -> None:
    supported = ", ".join(sorted(SUPPORTED_TRADING_ASSETS))
    print(
        f"❌ [config] {message}\n"
        f"   Set ASSET=BTC or TRADING_ASSETS={supported} in .env / Render environment.",
        file=sys.stderr,
    )
    sys.exit(1)


def normalize_asset_slug(raw: str) -> str:
    """Normalize a user-provided asset token to a supported lowercase slug."""
    token = (raw or "").strip().lower()
    if not token:
        raise ValueError("empty asset token")
    return _ASSET_ALIASES.get(token, token)


def parse_trading_assets_from_env() -> Tuple[str, ...]:
    """
    Read TRADING_ASSETS or ASSET from the environment and return validated slugs.
    """
    multi_raw = os.getenv("TRADING_ASSETS", "").strip()
    single_raw = os.getenv("ASSET", "").strip()

    if multi_raw:
        source = "TRADING_ASSETS"
        raw_tokens = [part.strip() for part in multi_raw.split(",") if part.strip()]
    elif single_raw:
        source = "ASSET"
        raw_tokens = [single_raw]
    else:
        _fatal(
            "Missing required asset configuration. "
            "Set ASSET (single asset) or TRADING_ASSETS (comma-separated list)."
        )

    if not raw_tokens:
        _fatal(f"{source} is set but contains no asset tokens.")

    normalized: list[str] = []
    invalid: list[str] = []

    for token in raw_tokens:
        try:
            slug = normalize_asset_slug(token)
        except ValueError:
            invalid.append(repr(token))
            continue
        if slug not in SUPPORTED_TRADING_ASSETS:
            invalid.append(repr(token))
            continue
        if slug not in normalized:
            normalized.append(slug)

    if invalid:
        supported = ", ".join(sorted(SUPPORTED_TRADING_ASSETS))
        _fatal(
            f"Invalid asset(s) in {source}: {', '.join(invalid)}. "
            f"Supported slugs: {supported}."
        )

    if not normalized:
        _fatal(f"No valid assets could be parsed from {source}.")

    return tuple(normalized)


TRADING_ASSETS: Tuple[str, ...] = parse_trading_assets_from_env()
TRADING_ASSETS_UPPER: Tuple[str, ...] = tuple(a.upper() for a in TRADING_ASSETS)

# Alias used by portfolio backfill / trade-history helpers in bot.py
ALL_TRACKED_ASSETS = TRADING_ASSETS


def asset_pnl_filename(asset: str) -> str:
    return f"{normalize_asset_slug(asset)}_pnl_history.json"


PNL_FILES: list[str] = [asset_pnl_filename(a) for a in TRADING_ASSETS]
TOTAL_BOTS: int = len(TRADING_ASSETS)


def binance_futures_symbol(asset: str) -> str:
    """Binance USDT-margined futures base symbol (e.g. btc → BTC)."""
    return normalize_asset_slug(asset).split("-")[0].upper()


def validate_trading_assets() -> Tuple[str, ...]:
    """Explicit startup hook; configuration is validated at import time."""
    if not TRADING_ASSETS:
        _fatal("TRADING_ASSETS resolved to an empty list.")
    return TRADING_ASSETS


def trading_assets_label(separator: str = " · ") -> str:
    return separator.join(TRADING_ASSETS_UPPER)


print(
    f"📌 Trading assets ({len(TRADING_ASSETS)}): "
    + ", ".join(TRADING_ASSETS_UPPER)
)
