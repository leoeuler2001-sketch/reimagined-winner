"""Resolve active Polymarket up/down markets per asset and time window."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests

GAMMA_URL = "https://gamma-api.polymarket.com/markets"

WINDOW_SPECS: Dict[str, Tuple[int, str]] = {
    "5m":  (300,  "5m"),
    "15m": (900,  "15m"),
    "1hr": (3600, "1h"),
}


def window_seconds(window: str) -> int:
    spec = WINDOW_SPECS.get(window.lower())
    if not spec:
        raise ValueError(f"unsupported window {window!r}")
    return spec[0]


def window_slug_label(window: str) -> str:
    return WINDOW_SPECS[window.lower()][1]


def interval_start(ts: int, window: str) -> int:
    dur = window_seconds(window)
    return (ts // dur) * dur


def market_slug(asset: str, window: str, start_ts: int) -> str:
    label = window_slug_label(window)
    return f"{asset.lower()}-updown-{label}-{start_ts}"


def fetch_market(asset: str, window: str) -> Optional[Dict[str, Any]]:
    """Return market metadata for the current window, or None."""
    now_ts = int(datetime.now(timezone.utc).timestamp())
    start  = interval_start(now_ts, window)
    slug   = market_slug(asset, window, start)
    try:
        resp = requests.get(f"{GAMMA_URL}?slug={slug}", timeout=4).json()
        if not resp:
            return None
        m = resp[0]
        end_date_str = m.get("endDate")
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        if datetime.now(timezone.utc) >= end_dt:
            return None
        clob_ids = m.get("clobTokenIds")
        if isinstance(clob_ids, str):
            clob_ids = json.loads(clob_ids)
        condition_id = m.get("conditionId") or m.get("condition_id")
        return {
            "question":     m.get("question"),
            "slug":         slug,
            "yes_id":       str(clob_ids[0]),
            "no_id":        str(clob_ids[1]),
            "up_id":        str(clob_ids[0]),
            "down_id":      str(clob_ids[1]),
            "expiry":       end_dt,
            "condition_id": condition_id,
            "start_ts":     start,
            "window":       window,
            "asset":        asset.upper(),
        }
    except Exception:
        return None
