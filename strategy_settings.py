"""Load strategy_config.yaml with optional env overrides."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

_DEFAULT: Dict[str, Any] = {
    "strategies": {
        "spread_capture": {
            "enabled": True,
            "spread_threshold": 0.99,
            "trade_cooldown_ms": 5000,
            "max_position_size": 10,
        },
        "momentum": {
            "enabled": True,
            "lookback_secs": 2,
            "entry_min_delta": 0.004,
            "execution_mode": "single_taker",
            "momentum_size": 10,
        },
        "market_making": {
            "enabled": True,
            "max_buy_order_size": 20,
            "cancel_threshold": 0.003,
            "imbalance_threshold": 30,
            "requote_delay_ms": 500,
        },
    },
    "stop_loss": {
        "stop_loss_usd": 50,
        "stop_loss_cooldown_secs": 300,
    },
    "assets": ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB"],
    "windows": ["5m", "15m", "1hr"],
    "dry_run": False,
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_strategy_config(path: str | None = None) -> Dict[str, Any]:
    cfg_path = Path(path or os.getenv("STRATEGY_CONFIG", "strategy_config.yaml"))
    data: Dict[str, Any] = dict(_DEFAULT)
    if cfg_path.exists() and yaml is not None:
        with open(cfg_path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        if isinstance(loaded, dict):
            data = _deep_merge(data, loaded)
    elif cfg_path.exists() and yaml is None:
        print("⚠️  PyYAML not installed — using built-in strategy defaults.")

    if os.getenv("DRY_MODE", "").lower() == "true":
        data["dry_run"] = True
    if os.getenv("STRATEGY_DRY_RUN", "").lower() in ("true", "1", "yes"):
        data["dry_run"] = True

    return data


STRATEGY_CONFIG: Dict[str, Any] = load_strategy_config()
