"""In-memory position ledger with Redis persistence (emiliano:positions)."""

from __future__ import annotations

import asyncio
import copy
import time
from typing import Any, Callable, Dict, Optional

from utils.redis_rate_limit import RateLimitedRedisWriter

POSITIONS_REDIS_KEY = "emiliano:positions"


class PositionTracker:
    """
    Structure:
      {strategy: {asset: {window: {UP: {qty, avg_cost}, DOWN: {qty, avg_cost}}}}}
    """

    def __init__(
        self,
        redis_get_json: Callable[[str], Any],
        redis_set_json: Callable[[str, Any], bool],
        redis_available: bool,
    ) -> None:
        self._get_json = redis_get_json
        self._writer = RateLimitedRedisWriter(redis_set_json, min_interval_sec=1.0)
        self._redis_available = redis_available
        self._lock = asyncio.Lock()
        self._data: Dict[str, Dict[str, Dict[str, Dict[str, Dict[str, float]]]]] = {}
        self._realized_pnl: Dict[str, float] = {
            "spread_capture": 0.0,
            "momentum": 0.0,
            "market_making": 0.0,
        }
        self._load()

    def _load(self) -> None:
        if not self._redis_available:
            return
        raw = self._get_json(POSITIONS_REDIS_KEY)
        if isinstance(raw, dict):
            self._data = raw.get("inventory", {}) or {}
            rp = raw.get("realized_pnl", {})
            if isinstance(rp, dict):
                for k in self._realized_pnl:
                    if k in rp:
                        self._realized_pnl[k] = float(rp[k])

    async def _persist(self, *, force: bool = False) -> None:
        if not self._redis_available:
            return
        payload = {
            "inventory": self._data,
            "realized_pnl": self._realized_pnl,
            "updated_at": time.time(),
        }
        await self._writer.set_json(POSITIONS_REDIS_KEY, payload, force=force)

    async def flush(self) -> None:
        await self._writer.flush_pending()
        await self._persist(force=True)

    def _ensure(self, strategy: str, asset: str, window: str) -> Dict[str, Dict[str, float]]:
        s = self._data.setdefault(strategy, {})
        a = s.setdefault(asset.upper(), {})
        w = a.setdefault(window, {})
        w.setdefault("UP", {"qty": 0.0, "avg_cost": 0.0})
        w.setdefault("DOWN", {"qty": 0.0, "avg_cost": 0.0})
        return w

    async def record_fill(
        self,
        strategy: str,
        asset: str,
        window: str,
        side: str,
        qty: float,
        price: float,
    ) -> None:
        if qty <= 0 or price <= 0:
            return
        side = side.upper()
        if side not in ("UP", "DOWN"):
            return
        async with self._lock:
            leg = self._ensure(strategy, asset, window)[side]
            old_q = leg["qty"]
            new_q = old_q + qty
            if new_q > 0:
                leg["avg_cost"] = ((old_q * leg["avg_cost"]) + (qty * price)) / new_q
            leg["qty"] = new_q
        await self._persist()

    async def record_realized(self, strategy: str, pnl: float) -> None:
        async with self._lock:
            self._realized_pnl[strategy] = round(
                self._realized_pnl.get(strategy, 0.0) + pnl, 4
            )
        await self._persist()

    async def reduce_leg(
        self, strategy: str, asset: str, window: str, side: str, qty: float,
    ) -> None:
        side = side.upper()
        async with self._lock:
            leg = self._ensure(strategy, asset, window).get(side)
            if leg:
                leg["qty"] = max(0.0, leg["qty"] - qty)
        await self._persist()

    def get_leg(
        self, strategy: str, asset: str, window: str, side: str,
    ) -> Dict[str, float]:
        side = side.upper()
        try:
            return dict(self._data[strategy][asset.upper()][window][side])
        except KeyError:
            return {"qty": 0.0, "avg_cost": 0.0}

    def imbalance(self, strategy: str, asset: str, window: str) -> float:
        w = self._data.get(strategy, {}).get(asset.upper(), {}).get(window, {})
        up = w.get("UP", {}).get("qty", 0.0)
        dn = w.get("DOWN", {}).get("qty", 0.0)
        return up - dn

    def matched_qty(self, strategy: str, asset: str, window: str) -> float:
        w = self._data.get(strategy, {}).get(asset.upper(), {}).get(window, {})
        up = w.get("UP", {}).get("qty", 0.0)
        dn = w.get("DOWN", {}).get("qty", 0.0)
        return min(up, dn)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "inventory": copy.deepcopy(self._data),
            "realized_pnl": dict(self._realized_pnl),
        }

    def total_realized(self) -> float:
        return round(sum(self._realized_pnl.values()), 4)
