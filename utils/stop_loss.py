"""Global stop-loss across all strategies."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional

from utils.position_tracker import PositionTracker


class GlobalStopLoss:
    def __init__(
        self,
        stop_loss_usd: float,
        cooldown_secs: int,
        cancel_all_orders_fn: Callable[[], Any],
    ) -> None:
        self.stop_loss_usd = stop_loss_usd
        self.cooldown_secs = cooldown_secs
        self._cancel_all = cancel_all_orders_fn
        self._lock = asyncio.Lock()
        self.shutdown = False
        self._cooldown_until: float = 0.0
        self._unrealized_fn: Optional[Callable[[], float]] = None

    def set_unrealized_provider(self, fn: Callable[[], float]) -> None:
        self._unrealized_fn = fn

    @property
    def in_cooldown(self) -> bool:
        return time.time() < self._cooldown_until

    @property
    def state(self) -> str:
        if self.shutdown:
            return "stopped"
        if self.in_cooldown:
            return "cooldown"
        return "armed"

    async def check(self, positions: PositionTracker) -> bool:
        """Return True if trading should proceed. False triggers shutdown."""
        if self.shutdown and not self.in_cooldown:
            return False
        if self.in_cooldown:
            if time.time() >= self._cooldown_until:
                async with self._lock:
                    self.shutdown = False
                    self._cooldown_until = 0.0
                print("🟢 [STOP-LOSS] Cooldown expired — strategies may resume.")
            else:
                return False

        realized = positions.total_realized()
        unrealized = self._unrealized_fn() if self._unrealized_fn else 0.0
        total = realized + unrealized
        if total <= -abs(self.stop_loss_usd):
            await self.trigger(total)
            return False
        return True

    async def trigger(self, total_pnl: float) -> None:
        async with self._lock:
            if self.shutdown:
                return
            self.shutdown = True
            self._cooldown_until = time.time() + self.cooldown_secs
        print(
            f"🛑 [STOP-LOSS] Total PnL ${total_pnl:.2f} breached "
            f"-${self.stop_loss_usd:.2f} — cancelling orders, pausing signals."
        )
        try:
            result = self._cancel_all()
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            print(f"⚠️ [STOP-LOSS] cancel_all_orders error: {e}")

    async def manual_stop(self) -> None:
        await self.trigger(0.0)

    def get_pnl_summary(self, positions: PositionTracker) -> Dict[str, Any]:
        realized = positions.snapshot()["realized_pnl"]
        unrealized = self._unrealized_fn() if self._unrealized_fn else 0.0
        total_realized = sum(realized.values())
        return {
            "realized_by_strategy": realized,
            "total_realized": round(total_realized, 4),
            "total_unrealized": round(unrealized, 4),
            "total_pnl": round(total_realized + unrealized, 4),
            "stop_loss_usd": self.stop_loss_usd,
            "stop_loss_state": self.state,
            "shutdown": self.shutdown,
            "cooldown_remaining_sec": max(0, int(self._cooldown_until - time.time())),
        }
