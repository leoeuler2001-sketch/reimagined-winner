"""Base class for asset×window strategy tasks."""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Deque, Dict, Optional

from utils.clob_helpers import ClobHelper
from utils.position_tracker import PositionTracker
from utils.stop_loss import GlobalStopLoss


class StrategyBase(ABC):
    name: str = "base"

    def __init__(
        self,
        asset: str,
        window: str,
        account: Any,
        positions: PositionTracker,
        stop_loss: GlobalStopLoss,
        clob: ClobHelper,
        config: Dict[str, Any],
        *,
        dry_run: bool = False,
    ) -> None:
        self.asset = asset.upper()
        self.window = window
        self.account = account
        self.positions = positions
        self.stop_loss = stop_loss
        self.clob = clob
        self.config = config
        self.dry_run = dry_run
        self.state = "running"
        self._pause = asyncio.Event()
        self._pause.set()
        self.trades_last_5m: Deque[float] = deque(maxlen=500)
        self.last_signal_time: Optional[float] = None
        self.last_imbalance: float = 0.0

    @property
    def key(self) -> str:
        return f"{self.name}:{self.asset}:{self.window}"

    def pause(self) -> None:
        self.state = "paused"
        self._pause.clear()

    def resume(self) -> None:
        self.state = "running"
        self._pause.set()

    def stop(self) -> None:
        self.state = "stopped"

    def record_trade(self) -> None:
        self.trades_last_5m.append(time.time())

    def trades_in_last_5m(self) -> int:
        cutoff = time.time() - 300
        return sum(1 for t in self.trades_last_5m if t >= cutoff)

    def status_row(self) -> Dict[str, Any]:
        return {
            "strategy": self.name,
            "asset": self.asset,
            "window": self.window,
            "state": self.state if not self.stop_loss.shutdown else "stopped",
            "trades_5m": self.trades_in_last_5m(),
            "imbalance": round(self.last_imbalance, 2),
            "last_signal": self.last_signal_time,
        }

    async def _gate(self) -> bool:
        if self.state == "stopped":
            return False
        await self._pause.wait()
        if self.stop_loss.shutdown:
            return False
        if not await self.stop_loss.check(self.positions):
            return False
        return True

    @abstractmethod
    async def run(self) -> None:
        ...
