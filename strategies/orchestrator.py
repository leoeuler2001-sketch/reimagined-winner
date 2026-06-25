"""Launch and manage all strategy tasks."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any, Dict, List, Optional

from config import binance_futures_symbol
from strategy_settings import STRATEGY_CONFIG
from strategies.base import StrategyBase
from strategies.market_making import MarketMakingStrategy
from strategies.momentum import MomentumStrategy
from strategies.spread_capture import SpreadCaptureStrategy
from utils.clob_helpers import ClobHelper
from utils.position_tracker import PositionTracker
from utils.stop_loss import GlobalStopLoss

log = logging.getLogger("emiliano.orchestrator")

_orchestrator: Optional["StrategyOrchestrator"] = None


class StrategyOrchestrator:
    def __init__(self, account: Any, redis_get_json, redis_set_json, redis_available: bool) -> None:
        self.account = account
        self.cfg = STRATEGY_CONFIG
        self.dry_run = bool(self.cfg.get("dry_run", False))
        self.shutdown = asyncio.Event()

        self.positions = PositionTracker(redis_get_json, redis_set_json, redis_available)
        self.clob = ClobHelper(account, dry_run=self.dry_run)

        sl_cfg = self.cfg.get("stop_loss", {})
        self.stop_loss = GlobalStopLoss(
            float(sl_cfg.get("stop_loss_usd", 50)),
            int(sl_cfg.get("stop_loss_cooldown_secs", 300)),
            self._cancel_all_orders,
        )
        self.stop_loss.set_unrealized_provider(lambda: 0.0)

        self.instances: List[StrategyBase] = []
        self._tasks: List[asyncio.Task] = []
        self._binance_feeds: Dict[str, Any] = {}

    async def _cancel_all_orders(self) -> int:
        return await asyncio.to_thread(self.clob.cancel_all_resting)

    async def _ensure_binance_feeds(self) -> None:
        from bot import BinanceDepthSignal

        assets = [a.upper() for a in self.cfg.get("assets", [])]
        for asset in assets:
            sym = binance_futures_symbol(asset)
            if sym not in self._binance_feeds:
                self._binance_feeds[sym] = await BinanceDepthSignal.get_or_create(sym)
                log.info("Binance feed ready: %s", sym)

    def _make(
        self,
        cls: type,
        asset: str,
        window: str,
        strat_cfg: Dict[str, Any],
        **extra,
    ) -> StrategyBase:
        return cls(
            asset,
            window,
            self.account,
            self.positions,
            self.stop_loss,
            self.clob,
            strat_cfg,
            dry_run=self.dry_run,
            **extra,
        )

    def build_instances(self) -> List[StrategyBase]:
        out: List[StrategyBase] = []
        assets = self.cfg.get("assets", ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB"])
        windows = self.cfg.get("windows", ["5m", "15m", "1hr"])
        sc = self.cfg.get("strategies", {})

        if sc.get("spread_capture", {}).get("enabled", True):
            for a in assets:
                for w in windows:
                    out.append(self._make(
                        SpreadCaptureStrategy, a, w, sc["spread_capture"],
                    ))

        if sc.get("momentum", {}).get("enabled", True):
            for a in assets:
                for w in windows:
                    sym = binance_futures_symbol(a)
                    feed = self._binance_feeds.get(sym)
                    out.append(self._make(
                        MomentumStrategy, a, w, sc["momentum"],
                        binance_feed=feed,
                    ))

        if sc.get("market_making", {}).get("enabled", True):
            for a in assets:
                for w in windows:
                    sym = binance_futures_symbol(a)
                    feed = self._binance_feeds.get(sym)
                    out.append(self._make(
                        MarketMakingStrategy, a, w, sc["market_making"],
                        binance_feed=feed,
                    ))

        return out

    async def start(self) -> None:
        await self._ensure_binance_feeds()
        self.instances = self.build_instances()
        log.info("Launching %d strategy tasks (dry_run=%s)", len(self.instances), self.dry_run)
        for inst in self.instances:
            self._tasks.append(asyncio.create_task(inst.run(), name=inst.key))

    async def stop(self) -> None:
        self.shutdown.set()
        for inst in self.instances:
            inst.stop()
        await self._cancel_all_orders()
        await self.positions.flush()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def get_by_name(self, name: str) -> List[StrategyBase]:
        return [i for i in self.instances if i.name == name]

    def status_rows(self) -> List[Dict[str, Any]]:
        return [i.status_row() for i in self.instances]

    def pause_strategy(self, name: str) -> int:
        n = 0
        for i in self.get_by_name(name):
            i.pause()
            n += 1
        return n

    def resume_strategy(self, name: str) -> int:
        n = 0
        for i in self.get_by_name(name):
            i.resume()
            n += 1
        return n


async def init_orchestrator(
    account: Any,
    redis_get_json,
    redis_set_json,
    redis_available: bool,
) -> StrategyOrchestrator:
    global _orchestrator
    _orchestrator = StrategyOrchestrator(
        account, redis_get_json, redis_set_json, redis_available,
    )
    return _orchestrator


def get_orchestrator() -> Optional[StrategyOrchestrator]:
    return _orchestrator


def install_sigterm_handler(loop: asyncio.AbstractEventLoop) -> None:
    def _handler(*_args):
        log.warning("SIGTERM received — shutting down strategies")
        orch = get_orchestrator()
        if orch:
            loop.create_task(orch.stop())

    try:
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, OSError):
        pass
