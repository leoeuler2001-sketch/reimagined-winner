"""Strategy 2 — Binance momentum signals → Polymarket UP/DOWN execution."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

from config import binance_futures_symbol
from strategies.base import StrategyBase
from utils.market_resolver import fetch_market, interval_start, window_seconds

log = logging.getLogger("emiliano.momentum")

STALE_SECS = 5.0


class MomentumStrategy(StrategyBase):
    name = "momentum"

    def __init__(self, *args, binance_feed: Any = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.lookback_secs = float(self.config.get("lookback_secs", 2))
        self.entry_min_delta = float(self.config.get("entry_min_delta", 0.004))
        self.execution_mode = str(self.config.get("execution_mode", "single_taker"))
        self.momentum_size = float(self.config.get("momentum_size", 10))
        self._binance_feed = binance_feed
        self._price_buf: Deque[Tuple[float, float]] = deque(maxlen=500)
        self._cooldown_until_window: Optional[int] = None
        self._active_gtc: Dict[str, str] = {}
        self._lock = asyncio.Lock()

    def _current_window_start(self) -> int:
        return interval_start(int(time.time()), self.window)

    def _in_cooldown(self) -> bool:
        if self._cooldown_until_window is None:
            return False
        return self._current_window_start() <= self._cooldown_until_window

    def _set_cooldown(self) -> None:
        self._cooldown_until_window = self._current_window_start()

    def _price_at_lookback(self, now: float) -> Optional[float]:
        target = now - self.lookback_secs
        for ts, px in reversed(self._price_buf):
            if ts <= target:
                return px
        return None

    async def on_price(self, price: float, ts: float) -> None:
        if self.state == "stopped":
            return
        self._price_buf.append((ts, price))
        if not await self._gate():
            return
        if (time.time() - ts) > STALE_SECS:
            return
        if self._in_cooldown():
            return

        ref = self._price_at_lookback(ts)
        if ref is None or ref <= 0:
            return

        delta = (price - ref) / ref
        if abs(delta) < self.entry_min_delta:
            return

        async with self._lock:
            if self._in_cooldown():
                return
            market = fetch_market(self.asset, self.window)
            if not market:
                return

            signal_side = "UP" if delta > 0 else "DOWN"
            contra_side = "DOWN" if signal_side == "UP" else "UP"
            token_id = market["up_id"] if signal_side == "UP" else market["down_id"]
            contra_id = market["down_id"] if signal_side == "UP" else market["up_id"]

            book = await asyncio.to_thread(self.clob.fetch_book, token_id)
            ask = self.clob.best_ask(book)
            bid = self.clob.best_bid(book)
            if ask <= 0:
                return

            self.last_signal_time = time.time()
            log.info(
                "[%s] momentum signal delta=%+.4f → %s @ %.4f mode=%s",
                self.key, delta, signal_side, ask, self.execution_mode,
            )

            filled = await self._execute(
                market, signal_side, contra_side, token_id, contra_id, ask, bid,
            )
            if filled:
                self._set_cooldown()
                self.record_trade()

    async def _execute(
        self,
        market: Dict[str, Any],
        signal_side: str,
        contra_side: str,
        token_id: str,
        contra_id: str,
        ask: float,
        bid: float,
    ) -> bool:
        mode = self.execution_mode
        size = self.momentum_size

        if mode == "single_taker":
            oid = await asyncio.to_thread(
                self.clob.post_limit, signal_side, token_id, ask, size, "FOK",
                f"{self.key}-mom",
            )
            if not oid:
                return False
            filled, _ = await asyncio.to_thread(self.clob.order_filled_qty, oid)
            if filled > 0:
                await self.positions.record_fill(
                    self.name, self.asset, self.window, signal_side, filled, ask,
                )
                return True
            log.info("[%s] FOK not filled — discarding", self.key)
            return False

        if mode == "gtc_at_ask":
            tag = f"{self.key}-gtc"
            old = self._active_gtc.get(signal_side)
            if old:
                await asyncio.to_thread(self.clob.cancel, old)
            oid = await asyncio.to_thread(
                self.clob.post_limit, signal_side, token_id, ask, size, "GTC", tag,
            )
            if oid:
                self._active_gtc[signal_side] = oid
            return oid is not None

        if mode == "single_maker":
            maker_px = round(min(bid, ask - 0.01), 4) if bid > 0 else round(ask - 0.01, 4)
            oid = await asyncio.to_thread(
                self.clob.post_limit, signal_side, token_id, maker_px, size, "GTC",
                f"{self.key}-maker",
            )
            return oid is not None

        if mode == "dual_hybrid":
            maker_bid = self.clob.best_bid(
                await asyncio.to_thread(self.clob.fetch_book, contra_id),
            )
            sig_task = asyncio.to_thread(
                self.clob.post_limit, signal_side, token_id, ask, size, "FOK",
                f"{self.key}-sig",
            )
            con_task = asyncio.to_thread(
                self.clob.post_limit, contra_side, contra_id,
                maker_bid if maker_bid > 0 else ask, size, "GTC",
                f"{self.key}-con",
            )
            sig_oid, con_oid = await asyncio.gather(sig_task, con_task)
            if sig_oid:
                filled, _ = await asyncio.to_thread(self.clob.order_filled_qty, sig_oid)
                if filled > 0:
                    await self.positions.record_fill(
                        self.name, self.asset, self.window, signal_side, filled, ask,
                    )
            return sig_oid is not None

        log.warning("[%s] unknown execution_mode=%s", self.key, mode)
        return False

    async def run(self) -> None:
        from bot import BinanceDepthSignal  # local import — reuse existing WS infra

        sym = binance_futures_symbol(self.asset)
        feed = self._binance_feed or await BinanceDepthSignal.get_or_create(sym)
        feed.subscribe_price(self.on_price)
        log.info("[%s] momentum subscribed to Binance %s", self.key, sym)

        while self.state != "stopped":
            try:
                if not await self._gate():
                    await asyncio.sleep(1.0)
                    continue
                if self.execution_mode == "gtc_at_ask":
                    for side, oid in list(self._active_gtc.items()):
                        filled, status = await asyncio.to_thread(
                            self.clob.order_filled_qty, oid,
                        )
                        if filled > 0:
                            await self.positions.record_fill(
                                self.name, self.asset, self.window, side, filled, 0.0,
                            )
                            self._active_gtc.pop(side, None)
                        elif status in ("cancelled", "canceled", "expired"):
                            self._active_gtc.pop(side, None)
                        else:
                            await asyncio.to_thread(self.clob.cancel, oid)
                            self._active_gtc.pop(side, None)
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("[%s] loop error: %s", self.key, e)
                await asyncio.sleep(2.0)
