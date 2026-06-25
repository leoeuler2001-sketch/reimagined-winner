"""Strategy 3 — market making with Binance-driven cancel/requote."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from config import binance_futures_symbol
from strategies.base import StrategyBase
from utils.market_resolver import fetch_market
from utils.merge import merge_position

log = logging.getLogger("emiliano.market_making")


class MarketMakingStrategy(StrategyBase):
    name = "market_making"

    def __init__(self, *args, binance_feed: Any = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.max_buy_order_size = float(self.config.get("max_buy_order_size", 20))
        self.cancel_threshold = float(self.config.get("cancel_threshold", 0.003))
        self.imbalance_threshold = float(self.config.get("imbalance_threshold", 30))
        self.requote_delay_ms = int(self.config.get("requote_delay_ms", 500))
        self._binance_feed = binance_feed
        self._resting: Dict[str, str] = {}
        self._prev_mid: float = 0.0
        self._started = False
        self._lock = asyncio.Lock()

    def _quote_size(self, side: str) -> float:
        imb = self.positions.imbalance(self.name, self.asset, self.window)
        base = self.max_buy_order_size
        if abs(imb) <= self.imbalance_threshold:
            return base
        if side == "UP" and imb < 0:
            return base * 1.5
        if side == "DOWN" and imb > 0:
            return base * 1.5
        return base * 0.75

    async def _post_resting_bids(self, market: Dict[str, Any]) -> None:
        for side, tid_key in (("UP", "up_id"), ("DOWN", "down_id")):
            book = await asyncio.to_thread(self.clob.fetch_book, market[tid_key])
            bid = self.clob.best_bid(book)
            if bid <= 0:
                continue
            size = self._quote_size(side)
            tag = f"{self.key}-{side.lower()}"
            oid = await asyncio.to_thread(
                self.clob.post_limit, side, market[tid_key], bid, size, "GTC", tag,
            )
            if oid:
                self._resting[side] = oid
                log.info("[%s] posted %s bid @ %.4f size=%.1f", self.key, side, bid, size)

    async def _cancel_side(self, side: str) -> None:
        oid = self._resting.pop(side, None)
        if oid:
            await asyncio.to_thread(self.clob.cancel, oid)
            log.info("[%s] cancelled resting %s bid", self.key, side)

    async def _requote_side(self, market: Dict[str, Any], side: str) -> None:
        await asyncio.sleep(self.requote_delay_ms / 1000.0)
        tid_key = "up_id" if side == "UP" else "down_id"
        book = await asyncio.to_thread(self.clob.fetch_book, market[tid_key])
        bid = self.clob.best_bid(book)
        if bid <= 0:
            return
        size = self._quote_size(side)
        tag = f"{self.key}-{side.lower()}-rq"
        oid = await asyncio.to_thread(
            self.clob.post_limit, side, market[tid_key], bid, size, "GTC", tag,
        )
        if oid:
            self._resting[side] = oid

    async def _poll_fills(self, market: Dict[str, Any]) -> None:
        for side, oid in list(self._resting.items()):
            filled, status = await asyncio.to_thread(self.clob.order_filled_qty, oid)
            if filled > 0:
                book = await asyncio.to_thread(
                    self.clob.fetch_book,
                    market["up_id"] if side == "UP" else market["down_id"],
                )
                px = self.clob.best_bid(book) or 0.5
                await self.positions.record_fill(
                    self.name, self.asset, self.window, side, filled, px,
                )
                self.record_trade()
            if status == "filled":
                self._resting.pop(side, None)

        matched = self.positions.matched_qty(self.name, self.asset, self.window)
        self.last_imbalance = self.positions.imbalance(
            self.name, self.asset, self.window,
        )
        if matched >= 1.0:
            await merge_position(
                self.account, self.asset, self.window, matched, market,
                dry_run=self.dry_run,
            )
            await self.positions.reduce_leg(
                self.name, self.asset, self.window, "UP", matched,
            )
            await self.positions.reduce_leg(
                self.name, self.asset, self.window, "DOWN", matched,
            )
            log.info("[%s] merged %.2f from MM inventory", self.key, matched)

    async def on_price(self, price: float, ts: float) -> None:
        if not self._started or self.state == "stopped":
            return
        if not await self._gate():
            return
        if self._prev_mid <= 0:
            self._prev_mid = price
            return

        delta = (price - self._prev_mid) / self._prev_mid
        self._prev_mid = price
        if abs(delta) < self.cancel_threshold:
            return

        market = fetch_market(self.asset, self.window)
        if not market:
            return

        self.last_signal_time = time.time()
        async with self._lock:
            if delta > 0:
                log.info("[%s] BTC pump delta=%+.4f — cancel DOWN bid", self.key, delta)
                await self._cancel_side("DOWN")
                await self._requote_side(market, "DOWN")
            else:
                log.info("[%s] BTC dump delta=%+.4f — cancel UP bid", self.key, delta)
                await self._cancel_side("UP")
                await self._requote_side(market, "UP")

    async def run(self) -> None:
        from bot import BinanceDepthSignal

        sym = binance_futures_symbol(self.asset)
        feed = self._binance_feed or await BinanceDepthSignal.get_or_create(sym)
        feed.subscribe_price(self.on_price)
        log.info("[%s] market making subscribed to Binance %s", self.key, sym)

        while self.state != "stopped":
            try:
                if not await self._gate():
                    await asyncio.sleep(1.0)
                    continue

                market = fetch_market(self.asset, self.window)
                if not market:
                    await asyncio.sleep(2.0)
                    continue

                if not self._started:
                    await self._post_resting_bids(market)
                    self._started = True
                    self._prev_mid = feed.mid_price or 0.0

                await self._poll_fills(market)
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("[%s] loop error: %s", self.key, e)
                await asyncio.sleep(2.0)
