"""Strategy 1 — spread capture via dual-sided maker bids on Polymarket CLOB."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

from strategies.base import StrategyBase
from utils.market_resolver import fetch_market
from utils.merge import merge_position

log = logging.getLogger("emiliano.spread_capture")


class SpreadCaptureStrategy(StrategyBase):
    name = "spread_capture"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.spread_threshold = float(self.config.get("spread_threshold", 0.99))
        self.trade_cooldown_ms = int(self.config.get("trade_cooldown_ms", 5000))
        self.max_position_size = float(self.config.get("max_position_size", 10))
        self._price_bias: Dict[str, float] = {"UP": 0.0, "DOWN": 0.0}
        self._pending_orders: Dict[str, str] = {}

    def _biased_bid(self, side: str, raw_bid: float) -> float:
        bias = self._price_bias.get(side, 0.0)
        return round(max(0.01, raw_bid + bias), 4)

    async def _maybe_merge(self, market: Dict[str, Any]) -> None:
        matched = self.positions.matched_qty(self.name, self.asset, self.window)
        if matched < 0.01:
            return
        await merge_position(
            self.account, self.asset, self.window, matched, market,
            dry_run=self.dry_run,
        )
        await self.positions.reduce_leg(self.name, self.asset, self.window, "UP", matched)
        await self.positions.reduce_leg(self.name, self.asset, self.window, "DOWN", matched)
        self.record_trade()
        log.info("[%s] merged %.2f matched shares", self.key, matched)

    async def _handle_fill_imbalance(
        self, market: Dict[str, Any], up_filled: float, down_filled: float,
    ) -> None:
        imb = up_filled - down_filled
        self.last_imbalance = imb
        if abs(imb) < 0.01:
            self._price_bias = {"UP": 0.0, "DOWN": 0.0}
            return
        if imb > 0:
            self._price_bias = {"UP": 0.0, "DOWN": 0.01}
            log.info("[%s] price_bias +0.01 DOWN (underweight)", self.key)
        else:
            self._price_bias = {"UP": 0.01, "DOWN": 0.0}
            log.info("[%s] price_bias +0.01 UP (underweight)", self.key)

    async def _check_pending(self, market: Dict[str, Any]) -> None:
        up_oid = self._pending_orders.get("UP")
        down_oid = self._pending_orders.get("DOWN")
        if not up_oid and not down_oid:
            return

        up_filled, up_st = (0.0, "none")
        down_filled, down_st = (0.0, "none")
        if up_oid:
            up_filled, up_st = await asyncio.to_thread(
                self.clob.order_filled_qty, up_oid,
            )
        if down_oid:
            down_filled, down_st = await asyncio.to_thread(
                self.clob.order_filled_qty, down_oid,
            )

        if up_filled > 0:
            await self.positions.record_fill(
                self.name, self.asset, self.window, "UP",
                up_filled, self._biased_bid("UP", 0.0) or 0.5,
            )
        if down_filled > 0:
            await self.positions.record_fill(
                self.name, self.asset, self.window, "DOWN",
                down_filled, self._biased_bid("DOWN", 0.0) or 0.5,
            )

        both_done = up_st in ("filled", "cancelled", "canceled", "none", "unknown") and \
                    down_st in ("filled", "cancelled", "canceled", "none", "unknown")

        if up_filled > 0.01 and down_filled > 0.01:
            log.info("[%s] both legs filled — merging", self.key)
            await self._maybe_merge(market)
            self._pending_orders.clear()
            self._price_bias = {"UP": 0.0, "DOWN": 0.0}
            return

        if up_filled > 0.01 and down_filled <= 0.01 and down_oid:
            await asyncio.to_thread(self.clob.cancel, down_oid)
            self._pending_orders.pop("DOWN", None)
            await self._handle_fill_imbalance(market, up_filled, down_filled)
        elif down_filled > 0.01 and up_filled <= 0.01 and up_oid:
            await asyncio.to_thread(self.clob.cancel, up_oid)
            self._pending_orders.pop("UP", None)
            await self._handle_fill_imbalance(market, up_filled, down_filled)
        elif up_filled <= 0.01 and down_filled <= 0.01 and both_done:
            for oid in (up_oid, down_oid):
                if oid:
                    await asyncio.to_thread(self.clob.cancel, oid)
            self._pending_orders.clear()
            log.info("[%s] neither leg filled — reset", self.key)

    async def run(self) -> None:
        log.info("[%s] spread capture started threshold=%.2f", self.key, self.spread_threshold)
        while self.state != "stopped":
            try:
                if not await self._gate():
                    await asyncio.sleep(1.0)
                    continue

                market = fetch_market(self.asset, self.window)
                if not market:
                    await asyncio.sleep(2.0)
                    continue

                if self._pending_orders:
                    await self._check_pending(market)
                    await asyncio.sleep(self.trade_cooldown_ms / 1000.0)
                    continue

                up_book = await asyncio.to_thread(
                    self.clob.fetch_book, market["up_id"],
                )
                down_book = await asyncio.to_thread(
                    self.clob.fetch_book, market["down_id"],
                )
                up_bid = self.clob.best_bid(up_book)
                down_bid = self.clob.best_bid(down_book)
                if up_bid <= 0 or down_bid <= 0:
                    await asyncio.sleep(1.0)
                    continue

                spread_sum = up_bid + down_bid
                if spread_sum >= self.spread_threshold:
                    await asyncio.sleep(1.0)
                    continue

                up_price = self._biased_bid("UP", up_bid)
                down_price = self._biased_bid("DOWN", down_bid)
                size = self.max_position_size
                log.info(
                    "[%s] spread opportunity UP=%.4f DOWN=%.4f sum=%.4f",
                    self.key, up_price, down_price, up_price + down_price,
                )
                self.last_signal_time = time.time()

                up_oid = await asyncio.to_thread(
                    self.clob.post_limit,
                    "UP", market["up_id"], up_price, size, "GTC", f"{self.key}-up",
                )
                down_oid = await asyncio.to_thread(
                    self.clob.post_limit,
                    "DOWN", market["down_id"], down_price, size, "GTC", f"{self.key}-down",
                )
                if up_oid:
                    self._pending_orders["UP"] = up_oid
                if down_oid:
                    self._pending_orders["DOWN"] = down_oid

                await asyncio.sleep(self.trade_cooldown_ms / 1000.0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("[%s] tick error: %s", self.key, e)
                await asyncio.sleep(2.0)
