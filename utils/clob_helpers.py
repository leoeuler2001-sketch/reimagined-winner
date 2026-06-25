"""CLOB orderbook + order helpers wrapping AccountService."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

import requests
from py_clob_client_v2.clob_types import OrderType

log = logging.getLogger("emiliano.clob")

HOST = "https://clob.polymarket.com"


class ClobHelper:
    def __init__(self, account: Any, *, dry_run: bool = False) -> None:
        self.account = account
        self.dry_run = dry_run
        self._resting_orders: Dict[str, str] = {}

    def fetch_book(self, token_id: str) -> Dict[str, Any]:
        try:
            resp = requests.get(
                f"{HOST}/book",
                params={"token_id": token_id},
                timeout=4,
            )
            return resp.json() if resp.ok else {}
        except Exception as e:
            log.warning("book fetch failed token=%s: %s", token_id, e)
            return {}

    @staticmethod
    def best_bid(book: Dict[str, Any]) -> float:
        bids = book.get("bids") or []
        if not bids:
            return 0.0
        try:
            return float(bids[0].get("price", bids[0][0] if isinstance(bids[0], list) else 0))
        except (TypeError, ValueError, IndexError):
            return 0.0

    @staticmethod
    def best_ask(book: Dict[str, Any]) -> float:
        asks = book.get("asks") or []
        if not asks:
            return 0.0
        try:
            return float(asks[0].get("price", asks[0][0] if isinstance(asks[0], list) else 0))
        except (TypeError, ValueError, IndexError):
            return 0.0

    def post_limit(
        self,
        side: str,
        token_id: str,
        price: float,
        size: float,
        order_type: str = "GTC",
        tag: str = "",
    ) -> Optional[str]:
        side_str = "BUY" if side.upper() in ("BUY", "UP", "DOWN", "YES", "NO") else "SELL"
        if self.dry_run:
            log.info(
                "[DRY RUN] post_limit %s token=%s price=%.4f size=%.2f type=%s tag=%s",
                side_str, token_id, price, size, order_type, tag,
            )
            return f"dry-{tag}-{token_id[:8]}"

        ot = OrderType.FOK if order_type.upper() == "FOK" else OrderType.GTC
        resp = self.account.create_and_post_order(
            side_str, price, size, token_id, order_type=ot,
        )
        order_id = None
        if isinstance(resp, dict):
            order_id = resp.get("orderID") or resp.get("order_id")
        elif isinstance(resp, str):
            try:
                order_id = json.loads(resp).get("orderID")
            except Exception:
                pass
        if order_id and tag:
            self._resting_orders[tag] = order_id
        return order_id

    def cancel(self, order_id: str) -> bool:
        if self.dry_run:
            log.info("[DRY RUN] cancel order_id=%s", order_id)
            return True
        try:
            self.account.cancel_order(order_id)
            return True
        except Exception as e:
            log.warning("cancel failed %s: %s", order_id, e)
            return False

    def cancel_tag(self, tag: str) -> None:
        oid = self._resting_orders.pop(tag, None)
        if oid:
            self.cancel(oid)

    def order_filled_qty(self, order_id: str) -> Tuple[float, str]:
        if self.dry_run and order_id.startswith("dry-"):
            return 10.0, "filled"
        try:
            info = self.account.get_order_status(order_id)
            if isinstance(info, str):
                info = json.loads(info)
            if not isinstance(info, dict):
                return 0.0, "unknown"
            status = str(info.get("status", "")).lower()
            matched = float(info.get("size_matched", 0) or 0)
            return matched, status
        except Exception:
            return 0.0, "error"

    def cancel_all_resting(self) -> int:
        n = 0
        for tag, oid in list(self._resting_orders.items()):
            if self.cancel(oid):
                n += 1
            self._resting_orders.pop(tag, None)
        return n
