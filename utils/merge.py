"""
On-chain merge for matched UP+DOWN inventory.

TODO: Wire full ProxyWallet Factory ABI when contract address is confirmed.
Expected interface:
  merge_position(asset, window, qty, market_meta) -> float pUSD recovered
Inputs: asset slug, window label, share qty, market dict (condition_id, yes_id, no_id)
Output: pUSD amount recovered (float)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

log = logging.getLogger("emiliano.merge")


async def merge_position(
    account: Any,
    asset: str,
    window: str,
    qty: float,
    market: Optional[Dict[str, Any]] = None,
    *,
    dry_run: bool = False,
) -> float:
    """
    Burn matched UP+DOWN tokens via on-chain mergePositions.
    Delegates to AccountService.merge_shares when market metadata is available.
    """
    if qty <= 0:
        return 0.0

    if dry_run:
        log.info(
            "[DRY RUN] merge_position asset=%s window=%s qty=%.4f",
            asset, window, qty,
        )
        return float(qty)

    if market is None:
        log.warning(
            "merge_position stub: no market metadata for %s/%s — "
            "TODO: resolve condition_id via market_resolver",
            asset, window,
        )
        return 0.0

    try:
        await account.merge_shares(market, qty)
        log.info("merge_position ok asset=%s window=%s qty=%.4f", asset, window, qty)
        return float(qty)
    except Exception as e:
        log.error("merge_position failed: %s", e)
        return 0.0
