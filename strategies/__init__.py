"""Multi-strategy trading modules."""

from strategies.orchestrator import (
    StrategyOrchestrator,
    get_orchestrator,
    init_orchestrator,
    install_sigterm_handler,
)
from strategies.spread_capture import SpreadCaptureStrategy
from strategies.momentum import MomentumStrategy
from strategies.market_making import MarketMakingStrategy

__all__ = [
    "StrategyOrchestrator",
    "get_orchestrator",
    "init_orchestrator",
    "install_sigterm_handler",
    "SpreadCaptureStrategy",
    "MomentumStrategy",
    "MarketMakingStrategy",
]
