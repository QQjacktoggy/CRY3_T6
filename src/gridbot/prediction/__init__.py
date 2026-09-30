"""Isolated Binance Web3 Wallet Prediction trading domain.

The package deliberately has no imports from the existing Futures runtime.  It
can therefore be shadowed, tested, and eventually wired into the application
without sharing state with the legacy bot.
"""

from .models import (
    ActionType,
    Campaign,
    CampaignState,
    Fill,
    MarketInfo,
    OutcomeSide,
    OrderIntent,
    OrderStatus,
    OrderType,
    Position,
    QuoteSnapshot,
)
from .risk import RiskConfig, RiskDecision, RiskEngine, RiskSnapshot
from .strategy import PnlResult, StrategyConfig, StrategyDecision, PredictionStateMachine, calculate_pnl
from .controller import PredictionController
from .worker import PredictionWorker, WorkerHeartbeat

__all__ = [
    "ActionType",
    "Campaign",
    "CampaignState",
    "Fill",
    "MarketInfo",
    "OutcomeSide",
    "OrderIntent",
    "OrderStatus",
    "OrderType",
    "Position",
    "QuoteSnapshot",
    "PnlResult",
    "calculate_pnl",
    "RiskConfig",
    "RiskDecision",
    "RiskEngine",
    "RiskSnapshot",
    "StrategyConfig",
    "StrategyDecision",
    "PredictionStateMachine",
    "PredictionController",
    "PredictionWorker",
    "WorkerHeartbeat",
]
