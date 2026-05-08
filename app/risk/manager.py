from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.config import Settings
from app.models import Outcome, Signal, SignalAction
from app.portfolio.manager import Portfolio


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reason: str
    size_usd: float = 0.0


class RiskManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.kill_switch = False
        self.paused = False
        self.cooldown_until: datetime | None = None

    def evaluate(self, signal: Signal, portfolio: Portfolio) -> RiskDecision:
        if self.kill_switch:
            return RiskDecision(False, "kill-switch")
        if self.paused:
            return RiskDecision(False, "paused")
        if signal.action == SignalAction.HOLD:
            return RiskDecision(False, signal.reason)
        if signal.quote_age_ms > self.settings.max_quote_age_ms:
            return RiskDecision(False, "quote_too_old")
        if self._stale_edge_share(signal) > self.settings.max_stale_edge_share:
            return RiskDecision(False, "stale_edge_dominant")
        now = datetime.now(UTC)
        if self.cooldown_until and now < self.cooldown_until:
            return RiskDecision(False, "cooldown")
        stats = portfolio.stats()
        if stats.consecutive_losses >= self.settings.max_consecutive_losses:
            self.cooldown_until = now + timedelta(seconds=self.settings.cooldown_seconds)
            return RiskDecision(False, "loss-streak cooldown")
        daily_loss_limit = -portfolio.starting_balance * self.settings.daily_loss_limit_pct
        if portfolio.daily_realized_pnl() <= daily_loss_limit:
            self.kill_switch = True
            return RiskDecision(False, "daily loss limit")
        if stats.max_drawdown_pct > self.settings.max_allowed_drawdown_pct:
            self.kill_switch = True
            return RiskDecision(False, "drawdown limit")
        market_trades = [trade for trade in portfolio.trades if trade.market_id == signal.market_id]
        if len(market_trades) >= self.settings.max_trades_per_market:
            return RiskDecision(False, "max trades per market")
        if market_trades:
            last_trade = max(market_trades, key=lambda trade: trade.timestamp)
            elapsed = (now - last_trade.timestamp).total_seconds()
            if elapsed < self.settings.market_trade_cooldown_seconds:
                return RiskDecision(False, "market trade cooldown")
        if signal.outcome is not None and self._has_opposite_position(signal, portfolio):
            return RiskDecision(False, "opposite position")
        if signal.outcome is not None and self._same_side_exposure(signal, portfolio) >= (
            portfolio.equity() * self.settings.max_same_side_exposure_pct
        ):
            return RiskDecision(False, "same-side exposure limit")
        if self._correlated_exposure(portfolio) >= (
            portfolio.equity() * self.settings.max_correlated_exposure_pct
        ):
            return RiskDecision(False, "correlated exposure limit")

        market_exposure = portfolio.market_exposure(signal.market_id)
        max_market = portfolio.equity() * self.settings.max_market_balance_pct
        if market_exposure >= max_market:
            return RiskDecision(False, "market exposure limit")

        max_trade = portfolio.equity() * self.settings.max_trade_balance_pct
        size = max(0.0, min(max_trade, max_market - market_exposure))
        if size <= 0:
            return RiskDecision(False, "zero risk budget")
        return RiskDecision(True, "allowed", size_usd=size)

    @staticmethod
    def _has_opposite_position(signal: Signal, portfolio: Portfolio) -> bool:
        opposite = Outcome.DOWN if signal.outcome == Outcome.UP else Outcome.UP
        return any(
            position.market_id == signal.market_id
            and position.outcome == opposite
            and position.size > 0
            for position in portfolio.positions.values()
        )

    def _stale_edge_share(self, signal: Signal) -> float:
        if signal.edge <= 0:
            return 1.0
        stale_component = min(1.0, signal.quote_age_ms / max(self.settings.stale_quote_ms, 1))
        lag_component = min(1.0, signal.repricing_lag_ms / max(self.settings.repricing_lag_ms, 1))
        return 0.5 * stale_component + 0.5 * lag_component

    @staticmethod
    def _same_side_exposure(signal: Signal, portfolio: Portfolio) -> float:
        return sum(
            position.cost_basis
            for position in portfolio.positions.values()
            if position.outcome == signal.outcome and position.size > 0
        )

    @staticmethod
    def _correlated_exposure(portfolio: Portfolio) -> float:
        return sum(
            position.cost_basis
            for position in portfolio.positions.values()
            if position.size > 0
        )

    def pause(self) -> None:
        self.paused = True

    def resume(self) -> None:
        self.paused = False

    def kill(self) -> None:
        self.kill_switch = True
