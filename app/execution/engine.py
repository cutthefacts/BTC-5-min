from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from datetime import UTC, datetime

from app.config import Settings
from app.models import Market, Order, Outcome, Side, Signal, SignalAction, Trade
from app.portfolio.manager import Portfolio


class ExecutionEngine(ABC):
    @abstractmethod
    async def execute(self, market: Market, signal: Signal, size_usd: float) -> Trade | None:
        raise NotImplementedError


class PaperExecutionEngine(ExecutionEngine):
    def __init__(self, settings: Settings, portfolio: Portfolio) -> None:
        self.settings = settings
        self.portfolio = portfolio

    async def execute(self, market: Market, signal: Signal, size_usd: float) -> Trade | None:
        is_entry = signal.action in {SignalAction.BUY_UP, SignalAction.BUY_DOWN}
        if not is_entry or signal.outcome is None:
            return None
        token_id = market.up_token_id if signal.outcome == Outcome.UP else market.down_token_id
        limit_price = signal.market_probability
        fill_price = min(0.99, limit_price * (1 + self.settings.slippage_bps / 10_000))
        if fill_price <= 0:
            return None
        shares = size_usd / fill_price
        fee = fill_price * shares * self.settings.fee_bps / 10_000
        submitted_at = datetime.now(UTC)
        filled_at = datetime.now(UTC)
        signal_to_submit_ms = max(
            0.0,
            (submitted_at - signal.timestamp).total_seconds() * 1_000,
        )
        submit_to_fill_ms = max(0.0, (filled_at - submitted_at).total_seconds() * 1_000)
        total_fill_latency_ms = signal_to_submit_ms + submit_to_fill_ms
        expected_edge_at_signal = signal.edge
        fee_cost = fill_price * self.settings.fee_bps / 10_000
        expected_edge_at_submit = signal.expected_probability - fill_price - fee_cost
        stale_reason = self._stale_reason(signal, fill_price, expected_edge_at_submit)
        order = Order(
            id=str(uuid.uuid4()),
            market_id=market.condition_id,
            token_id=token_id,
            outcome=signal.outcome,
            side=Side.BUY,
            price=limit_price,
            size=shares,
            status="filled",
        )
        trade = Trade(
            order_id=order.id,
            market_id=market.condition_id,
            token_id=token_id,
            outcome=signal.outcome,
            price=fill_price,
            size=shares,
            fee=fee,
            expected_edge=signal.edge,
            signal_to_fill_delay_ms=total_fill_latency_ms,
            fill_latency_ms=total_fill_latency_ms,
            realized_edge=expected_edge_at_submit,
            stale_fill=stale_reason is not None,
            signal_timestamp=signal.timestamp,
            order_submit_timestamp=submitted_at,
            fill_timestamp=filled_at,
            signal_to_submit_ms=signal_to_submit_ms,
            submit_to_fill_ms=submit_to_fill_ms,
            total_fill_latency_ms=total_fill_latency_ms,
            expected_edge_at_signal=expected_edge_at_signal,
            expected_edge_at_submit=expected_edge_at_submit,
            realized_edge_after_fill=expected_edge_at_submit,
            stale_reason=stale_reason,
            strategy_name=signal.strategy_name,
            strategy_version=signal.strategy_version,
            feature_schema_version=signal.feature_schema_version,
            data_collection_started_at=signal.data_collection_started_at,
            timestamp=filled_at,
        )
        self.portfolio.apply_fill(order, trade)
        return trade

    def _stale_reason(
        self,
        signal: Signal,
        fill_price: float,
        expected_edge_at_submit: float,
    ) -> str | None:
        if signal.quote_age_ms > self.settings.max_quote_age_ms:
            return "quote_age_exceeded"
        if fill_price > signal.market_probability + self.settings.max_spread:
            return "repriced_beyond_threshold"
        if expected_edge_at_submit < self.settings.min_edge:
            return "edge_disappeared_before_fill"
        return None


class RealExecutionEngine(ExecutionEngine):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def execute(self, market: Market, signal: Signal, size_usd: float) -> Trade | None:
        raise RuntimeError(
            "Real execution is intentionally disabled in MVP. Use py_clob_client_v2 signing only "
            "after paper gates and Telegram confirmation pass."
        )
