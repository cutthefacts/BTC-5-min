from __future__ import annotations

import math
from dataclasses import dataclass

from app.config import Settings
from app.models import FeatureSnapshot, Outcome


@dataclass(slots=True)
class FairValueResult:
    outcome: Outcome
    fair_price: float
    confidence: float
    edge_before_costs: float
    expected_edge: float


class FairValueEngine:
    """Heuristic, time-aware probability model for BTC 5m Up/Down markets."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def fair_value_up(self, features: FeatureSnapshot, market_price: float) -> FairValueResult:
        return self._fair_value(features, Outcome.UP, market_price)

    def fair_value_down(self, features: FeatureSnapshot, market_price: float) -> FairValueResult:
        return self._fair_value(features, Outcome.DOWN, market_price)

    def _fair_value(
        self,
        features: FeatureSnapshot,
        outcome: Outcome,
        market_price: float,
    ) -> FairValueResult:
        micro = features.microstructure
        remaining = max(features.market.seconds_to_end(features.timestamp), 0.0)
        time_weight = self._time_weight(remaining)
        direction = 1.0 if outcome == Outcome.UP else -1.0
        displacement = direction * features.distance_bps
        displacement_term = displacement / max(features.volatility_bps * 1.6 + 6.0, 1.0)
        momentum_weight = 0.12 if outcome == Outcome.UP else 0.25
        momentum_term = direction * (
            0.20 * features.momentum_bps.get(5, 0.0)
            + 0.12 * features.momentum_bps.get(15, 0.0)
            + 0.06 * features.momentum_bps.get(30, 0.0)
        ) / 10.0
        imbalance = 0.0
        stale = 0.0
        lag = 0.0
        liquidity_pressure = 0.0
        if micro is not None:
            imbalance = (
                micro.imbalance_ratio if outcome == Outcome.UP else -micro.imbalance_ratio
            )
            stale = min(1.0, micro.quote_age_ms / max(self.settings.stale_quote_ms, 1))
            lag = min(1.0, micro.repricing_lag_ms / max(self.settings.repricing_lag_ms, 1))
            liquidity_pressure = 0.12 if micro.liquidity_sweep else 0.0
            if micro.liquidity_vacuum:
                liquidity_pressure += 0.06
        volatility_penalty = min(0.45, features.volatility_bps / 80.0)
        early_penalty = 0.0
        high_price_penalty = 0.0
        weak_imbalance_penalty = 0.0
        if outcome == Outcome.UP:
            early_penalty = max(0.0, (remaining - 180.0) / 300.0) * 0.35
            high_price_penalty = max(0.0, market_price - 0.62) * 0.55
            weak_imbalance_penalty = 0.18 if imbalance < 0.15 else 0.0
        raw = (
            1.05 * displacement_term * time_weight
            + momentum_weight * momentum_term
            + 0.28 * imbalance
            + 0.22 * stale
            + 0.18 * lag
            + liquidity_pressure
            - volatility_penalty
            - early_penalty
            - high_price_penalty
            - weak_imbalance_penalty
        )
        fair_price = 1.0 / (1.0 + math.exp(-raw))
        fair_price = max(0.02, min(0.98, fair_price))
        fee_cost = market_price * self.settings.fee_bps / 10_000
        slippage_cost = market_price * self.settings.slippage_bps / 10_000
        edge_before_costs = fair_price - market_price
        expected_edge = edge_before_costs - fee_cost - slippage_cost
        confidence = max(0.0, min(1.0, abs(fair_price - 0.5) * 1.6 + 0.20 * stale + 0.15 * lag))
        return FairValueResult(
            outcome=outcome,
            fair_price=fair_price,
            confidence=confidence,
            edge_before_costs=edge_before_costs,
            expected_edge=expected_edge,
        )

    def _time_weight(self, remaining_seconds: float) -> float:
        if remaining_seconds <= 0:
            return 0.0
        normalized = max(0.0, min(1.0, (300.0 - remaining_seconds) / 300.0))
        return 0.35 + 1.65 * normalized**1.7
