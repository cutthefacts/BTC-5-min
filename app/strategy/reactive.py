from __future__ import annotations

import math

from app.backtest.filters import allowed_by_windows
from app.config import Settings
from app.models import FeatureSnapshot, Outcome, Signal, SignalAction
from app.strategy.edge_quality import edge_quality_score, extreme_edge_reason
from app.strategy.fair_value import FairValueEngine
from app.strategy.regime import MarketRegimeEngine


class ReactiveDirectionalStrategy:
    """Inefficiency-first strategy.

    Despite the legacy class name, this is no longer a raw directional momentum
    model. Direction chooses which market side can be mispriced; trade permission
    comes from quote staleness, repricing lag, imbalance, spread quality, and EV.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.fair_value = FairValueEngine(settings)
        self.regime = MarketRegimeEngine()

    def evaluate(self, features: FeatureSnapshot) -> Signal:
        block_reason = self._blocked(features)
        if block_reason:
            return self._hold(features, block_reason)

        if features.distance_bps > 0:
            return self._candidate(features, Outcome.UP)
        if features.distance_bps < 0:
            return self._candidate(features, Outcome.DOWN)
        return self._hold(features, "no displacement")

    def _blocked(self, f: FeatureSnapshot) -> str | None:
        if f.microstructure is None:
            return "missing microstructure"
        regime_snapshot = self.regime.classify_snapshot(f)
        regime = regime_snapshot.regime
        allowed = self.settings.allowed_regime_set()
        disabled = self.settings.disabled_regime_set()
        if allowed and regime not in allowed:
            return f"regime_not_allowed:{regime}"
        if regime in disabled:
            return f"regime_disabled:{regime}"
        if f.volatility_bps > self.settings.max_volatility_bps:
            return "volatility_too_high"
        for book in (f.up_book, f.down_book):
            if book.spread is None:
                return "missing book"
            if book.spread > self.settings.max_spread:
                return "spread_too_wide"
            if book.top_liquidity < self.settings.min_liquidity_usd:
                return "liquidity_too_low"
        return None

    def _candidate(self, f: FeatureSnapshot, outcome: Outcome) -> Signal:
        book = f.up_book if outcome == Outcome.UP else f.down_book
        ask = book.best_ask
        if ask is None:
            return self._hold(f, "missing ask")
        seconds_to_close = f.market.seconds_to_end(f.timestamp)
        if outcome == Outcome.UP and self.settings.up_disabled_for_research:
            return self._hold_candidate(
                f,
                outcome,
                0.0,
                ask,
                0.0,
                0.0,
                0.0,
                "up_disabled_for_research",
            )
        if self.settings.side_mode == "UP_ONLY" and outcome == Outcome.DOWN:
            return self._hold_candidate(f, outcome, 0.0, ask, 0.0, 0.0, 0.0, "side_mode_up_only")
        if self.settings.side_mode == "DOWN_ONLY" and outcome == Outcome.UP:
            return self._hold_candidate(f, outcome, 0.0, ask, 0.0, 0.0, 0.0, "side_mode_down_only")
        regime = self.regime.classify_snapshot(f).regime
        if (
            self.settings.side_mode == "UP_PREFERRED"
            and outcome == Outcome.DOWN
            and regime not in {"compression", "low_volatility", "trending_down"}
        ):
            return self._hold_candidate(
                f,
                outcome,
                0.0,
                ask,
                0.0,
                0.0,
                0.0,
                f"down_disabled_in_weak_regime:{regime}",
            )
        allowed_windows = (
            self.settings.up_allowed_entry_windows
            if outcome == Outcome.UP
            else self.settings.down_allowed_entry_windows
        )
        if not allowed_by_windows(
            seconds_to_close,
            allowed_windows,
            self.settings.blocked_entry_windows_seconds_to_close,
        ):
            return self._hold_candidate(f, outcome, 0.0, ask, 0.0, 0.0, 0.0, "entry_window_blocked")
        micro = f.microstructure
        if micro is not None:
            if self.settings.avoid_liquidity_sweep and micro.liquidity_sweep:
                return self._hold_candidate(
                    f, outcome, 0.0, ask, 0.0, 0.0, 0.0, "liquidity_sweep_risk"
                )
            if not (
                self.settings.min_quote_age_ms
                <= micro.quote_age_ms
                <= self.settings.max_quote_age_ms_filter
            ):
                return self._hold_candidate(f, outcome, 0.0, ask, 0.0, 0.0, 0.0, "quote_age_range")
            if not (
                self.settings.min_repricing_lag_ms
                <= micro.repricing_lag_ms
                <= self.settings.max_repricing_lag_ms
            ):
                return self._hold_candidate(
                    f, outcome, 0.0, ask, 0.0, 0.0, 0.0, "repricing_lag_range"
                )
        market_probability = ask
        min_prob, max_prob = (
            (
                self.settings.up_market_probability_min,
                self.settings.up_market_probability_max,
            )
            if outcome == Outcome.UP
            else (
                self.settings.down_market_probability_min,
                self.settings.down_market_probability_max,
            )
        )
        if not (min_prob <= market_probability <= max_prob):
            return self._hold_candidate(
                f,
                outcome,
                0.0,
                market_probability,
                0.0,
                0.0,
                0.0,
                "market_probability_range",
            )
        score = self.inefficiency_score(f, outcome)
        fair = (
            self.fair_value.fair_value_up(f, market_probability)
            if outcome == Outcome.UP
            else self.fair_value.fair_value_down(f, market_probability)
        )
        confidence = min(1.0, 0.55 * self.confidence(f, score) + 0.45 * fair.confidence)
        expected_probability = fair.fair_price
        edge = fair.expected_edge
        extreme_reason = extreme_edge_reason(
            edge,
            micro.quote_age_ms if micro else 0.0,
            micro.repricing_lag_ms if micro else 0.0,
            f.volatility_bps,
            regime,
            self.settings,
        )
        if extreme_reason:
            return self._hold_candidate(
                f,
                outcome,
                expected_probability,
                market_probability,
                edge,
                score,
                confidence,
                extreme_reason,
            )
        quality = edge_quality_score(
            edge,
            None,
            micro.quote_age_ms if micro else 0.0,
            micro.repricing_lag_ms if micro else 0.0,
            0.0,
            None,
            self.settings,
        )
        confidence = min(1.0, 0.75 * confidence + 0.25 * quality)

        min_score = (
            self.settings.up_min_inefficiency_score
            if outcome == Outcome.UP
            else self.settings.down_min_inefficiency_score
        )
        min_confidence = (
            self.settings.up_min_confidence
            if outcome == Outcome.UP
            else self.settings.down_min_confidence
        )
        min_edge = (
            self.settings.up_min_edge
            if outcome == Outcome.UP
            else self.settings.down_min_edge
        )

        if score < min_score:
            return self._hold_candidate(
                f,
                outcome,
                expected_probability,
                market_probability,
                edge,
                score,
                confidence,
                "insufficient inefficiency score",
            )
        if confidence < min_confidence:
            return self._hold_candidate(
                f,
                outcome,
                expected_probability,
                market_probability,
                edge,
                score,
                confidence,
                "insufficient confidence",
            )
        if edge < min_edge:
            return self._hold_candidate(
                f,
                outcome,
                expected_probability,
                market_probability,
                edge,
                score,
                confidence,
                "insufficient edge",
            )

        action = SignalAction.BUY_UP if outcome == Outcome.UP else SignalAction.BUY_DOWN
        return self._signal(
            f,
            action,
            outcome,
            expected_probability,
            market_probability,
            edge,
            score,
            confidence,
            "inefficiency accepted",
        )

    def inefficiency_score(self, f: FeatureSnapshot, outcome: Outcome) -> float:
        micro = f.microstructure
        if micro is None:
            return 0.0
        if micro.quote_age_ms < self.settings.stale_quote_ms * 0.5:
            return 0.0
        if micro.repricing_lag_ms < self.settings.repricing_lag_ms * 0.5:
            return 0.0
        displacement = min(1.0, abs(f.distance_bps) / 12.0)
        quote_staleness = min(1.0, micro.quote_age_ms / self.settings.stale_quote_ms)
        repricing_lag = min(1.0, micro.repricing_lag_ms / self.settings.repricing_lag_ms)
        directional_imbalance = (
            micro.imbalance_ratio if outcome == Outcome.UP else -micro.imbalance_ratio
        )
        imbalance = max(0.0, min(1.0, (directional_imbalance + 1.0) / 2.0))
        sweep = 1.0 if micro.liquidity_sweep else 0.0
        spread_quality = self._spread_quality(f, outcome)
        volatility_penalty = min(
            0.45,
            f.volatility_bps / max(self.settings.max_volatility_bps, 1.0),
        )
        timing = self._adaptive_timing_quality(f)
        raw = (
            0.25 * displacement
            + 0.25 * quote_staleness
            + 0.18 * repricing_lag
            + 0.15 * imbalance
            + 0.08 * sweep
            + 0.06 * spread_quality
            + 0.03 * timing
        )
        return max(0.0, min(1.0, raw * (1.0 - volatility_penalty)))

    def confidence(self, f: FeatureSnapshot, score: float) -> float:
        micro = f.microstructure
        if micro is None:
            return 0.0
        book_ok = 1.0 if f.up_book.spread is not None and f.down_book.spread is not None else 0.0
        stale_ok = min(1.0, micro.quote_age_ms / self.settings.stale_quote_ms)
        return max(0.0, min(1.0, 0.55 * score + 0.25 * stale_ok + 0.20 * book_ok))

    def estimate_probability(
        self,
        f: FeatureSnapshot,
        outcome: Outcome,
        inefficiency_score: float,
    ) -> float:
        displacement_term = f.distance_bps / 14.0
        aux_momentum = (
            0.20 * f.momentum_bps.get(5, 0.0)
            + 0.10 * f.momentum_bps.get(15, 0.0)
            + 0.05 * f.momentum_bps.get(30, 0.0)
        ) / 12.0
        if outcome == Outcome.DOWN:
            displacement_term *= -1
            aux_momentum *= -1
        raw = 1.8 * inefficiency_score + 0.35 * displacement_term + 0.15 * aux_momentum - 0.9
        probability = 1.0 / (1.0 + math.exp(-raw))
        return max(0.02, min(0.98, probability))

    def legacy_momentum_probability(self, f: FeatureSnapshot, outcome: Outcome) -> float:
        distance_term = f.distance_bps / 8.0
        momentum_term = (
            0.50 * f.momentum_bps.get(5, 0.0)
            + 0.30 * f.momentum_bps.get(15, 0.0)
            + 0.15 * f.momentum_bps.get(30, 0.0)
            + 0.05 * f.momentum_bps.get(60, 0.0)
        ) / 8.0
        time_left = max(f.market.seconds_to_end(f.timestamp), 1.0)
        time_confidence = min(1.0, max(0.25, (300.0 - time_left) / 180.0))
        volatility_penalty = min(0.20, f.volatility_bps / 250.0)
        raw = (distance_term + momentum_term) * time_confidence
        up_prob = 1.0 / (1.0 + math.exp(-raw))
        up_prob = 0.5 + (up_prob - 0.5) * (1.0 - volatility_penalty)
        up_prob = max(0.02, min(0.98, up_prob))
        return up_prob if outcome == Outcome.UP else 1.0 - up_prob

    def _hold_candidate(
        self,
        f: FeatureSnapshot,
        outcome: Outcome,
        expected_probability: float,
        market_probability: float,
        edge: float,
        score: float,
        confidence: float,
        reason: str,
    ) -> Signal:
        reason = {
            "insufficient inefficiency score": "low_inefficiency_score",
            "insufficient confidence": "low_confidence",
            "insufficient edge": "negative_ev",
        }.get(reason, reason)
        return self._signal(
            f,
            SignalAction.HOLD,
            outcome,
            expected_probability,
            market_probability,
            edge,
            score,
            confidence,
            reason,
        )

    def _hold(self, f: FeatureSnapshot, reason: str) -> Signal:
        normalized = {
            "insufficient inefficiency score": "low_inefficiency_score",
            "insufficient confidence": "low_confidence",
            "insufficient edge": "negative_ev",
            "no displacement": "weak_orderbook_imbalance",
        }.get(reason, reason)
        return self._signal(f, SignalAction.HOLD, None, 0.0, 0.0, 0.0, 0.0, 0.0, normalized)

    def _signal(
        self,
        f: FeatureSnapshot,
        action: SignalAction,
        outcome: Outcome | None,
        expected_probability: float,
        market_probability: float,
        edge: float,
        score: float,
        confidence: float,
        reason: str,
    ) -> Signal:
        micro = f.microstructure
        return Signal(
            market_id=f.market.condition_id,
            action=action,
            outcome=outcome,
            expected_probability=expected_probability,
            market_probability=market_probability,
            edge=edge,
            strength=score,
            reason=reason,
            inefficiency_score=score,
            confidence=confidence,
            quote_age_ms=micro.quote_age_ms if micro else 0.0,
            repricing_lag_ms=micro.repricing_lag_ms if micro else 0.0,
            imbalance_ratio=micro.imbalance_ratio if micro else 0.0,
            liquidity_sweep=micro.liquidity_sweep if micro else False,
            btc_price=f.btc_price,
            price_to_beat=f.market.price_to_beat,
            distance_bps=f.distance_bps,
            drift_15s_bps=f.momentum_bps.get(15),
            drift_30s_bps=f.momentum_bps.get(30),
            drift_60s_bps=f.momentum_bps.get(60),
            drift_180s_bps=f.momentum_bps.get(180),
            realized_vol_30s_bps=f.volatility_bps,
            realized_vol_60s_bps=f.volatility_bps,
            realized_vol_180s_bps=f.volatility_bps,
            seconds_to_close=f.market.seconds_to_end(f.timestamp),
            seconds_from_open=f.market.seconds_from_start(f.timestamp),
            best_bid=self._book_for(f, outcome).best_bid if outcome else None,
            best_ask=self._book_for(f, outcome).best_ask if outcome else None,
            mid_price=self._mid_price(f, outcome),
            spread=self._book_for(f, outcome).spread if outcome else None,
            spread_bps=self._spread_bps(f, outcome),
            top_bid_size=self._book_for(f, outcome).bids[0].size
            if outcome and self._book_for(f, outcome).bids
            else None,
            top_ask_size=self._book_for(f, outcome).asks[0].size
            if outcome and self._book_for(f, outcome).asks
            else None,
            bid_depth_3=self._depth(f, outcome, "bid", 3),
            ask_depth_3=self._depth(f, outcome, "ask", 3),
            bid_depth_5=self._depth(f, outcome, "bid", 5),
            ask_depth_5=self._depth(f, outcome, "ask", 5),
            imbalance_acceleration=micro.imbalance_acceleration if micro else None,
            disappearing_liquidity=micro.disappearing_liquidity if micro else None,
            edge_quality_score=edge_quality_score(
                edge,
                None,
                micro.quote_age_ms if micro else 0.0,
                micro.repricing_lag_ms if micro else 0.0,
                0.0,
                None,
                self.settings,
            ),
            extreme_edge_suspect=extreme_edge_reason(
                edge,
                micro.quote_age_ms if micro else 0.0,
                micro.repricing_lag_ms if micro else 0.0,
                f.volatility_bps,
                self.regime.classify_snapshot(f).regime,
                self.settings,
            )
            is not None,
            fair_value=expected_probability,
            expected_edge=edge,
            regime=self.regime.classify_snapshot(f).regime,
            regime_confidence=self.regime.classify_snapshot(f).confidence,
            regime_source="snapshot",
            side_mode=self.settings.side_mode,
            strategy_version=self.settings.strategy_version,
            feature_schema_version=self.settings.feature_schema_version,
            data_collection_started_at=self.settings.data_collection_started_at or None,
            timestamp=f.timestamp,
        )

    def _book_for(self, f: FeatureSnapshot, outcome: Outcome | None):
        return f.up_book if outcome == Outcome.UP else f.down_book

    def _mid_price(self, f: FeatureSnapshot, outcome: Outcome | None) -> float | None:
        if outcome is None:
            return None
        book = self._book_for(f, outcome)
        if book.best_bid is None or book.best_ask is None:
            return None
        return (book.best_bid + book.best_ask) / 2

    def _spread_bps(self, f: FeatureSnapshot, outcome: Outcome | None) -> float | None:
        mid = self._mid_price(f, outcome)
        if mid is None or mid <= 0:
            return None
        spread = self._book_for(f, outcome).spread
        return None if spread is None else spread / mid * 10_000

    def _depth(
        self,
        f: FeatureSnapshot,
        outcome: Outcome | None,
        side: str,
        levels: int,
    ) -> float | None:
        if outcome is None:
            return None
        book = self._book_for(f, outcome)
        rows = book.bids if side == "bid" else book.asks
        return sum(level.price * level.size for level in rows[:levels])

    def _spread_quality(self, f: FeatureSnapshot, outcome: Outcome) -> float:
        book = f.up_book if outcome == Outcome.UP else f.down_book
        if book.spread is None:
            return 0.0
        return max(0.0, 1.0 - book.spread / max(self.settings.max_spread, 0.01))

    def _adaptive_timing_quality(self, f: FeatureSnapshot) -> float:
        remaining = f.market.seconds_to_end(f.timestamp)
        if remaining <= 3:
            return 0.0
        if 8 <= remaining <= 90:
            return 1.0
        if remaining < 8:
            return 0.6
        return 0.35
