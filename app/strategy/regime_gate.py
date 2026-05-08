from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings


@dataclass(frozen=True, slots=True)
class RegimePerformance:
    regime: str
    side: str
    entry_window: str
    trades: int
    profit_factor: float | None
    max_drawdown: float
    stale_fill_rate: float
    missed_fill_rate: float
    rolling_profit_factor: float | None = None
    rolling_drawdown: float = 0.0
    bad_regime_score: float = 0.0


@dataclass(frozen=True, slots=True)
class RegimeGateDecision:
    allowed: bool
    reason: str
    bad_regime_score: float = 0.0


class RegimeGate:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def evaluate(self, performance: RegimePerformance | None) -> RegimeGateDecision:
        if not self.settings.regime_gate_enabled:
            return RegimeGateDecision(True, "regime_gate_disabled")
        if performance is None:
            return RegimeGateDecision(True, "insufficient_regime_memory")
        score = bad_regime_score(performance, self.settings)
        if performance.trades < self.settings.regime_gate_min_trades:
            return RegimeGateDecision(True, "insufficient_regime_sample", score)
        pf = performance.rolling_profit_factor or performance.profit_factor or 0.0
        if pf < self.settings.regime_gate_min_pf:
            return RegimeGateDecision(False, "regime_pf_below_threshold", score)
        if performance.max_drawdown > self.settings.regime_gate_max_drawdown:
            return RegimeGateDecision(False, "regime_drawdown_too_high", score)
        if performance.stale_fill_rate > self.settings.regime_gate_max_stale_fill_rate:
            return RegimeGateDecision(False, "regime_stale_prone", score)
        if performance.missed_fill_rate > self.settings.regime_gate_max_missed_fill_rate:
            return RegimeGateDecision(False, "regime_missed_fill_prone", score)
        return RegimeGateDecision(True, "regime_profitable", score)


def bad_regime_score(performance: RegimePerformance, settings: Settings | None = None) -> float:
    settings = settings or Settings()
    pf = performance.rolling_profit_factor or performance.profit_factor or 0.0
    pf_penalty = max(0.0, settings.regime_gate_min_pf - pf) / max(
        settings.regime_gate_min_pf, 0.01
    )
    dd_penalty = min(
        1.0,
        performance.max_drawdown / max(settings.regime_gate_max_drawdown, 0.01),
    )
    stale_penalty = min(
        1.0,
        performance.stale_fill_rate / max(settings.regime_gate_max_stale_fill_rate, 0.01),
    )
    missed_penalty = min(
        1.0,
        performance.missed_fill_rate / max(settings.regime_gate_max_missed_fill_rate, 0.01),
    )
    sample_penalty = (
        0.25
        if performance.trades < settings.regime_gate_min_trades
        else 0.0
    )
    return min(
        1.0,
        0.35 * pf_penalty
        + 0.25 * dd_penalty
        + 0.15 * stale_penalty
        + 0.15 * missed_penalty
        + sample_penalty,
    )
