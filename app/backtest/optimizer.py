from __future__ import annotations

from dataclasses import dataclass
from itertools import product


@dataclass(frozen=True, slots=True)
class StrategyParams:
    threshold_bps: float
    momentum_window: int
    min_edge: float
    entry_start_second: int
    entry_end_buffer_second: int
    min_liquidity: float
    max_spread: float


@dataclass(slots=True)
class BacktestMetrics:
    params: StrategyParams
    net_pnl: float
    max_drawdown: float
    profit_factor: float
    winrate: float
    trades: int

    @property
    def score(self) -> float:
        if self.trades < 30:
            return -1e9
        return self.net_pnl - 2.0 * self.max_drawdown + min(self.profit_factor, 3.0)


class ParameterGrid:
    def __init__(self) -> None:
        self.threshold_bps = [1.5, 2.0, 3.0, 5.0]
        self.momentum_window = [5, 15, 30, 60]
        self.min_edge = [0.02, 0.035, 0.05]
        self.entry_start_second = [5, 10, 20]
        self.entry_end_buffer_second = [15, 20, 30]
        self.min_liquidity = [50.0, 100.0, 250.0]
        self.max_spread = [0.02, 0.04, 0.06]

    def iter_params(self):
        for values in product(
            self.threshold_bps,
            self.momentum_window,
            self.min_edge,
            self.entry_start_second,
            self.entry_end_buffer_second,
            self.min_liquidity,
            self.max_spread,
        ):
            yield StrategyParams(*values)


class BacktestOptimizer:
    def optimize(self, runs: list[BacktestMetrics]) -> list[BacktestMetrics]:
        return sorted(runs, key=lambda metric: metric.score, reverse=True)
