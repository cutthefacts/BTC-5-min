from __future__ import annotations

from app.backtest.optimizer import BacktestMetrics, BacktestOptimizer, ParameterGrid


def main() -> None:
    # Placeholder runner: wire historical SQLite replay once enough paper data exists.
    sample = [
        BacktestMetrics(
            params=params,
            net_pnl=0.0,
            max_drawdown=0.0,
            profit_factor=0.0,
            winrate=0.0,
            trades=0,
        )
        for params in ParameterGrid().iter_params()
    ]
    best = BacktestOptimizer().optimize(sample)[:5]
    for item in best:
        print(item)


if __name__ == "__main__":
    main()
