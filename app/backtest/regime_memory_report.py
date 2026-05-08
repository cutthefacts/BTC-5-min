from __future__ import annotations

import argparse
from datetime import UTC, datetime

from app.backtest.analytics import metric_dict, summarize_bucket
from app.backtest.filters import add_common_report_args, filters_from_args
from app.backtest.presets import load_preset
from app.backtest.research import (
    classify_research_regime,
    entry_window_bucket,
    filtered_candidate_rows,
    open_conn,
)
from app.config import get_settings
from app.storage.sqlite import SQLiteStore
from app.strategy.regime_gate import RegimePerformance, bad_regime_score


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="candidate_v1")
    parser.add_argument("--write", action="store_true")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = filters_from_args(args)
    rows = filtered_candidate_rows(conn, preset, filters)
    grouped: dict[tuple[str, str, str], list[float]] = {}
    for row in rows:
        key = (
            classify_research_regime(row),
            str(row.get("outcome") or "missing"),
            entry_window_bucket(row.get("seconds_to_close"), 30),
        )
        grouped.setdefault(key, []).append(float(row["pnl"]))
    settings = get_settings()
    store = SQLiteStore(settings.database_url)
    store.init()
    print("Regime Performance Memory")
    print("=========================")
    for (regime, side, window), pnls in sorted(grouped.items()):
        metrics = summarize_bucket(
            f"{regime}:{side}:{window}",
            pnls,
            min_trades=settings.regime_gate_min_trades,
        )
        perf = RegimePerformance(
            regime=regime,
            side=side,
            entry_window=window,
            trades=metrics.trades,
            profit_factor=metrics.profit_factor,
            max_drawdown=metrics.max_drawdown,
            stale_fill_rate=0.0,
            missed_fill_rate=0.0,
            rolling_profit_factor=metrics.profit_factor,
            rolling_drawdown=metrics.max_drawdown,
        )
        score = bad_regime_score(perf, settings)
        item = metric_dict(metrics)
        item["bad_regime_score"] = round(score, 4)
        print(item)
        if args.write:
            store.save_regime_performance(
                datetime.now(UTC).isoformat(),
                regime,
                side,
                window,
                metrics.trades,
                metrics.profit_factor,
                metrics.max_drawdown,
                0.0,
                0.0,
                metrics.profit_factor,
                metrics.max_drawdown,
                score,
            )


if __name__ == "__main__":
    main()
