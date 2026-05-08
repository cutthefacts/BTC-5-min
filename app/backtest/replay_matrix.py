from __future__ import annotations

import argparse
import sqlite3
from dataclasses import asdict

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.replay import TickByTickReplay
from app.config import get_settings
from app.storage.sqlite import SQLiteStore

WINDOW_MODES = {
    "all": None,
    "blocked_bad_windows": None,
    "only_best_candidate_windows": "120-180",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--preset", default="strict")
    parser.add_argument("--soft-filters", action="store_true")
    add_common_report_args(parser)
    args = parser.parse_args()
    settings = get_settings()
    preset = load_preset(args.preset)
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    replay = TickByTickReplay(conn)
    results = []
    print("Replay Matrix")
    print("=============")
    for latency in (50, 100, 250, 500, 1000):
        for side_mode in ("UP_ONLY", "DOWN_ONLY", "BOTH"):
            for avoid_sweep in (True, False):
                for window_mode, allowed_windows in WINDOW_MODES.items():
                    blocked = (
                        settings.blocked_entry_windows_seconds_to_close
                        if window_mode != "all"
                        else None
                    )
                    result = replay.run(
                        latency_ms=latency,
                        slippage_bps=settings.slippage_bps,
                        min_score=preset.min_score,
                        min_confidence=preset.min_confidence,
                        min_edge=preset.min_edge,
                        include_hold_candidates=True,
                        side_mode=side_mode,
                        avoid_liquidity_sweep=avoid_sweep,
                        allowed_entry_windows=allowed_windows,
                        blocked_entry_windows=blocked,
                        min_quote_age_ms=preset.min_quote_age_ms,
                        max_quote_age_ms=preset.max_quote_age_ms,
                        min_repricing_lag_ms=preset.min_repricing_lag_ms,
                        max_repricing_lag_ms=preset.max_repricing_lag_ms,
                        time_filters=TimeFilters(
                            only_complete_microstructure=args.only_complete_microstructure,
                            from_timestamp=args.from_timestamp,
                            to_timestamp=args.to_timestamp,
                        ),
                        soft_filters=args.soft_filters or preset.soft_filters,
                    )
                    avg_pnl = result.net_pnl / result.trades if result.trades else 0.0
                    row = {
                        "latency_ms": latency,
                        "side_mode": side_mode,
                        "liquidity_sweep": "avoid" if avoid_sweep else "allow",
                        "entry_windows": window_mode,
                        "avg_pnl": round(avg_pnl, 5),
                        **asdict(result),
                    }
                    results.append(row)
                    if not args.summary:
                        print(row)
    if args.summary:
        print_summary(results)


def print_summary(results: list[dict]) -> None:
    print("Replay Matrix Summary")
    print("=====================")
    for side in ("DOWN_ONLY", "UP_ONLY", "BOTH"):
        ranked = sorted(
            [row for row in results if row["side_mode"] == side and row["trades"] > 0],
            key=lambda row: (row["profit_factor"] or 0, row["net_pnl"]),
            reverse=True,
        )
        print({f"best_{side}": ranked[:3]})
    zero = [row for row in results if row["trades"] == 0]
    print({"configs_with_trades_0": len(zero)})
    print({"likely_zero_trade_filters": "strict quote/lag/window/sweep combination"})
    latency = {}
    for row in results:
        latency.setdefault(row["latency_ms"], []).append(row["net_pnl"])
    print({"latency_sensitivity": {k: round(sum(v), 4) for k, v in latency.items()}})


if __name__ == "__main__":
    main()
