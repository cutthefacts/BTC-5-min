from __future__ import annotations

import argparse
import csv
import itertools
import json
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.replay import ReplayResult, TickByTickReplay
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="strict")
    parser.add_argument("--mode", choices=["grid", "minimum_viable_edge"], default="grid")
    parser.add_argument("--soft-filters", action="store_true")
    parser.add_argument(
        "--max-configs",
        type=int,
        default=None,
        help="Maximum unique replay configs. Defaults to 180 for minimum_viable_edge.",
    )
    parser.add_argument("--progress-every", type=int, default=10)
    add_common_report_args(parser)
    args = parser.parse_args()
    settings = get_settings()
    preset = load_preset(args.preset)
    store = SQLiteStore(settings.database_url)
    store.init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    replay = TickByTickReplay(conn)
    rows: list[dict] = []
    trade_targets = [30, 50, 100] if args.mode == "minimum_viable_edge" else [50]
    max_configs = args.max_configs
    if max_configs is None and args.mode == "minimum_viable_edge":
        max_configs = 180
    grid = build_grid(args.mode, preset)
    if max_configs is not None:
        grid = grid[:max_configs]
    started = time.perf_counter()
    print(
        {
            "mode": args.mode,
            "preset": preset.name,
            "unique_replay_configs": len(grid),
            "rows_after_trade_targets": len(grid) * len(trade_targets),
            "soft_filters": args.soft_filters or preset.soft_filters,
        },
        flush=True,
    )
    for index, (
        side,
        min_score,
        min_conf,
        min_edge,
        quote_range,
        lag_range,
        windows,
        avoid_sweep,
    ) in enumerate(grid, start=1):
        if args.progress_every and (index == 1 or index % args.progress_every == 0):
            elapsed = round(time.perf_counter() - started, 1)
            print({"progress": f"{index}/{len(grid)}", "elapsed_seconds": elapsed}, flush=True)
        result = replay.run(
            latency_ms=250,
            slippage_bps=settings.slippage_bps,
            min_score=min_score,
            min_confidence=min_conf,
            min_edge=min_edge,
            include_hold_candidates=True,
            side_mode=side,
            avoid_liquidity_sweep=avoid_sweep,
            allowed_entry_windows=windows,
            blocked_entry_windows=settings.blocked_entry_windows_seconds_to_close,
            min_quote_age_ms=quote_range[0],
            max_quote_age_ms=quote_range[1],
            min_repricing_lag_ms=lag_range[0],
            max_repricing_lag_ms=lag_range[1],
            time_filters=TimeFilters(
                only_complete_microstructure=args.only_complete_microstructure,
                from_timestamp=args.from_timestamp,
                to_timestamp=args.to_timestamp,
            ),
            soft_filters=args.soft_filters or preset.soft_filters,
        )
        for min_trades in trade_targets:
            config = {
                "SIDE_MODE": side,
                "MIN_INEFFICIENCY_SCORE": min_score,
                "MIN_CONFIDENCE": min_conf,
                "MIN_EDGE": min_edge,
                "QUOTE_AGE_RANGE": quote_range,
                "REPRICING_LAG_RANGE": lag_range,
                "ALLOWED_ENTRY_WINDOWS": windows,
                "AVOID_LIQUIDITY_SWEEP": avoid_sweep,
                "MIN_TRADES_TARGET": min_trades,
                "PRESET": preset.name,
                "SOFT_FILTERS": args.soft_filters or preset.soft_filters,
            }
            rows.append(row_for(config, result, settings, min_trades))

    output = Path("data/optimization_runs.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {output}")
    print_top("best DOWN config", rows, "DOWN_ONLY")
    print_top("best UP config", rows, "UP_ONLY")
    print_top("combined config", rows, "BOTH")
    safest = sorted(
        [row for row in rows if row["reliable"] == "True"],
        key=lambda row: (float(row["max_drawdown"]), -float(row["profit_factor"] or 0)),
    )
    print("\nsafest config")
    print(safest[0] if safest else "none")
    print_rejected_diagnostics(rows)


def build_grid(mode: str, preset) -> list[tuple]:
    side_modes = ["DOWN_ONLY", "BOTH", "UP_ONLY"]
    if mode == "minimum_viable_edge":
        score_values = unique([preset.min_score, 0.35, 0.40, 0.45, 0.50])
        confidence_values = unique([preset.min_confidence, 0.50, 0.55, 0.60])
        edge_values = unique([preset.min_edge, 0.03, 0.04, 0.05])
        quote_ranges = unique_ranges(
            [
                (preset.min_quote_age_ms or 0, preset.max_quote_age_ms or 2500),
                (0, 1000),
                (0, 1500),
                (0, 2500),
            ]
        )
        lag_ranges = unique_ranges(
            [
                (preset.min_repricing_lag_ms or 0, preset.max_repricing_lag_ms or 1500),
                (0, 1500),
                (100, 1000),
                (250, 750),
            ]
        )
        windows = unique([preset.allowed_entry_windows or "45-240", "120-180", "75-195", "45-240"])
        avoid_sweep_values = unique([preset.avoid_liquidity_sweep, True, False])
    else:
        score_values = sorted({preset.min_score, 0.30, 0.40, 0.45, 0.55, 0.65})
        confidence_values = sorted({preset.min_confidence, 0.45, 0.55, 0.60, 0.70})
        edge_values = sorted({preset.min_edge, 0.02, 0.035, 0.05, 0.08})
        quote_ranges = [(0, 1000), (0, 1500), (0, 2500), (250, 1000)]
        lag_ranges = [(0, 1500), (100, 1000), (250, 750), (250, 1250)]
        windows = ["45-240", "75-195", "90-180", "120-180", "90-150"]
        avoid_sweep_values = [True, False]

    grid_by_side = {
        side: list(
            itertools.product(
                [side],
                score_values,
                confidence_values,
                edge_values,
                quote_ranges,
                lag_ranges,
                windows,
                avoid_sweep_values,
            )
        )
        for side in side_modes
    }
    return round_robin(grid_by_side[side] for side in side_modes)


def unique(values: Iterable) -> list:
    result = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def unique_ranges(values: Iterable[tuple[float, float]]) -> list[tuple[float, float]]:
    return unique([(float(start), float(end)) for start, end in values])


def round_robin(groups: Iterable[list[tuple]]) -> list[tuple]:
    groups = [list(group) for group in groups]
    output: list[tuple] = []
    for index in range(max((len(group) for group in groups), default=0)):
        for group in groups:
            if index < len(group):
                output.append(group[index])
    return output

def row_for(config: dict, result: ReplayResult, settings, min_trades: int = 50) -> dict:
    avg_pnl = result.net_pnl / result.trades if result.trades else 0.0
    reject_reasons = reliability_reject_reasons(result, settings, min_trades)
    reliable = not reject_reasons
    return {
        "config": json.dumps(config, separators=(",", ":")),
        "side_mode": config["SIDE_MODE"],
        "trades": result.trades,
        "net_pnl": result.net_pnl,
        "profit_factor": result.profit_factor,
        "max_drawdown": result.max_drawdown,
        "winrate": result.winrate,
        "avg_pnl": avg_pnl,
        "stale_fill_rate": result.stale_fill_rate,
        "missed_fill_rate": result.missed_fill_rate,
        "stale_reasons": json.dumps(result.stale_reasons or {}, separators=(",", ":")),
        "reject_reasons": "|".join(reject_reasons),
        "reliable": str(reliable),
        "result": json.dumps(asdict(result), default=str, separators=(",", ":")),
    }


def reliability_reject_reasons(
    result: ReplayResult,
    settings,
    min_trades: int,
) -> list[str]:
    reasons = []
    pf = result.profit_factor or 0.0
    avg_pnl = result.net_pnl / result.trades if result.trades else 0.0
    if result.trades < min_trades:
        reasons.append("trades_below_target")
    if pf <= 1.15:
        reasons.append("profit_factor_too_low")
    if result.max_drawdown > settings.paper_starting_balance * 0.20:
        reasons.append("drawdown_too_high")
    if avg_pnl <= 0:
        reasons.append("avg_pnl_not_positive")
    if result.stale_fill_rate > 0.20:
        reasons.append("stale_fill_rate_too_high")
    return reasons


def print_top(title: str, rows: list[dict], side_mode: str) -> None:
    ranked = sorted(
        [row for row in rows if row["side_mode"] == side_mode and row["reliable"] == "True"],
        key=lambda row: float(row["net_pnl"]),
        reverse=True,
    )
    print(f"\n{title}")
    if ranked:
        print(ranked[0])
        return
    fallback = sorted(
        [row for row in rows if row["side_mode"] == side_mode],
        key=lambda row: (float(row["profit_factor"] or 0), float(row["net_pnl"])),
        reverse=True,
    )
    print("none")
    print({"best_rejected": fallback[:3]})


def print_rejected_diagnostics(rows: list[dict]) -> None:
    rejected = [row for row in rows if row["reliable"] != "True"]
    breakdown: dict[str, int] = {}
    for row in rejected:
        for reason in str(row.get("reject_reasons", "")).split("|"):
            if reason:
                breakdown[reason] = breakdown.get(reason, 0) + 1
    print("\nreject reasons breakdown")
    print(dict(sorted(breakdown.items(), key=lambda item: item[1], reverse=True)))
    print_ranked_rejected("top 10 best_rejected by profit_factor", rejected, "profit_factor")
    print_ranked_rejected("top 10 best_rejected by net_pnl", rejected, "net_pnl")
    ranked = sorted(
        rejected,
        key=lambda row: float(row["net_pnl"]) / max(float(row["max_drawdown"]), 1.0),
        reverse=True,
    )
    print("\ntop 10 best_rejected by drawdown-adjusted score")
    for row in ranked[:10]:
        print(compact_rejected(row))


def print_ranked_rejected(title: str, rows: list[dict], column: str) -> None:
    print(f"\n{title}")
    ranked = sorted(rows, key=lambda row: float(row[column] or 0), reverse=True)
    for row in ranked[:10]:
        print(compact_rejected(row))


def compact_rejected(row: dict) -> dict:
    config = json.loads(row["config"])
    return {
        "preset": config.get("PRESET"),
        "side_mode": row["side_mode"],
        "entry_windows": config.get("ALLOWED_ENTRY_WINDOWS"),
        "quote_age_range": config.get("QUOTE_AGE_RANGE"),
        "repricing_lag_range": config.get("REPRICING_LAG_RANGE"),
        "min_edge": config.get("MIN_EDGE"),
        "min_confidence": config.get("MIN_CONFIDENCE"),
        "min_inefficiency_score": config.get("MIN_INEFFICIENCY_SCORE"),
        "trades": row["trades"],
        "net_pnl": round(float(row["net_pnl"]), 4),
        "PF": round(float(row["profit_factor"] or 0), 4),
        "DD": round(float(row["max_drawdown"]), 4),
        "stale_fill_rate": round(float(row["stale_fill_rate"]), 4),
        "missed_fill_rate": round(float(row["missed_fill_rate"]), 4),
        "reject_reason": row.get("reject_reasons"),
    }


if __name__ == "__main__":
    main()
