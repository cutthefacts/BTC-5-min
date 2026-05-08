from __future__ import annotations

import argparse
from dataclasses import replace

from app.backtest.filters import TimeFilters, add_common_report_args, filters_from_args
from app.backtest.presets import load_preset
from app.backtest.research import iter_time_windows, open_conn, replay_dict, run_replay, time_bounds
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="balanced")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    strict = preset.name == "candidate_v1"
    filters = filters_from_args(args)
    if strict:
        filters = replace(
            filters,
            regime_source="snapshot",
            strategy_name=filters.strategy_name or preset.name,
            feature_schema_version=filters.feature_schema_version
            or get_settings().feature_schema_version,
            forward_only=args.forward_only,
        )
    result = run_replay(conn, preset, filters, latency_ms=250)
    reasons = gate_reasons(result, strict=strict)
    validation_pf = None
    bounds = time_bounds(conn, filters)
    if strict and args.forward_only and result.trades < 300:
        print("Research Edge Gate")
        print("==================")
        print(
            {
                "status": "INSUFFICIENT_FORWARD_DATA",
                "reasons": ["forward_snapshot_trades_below_300"],
                **replay_dict(result),
            }
        )
        return
    if bounds:
        start, end = bounds
        midpoint = start + (end - start) / 2
        train = run_replay(
            conn,
            preset,
            replace(filters, from_timestamp=start.isoformat(), to_timestamp=midpoint.isoformat()),
        )
        validation = run_replay(
            conn,
            preset,
            replace(filters, from_timestamp=midpoint.isoformat(), to_timestamp=end.isoformat()),
        )
        train_pf = train.profit_factor or 0.0
        validation_pf = validation.profit_factor or 0.0
        degradation = (train_pf - validation_pf) / train_pf if train_pf else 0.0
        min_validation_pf = 1.25 if strict else 1.1
        if validation_pf < min_validation_pf:
            reasons.append("validation_pf_too_low")
        if strict and degradation > 0.20:
            reasons.append("walk_forward_degradation_too_high")
        if strict:
            reasons.extend(strict_candidate_reasons(conn, preset, start, end))
            forward = SQLiteStore(get_settings().database_url).strategy_trade_summary(preset.name)
            if float(forward["trades"] or 0) < 30:
                reasons.append("forward_paper_insufficient")
    print("Research Edge Gate")
    print("==================")
    print(
        {
            "status": "PASS" if not reasons else "FAIL",
            "reasons": reasons,
            "validation_pf": validation_pf,
            **replay_dict(result),
        }
    )


def gate_reasons(result, strict: bool = False) -> list[str]:
    reasons = []
    min_trades = 300 if strict else 80
    min_pf = 1.5 if strict else 1.3
    max_stale = 0.05 if strict else 0.15
    max_missed = 0.12 if strict else 0.20
    if result.trades < min_trades:
        reasons.append(f"trades_below_{min_trades}")
    if (result.profit_factor or 0) < min_pf:
        reasons.append(f"profit_factor_below_{str(min_pf).replace('.', '_')}")
    if result.max_drawdown > 200:
        reasons.append("drawdown_too_high")
    if result.stale_fill_rate > max_stale:
        reasons.append("stale_fill_rate_too_high")
    if result.missed_fill_rate > max_missed:
        reasons.append("missed_fill_rate_too_high")
    return reasons


def strict_candidate_reasons(conn, preset, start, end) -> list[str]:
    reasons: list[str] = []
    for side in ("UP_ONLY", "DOWN_ONLY"):
        side_result = run_replay(
            conn,
            preset,
            TimeFilters(
                True,
                start.isoformat(),
                end.isoformat(),
                regime_source="snapshot",
                strategy_name=preset.name,
                feature_schema_version=get_settings().feature_schema_version,
            ),
            side_mode=side,
        )
        if side_result.trades >= 30 and side_result.net_pnl < 0:
            reasons.append(f"{side.lower()}_persistent_negative_pnl")
    for left, right in iter_time_windows(start, end, 3):
        result = run_replay(
            conn,
            preset,
            TimeFilters(
                True,
                left.isoformat(),
                right.isoformat(),
                regime_source="snapshot",
                strategy_name=preset.name,
                feature_schema_version=get_settings().feature_schema_version,
            ),
        )
        if result.trades >= 30 and (result.profit_factor or 0.0) < 0.8:
            reasons.append("rolling_3h_pf_below_0_8")
            break
    return reasons


if __name__ == "__main__":
    main()
