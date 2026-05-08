from __future__ import annotations

import argparse

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import (
    filtered_candidate_rows,
    grouped_metrics,
    open_conn,
    replay_dict,
    run_replay,
)


def section(title: str) -> None:
    print(f"\n{title}")
    print("=" * len(title))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="balanced")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)

    print("Baseline Report")
    print("===============")
    print({"preset": preset.name, **replay_dict(run_replay(conn, preset, filters))})

    rows = filtered_candidate_rows(conn, preset, filters)
    for title, column, step in (
        ("UP vs DOWN", "outcome", None),
        ("PnL by seconds_to_close", "seconds_to_close", 15),
        ("PnL by quote_age", "quote_age_ms", 500),
        ("PnL by repricing_lag", "repricing_lag_ms", 250),
        ("PnL by expected_edge", "edge", 0.05),
        ("PnL by confidence", "confidence", 0.10),
        ("PnL by inefficiency_score", "inefficiency_score", 0.10),
        ("PnL by market_probability", "market_probability", 0.10),
        ("PnL by liquidity_sweep", "liquidity_sweep", None),
    ):
        section(title)
        for item in grouped_metrics(rows, column, step):
            print(item)

    patterns = pattern_metrics(rows)
    section("Top 5 winning patterns")
    for item in patterns[:5]:
        print(item)
    section("Top 5 losing patterns")
    for item in patterns[-5:]:
        print(item)


def pattern_metrics(rows: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        key = "|".join(
            [
                str(row.get("outcome")),
                bucket(row.get("seconds_to_close"), 30),
                bucket(row.get("quote_age_ms"), 500),
                bucket(row.get("repricing_lag_ms"), 250),
            ]
        )
        grouped.setdefault(key, []).append(row)
    ranked = []
    for key, items in grouped.items():
        pnl = sum(float(item["pnl"]) for item in items)
        ranked.append({"pattern": key, "trades": len(items), "net_pnl": round(pnl, 4)})
    return sorted(ranked, key=lambda item: item["net_pnl"], reverse=True)


def bucket(value, step: int) -> str:
    if value is None:
        return "missing"
    start = int(float(value) // step) * step
    return f"{start}-{start + step}"


if __name__ == "__main__":
    main()
