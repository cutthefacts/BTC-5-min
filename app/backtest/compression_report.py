from __future__ import annotations

import argparse

from app.backtest.filters import add_common_report_args, filters_from_args
from app.backtest.presets import load_preset
from app.backtest.research import (
    classify_research_regime,
    filtered_candidate_rows,
    grouped_metrics,
    open_conn,
)


def section(title: str) -> None:
    print(f"\n{title}")
    print("=" * len(title))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="candidate_v1")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = filters_from_args(args)
    rows = [
        row
        for row in filtered_candidate_rows(conn, preset, filters)
        if classify_research_regime(row) == "compression"
    ]
    print("Compression Regime Report")
    print("=========================")
    print({"preset": preset.name, "compression_trades": len(rows)})
    for title, column, step in (
        ("UP vs DOWN", "outcome", None),
        ("PnL by quote_age", "quote_age_ms", 250),
        ("PnL by repricing_lag", "repricing_lag_ms", 250),
        ("PnL by edge", "edge", 0.05),
        ("PnL by confidence", "confidence", 0.10),
        ("PnL by imbalance", "imbalance_ratio", 0.20),
        ("PnL by distance_bps", "distance_bps", 5),
        ("PnL by vol_60s", "realized_vol_60s_bps", 5),
        ("PnL by drift_60s", "drift_60s_bps", 5),
        ("PnL by spread_bps", "spread_bps", 25),
    ):
        section(title)
        for item in grouped_metrics(rows, column, step):
            print(item)


if __name__ == "__main__":
    main()
