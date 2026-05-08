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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="candidate_v1")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = filters_from_args(args)
    rows = filtered_candidate_rows(conn, preset, filters)
    for row in rows:
        row["regime"] = classify_research_regime(row)
        row["regime_source_bucket"] = row.get("regime_source") or "proxy"
    print("Regime Report")
    print("=============")
    for item in grouped_metrics(rows, "regime", None):
        print(item)
    print("\nUP/DOWN by regime")
    for row in rows:
        row["side_regime"] = f"{row.get('outcome')}:{row.get('regime')}"
    for item in grouped_metrics(rows, "side_regime", None):
        print(item)
    print("\nRegime source")
    for item in grouped_metrics(rows, "regime_source_bucket", None):
        print(item)
    for title, column, step in (
        ("\nRegime confidence", "regime_confidence", 0.25),
        ("\nDistance bps", "distance_bps", 5),
        ("\nRealized vol 60s", "realized_vol_60s_bps", 5),
        ("\nDrift 60s", "drift_60s_bps", 5),
        ("\nSpread bps", "spread_bps", 25),
        ("\nDepth 3", "bid_depth_3", 500),
    ):
        print(title)
        for item in grouped_metrics(rows, column, step):
            print(item)


if __name__ == "__main__":
    main()
