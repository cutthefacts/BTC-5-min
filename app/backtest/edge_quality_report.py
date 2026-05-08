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
from app.strategy.edge_quality import edge_quality_score, extreme_edge_reason


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="candidate_v1")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = filters_from_args(args)
    rows = filtered_candidate_rows(conn, preset, filters)
    extreme = 0
    for row in rows:
        regime = classify_research_regime(row)
        edge = float(row.get("edge") or 0.0)
        quote_age = float(row.get("quote_age_ms") or 0.0)
        lag = float(row.get("repricing_lag_ms") or 0.0)
        row["edge_quality_score"] = edge_quality_score(edge, None, quote_age, lag, 0.0, None)
        row["edge_quality_bucket"] = quality_bucket(row["edge_quality_score"])
        if extreme_edge_reason(edge, quote_age, lag, 0.0, regime):
            extreme += 1
            row["extreme_edge"] = "true"
        else:
            row["extreme_edge"] = "false"
    print("Edge Quality Report")
    print("===================")
    print({"preset": preset.name, "trades": len(rows), "extreme_edge_candidates": extreme})
    print("\nPnL by edge_quality")
    for item in grouped_metrics(rows, "edge_quality_bucket", None):
        print(item)
    print("\nPnL by extreme_edge")
    for item in grouped_metrics(rows, "extreme_edge", None):
        print(item)


def quality_bucket(value: float) -> str:
    if value < 0.4:
        return "low"
    if value < 0.65:
        return "medium"
    return "high"


if __name__ == "__main__":
    main()
