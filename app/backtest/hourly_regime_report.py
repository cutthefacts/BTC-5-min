from __future__ import annotations

import argparse

from app.backtest.filters import add_common_report_args, filters_from_args
from app.backtest.presets import load_preset
from app.backtest.research import (
    classify_research_regime,
    filtered_candidate_rows,
    grouped_metrics,
    open_conn,
    parse_dt,
)
from app.strategy.edge_quality import edge_quality_score


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
        regime = classify_research_regime(row)
        row["utc_hour"] = str(parse_dt(row["timestamp"]).hour).zfill(2)
        row["hour_regime"] = f"{row['utc_hour']}:{regime}"
        row["edge_quality"] = edge_quality_score(
            float(row.get("edge") or 0.0),
            None,
            float(row.get("quote_age_ms") or 0.0),
            float(row.get("repricing_lag_ms") or 0.0),
            0.0,
            None,
        )
    print("Hourly Regime Report")
    print("====================")
    print("\nPF by UTC hour")
    for item in grouped_metrics(rows, "utc_hour", None):
        print(item)
    print("\nPF by UTC hour and regime")
    for item in grouped_metrics(rows, "hour_regime", None):
        print(item)
    print("\nEdge quality by UTC hour")
    for hour in sorted({row["utc_hour"] for row in rows}):
        values = [float(row["edge_quality"]) for row in rows if row["utc_hour"] == hour]
        print({"utc_hour": hour, "avg_edge_quality": round(sum(values) / len(values), 4)})


if __name__ == "__main__":
    main()
