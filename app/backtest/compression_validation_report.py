from __future__ import annotations

import argparse

from app.backtest.analytics import metric_dict, summarize_bucket
from app.backtest.filters import add_common_report_args, filters_from_args
from app.backtest.presets import load_preset
from app.backtest.research import classify_research_regime, filtered_candidate_rows, open_conn


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="candidate_v1")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    rows = filtered_candidate_rows(conn, preset, filters_from_args(args))
    for row in rows:
        row["is_compression"] = classify_research_regime(row) == "compression"
        row["group"] = "compression" if row["is_compression"] else "non_compression"
        row["side_compression"] = f"{row.get('outcome')}:{row['group']}"
    print("Compression Validation Report")
    print("=============================")
    for group in ("group", "side_compression"):
        print(f"\n{group}")
        for item in summarize(rows, group):
            print(item)


def summarize(rows: list[dict], group: str) -> list[dict]:
    output = []
    for key in sorted({str(row.get(group)) for row in rows}):
        items = [row for row in rows if str(row.get(group)) == key]
        metrics = metric_dict(
            summarize_bucket(
                key,
                [float(row["pnl"]) for row in items],
                min_trades=30,
            )
        )
        metrics.update(
            {
                "avg_distance_bps": avg(items, "distance_bps"),
                "avg_vol_60s": avg(items, "realized_vol_60s_bps"),
                "avg_drift_60s": avg(items, "drift_60s_bps"),
                "avg_quote_age": avg(items, "quote_age_ms"),
                "avg_repricing_lag": avg(items, "repricing_lag_ms"),
                "stale_fill_rate": 0.0,
                "missed_fill_rate": 0.0,
            }
        )
        output.append(metrics)
    return output


def avg(rows: list[dict], column: str) -> float | None:
    values = [float(row[column]) for row in rows if row.get(column) is not None]
    return round(sum(values) / len(values), 4) if values else None


if __name__ == "__main__":
    main()
