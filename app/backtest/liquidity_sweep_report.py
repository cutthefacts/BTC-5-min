from __future__ import annotations

import argparse
import sqlite3

from app.backtest.analytics import metric_dict, numeric_bucket, summarize_bucket
from app.backtest.filters import TimeFilters, add_common_report_args, signal_time_filter_sql
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    add_common_report_args(parser)
    args = parser.parse_args()
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    conn.row_factory = sqlite3.Row
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    filter_sql, params = signal_time_filter_sql("s", filters)
    rows = conn.execute(
        """
        select s.*, m.up_token_id, m.down_token_id, m.end_time, r.winning_token_id,
               (julianday(m.end_time) - julianday(s.timestamp)) * 86400.0 as seconds_to_close
        from signals s
        join markets m on m.condition_id = s.market_id
        join results r on r.market_id = s.market_id
        where s.outcome is not null
          and s.liquidity_sweep = 1
        """ + filter_sql + """
        order by s.timestamp
        """,
        params,
    ).fetchall()
    grouped: dict[str, list[float]] = {}
    print("Liquidity Sweep Report")
    print("======================")
    for row in rows:
        token_id = row["up_token_id"] if row["outcome"] == "UP" else row["down_token_id"]
        price = float(row["market_probability"] or 0)
        if price <= 0:
            continue
        fee = settings.fee_bps / 10_000
        pnl = (1.0 / price if token_id == row["winning_token_id"] else 0.0) - 1.0 - fee
        direction = "bid_sweep" if float(row["imbalance_ratio"] or 0) < 0 else "ask_sweep"
        predicted = "continuation" if pnl > 0 else "reversal_or_trap"
        bucket = "|".join(
            [
                direction,
                f"side={row['outcome']}",
                f"window={numeric_bucket(row['seconds_to_close'], 30)}",
                f"predicted={predicted}",
            ]
        )
        grouped.setdefault(bucket, []).append(pnl)
    for bucket, pnls in sorted(grouped.items()):
        metrics = summarize_bucket(
            bucket,
            pnls,
            min_trades=settings.min_reliable_trades_per_bucket,
        )
        print(metric_dict(metrics))
    print(
        {
            "samples": len(rows),
            "note": (
                "liquidity_sweep=true is currently avoided by strategy "
                "when AVOID_LIQUIDITY_SWEEP=true"
            ),
        }
    )


if __name__ == "__main__":
    main()
