from __future__ import annotations

import argparse
import sqlite3
from datetime import timedelta

from app.backtest.filters import TimeFilters, add_common_report_args, signal_time_filter_sql
from app.config import get_settings
from app.storage.sqlite import SQLiteStore

INTERVALS_MS = (250, 500, 1_000, 2_000)


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
    signals = conn.execute(
        """
        select s.*, m.up_token_id, m.down_token_id
        from signals s
        join markets m on m.condition_id = s.market_id
        where s.outcome is not null
          and s.market_probability > 0
        """ + filter_sql + """
        order by s.timestamp
        """,
        params,
    ).fetchall()

    print("Edge Decay")
    print("==========")
    for interval in INTERVALS_MS:
        deltas = []
        vanished = 0
        samples = 0
        for signal in signals:
            token_id = (
                signal["up_token_id"] if signal["outcome"] == "UP" else signal["down_token_id"]
            )
            future = future_ask(conn, token_id, signal["timestamp"], interval)
            if future is None:
                continue
            samples += 1
            delta = float(future) - float(signal["market_probability"])
            deltas.append(delta)
            if float(signal["expected_probability"]) - float(future) < settings.min_edge:
                vanished += 1
        avg_delta = sum(deltas) / len(deltas) if deltas else 0.0
        vanish_rate = vanished / samples if samples else 0.0
        print(
            {
                "interval_ms": interval,
                "samples": samples,
                "avg_ask_drift": round(avg_delta, 5),
                "edge_vanish_rate": round(vanish_rate, 4),
            }
        )


def future_ask(
    conn: sqlite3.Connection,
    token_id: str,
    timestamp: str,
    interval_ms: int,
) -> float | None:
    target = (
        sqlite_datetime(timestamp) + timedelta(milliseconds=interval_ms)
    ).isoformat()
    row = conn.execute(
        """
        select best_ask
        from orderbooks
        where token_id = ?
          and timestamp >= ?
          and best_ask is not null
        order by timestamp asc
        limit 1
        """,
        (token_id, target),
    ).fetchone()
    return float(row["best_ask"]) if row else None


def sqlite_datetime(raw: str):
    from datetime import datetime

    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


if __name__ == "__main__":
    main()
