from __future__ import annotations

import argparse
import sqlite3

from app.backtest.filters import add_common_report_args, filters_from_args, signal_time_filter_sql
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def scalar(conn: sqlite3.Connection, sql: str):
    return conn.execute(sql).fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    add_common_report_args(parser)
    args = parser.parse_args()
    filters = filters_from_args(args)
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    path = settings.database_url.removeprefix("sqlite:///")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    print("Counts")
    print("======")
    for table in (
        "btc_ticks",
        "orderbooks",
        "microstructure_events",
        "trades",
        "results",
    ):
        print(table, scalar(conn, f"select count(*) from {table}"))
    filter_sql, params = signal_time_filter_sql("s", filters)
    print("signals", scalar(conn, "select count(*) from signals s where 1=1" + filter_sql))

    print("\nBTC Tick Gaps")
    print("=============")
    row = conn.execute(
        """
        select sum(case when gap_seconds > 2 then 1 else 0 end) as gaps_over_2s,
               round(max(gap_seconds), 3) as max_gap_seconds,
               round(avg(gap_seconds), 3) as avg_gap_seconds
        from (
            select (julianday(timestamp) -
                    julianday(lag(timestamp) over (order by timestamp))) * 86400.0 as gap_seconds
            from btc_ticks
        )
        where gap_seconds is not null
        """
    ).fetchone()
    print(dict(row))
    stale_btc = scalar(
        conn,
        """
        select count(*)
        from (
            select (julianday(timestamp) -
                    julianday(lag(timestamp) over (order by timestamp))) * 86400.0 as gap_seconds
            from btc_ticks
        )
        where gap_seconds > 2
        """,
    )
    print("stale_btc_tick_gaps_over_2s", stale_btc)

    print("\nOrderbook Quality")
    print("=================")
    stale_books = scalar(
        conn,
        """
        select count(*)
        from orderbooks
        where best_bid is null or best_ask is null or spread is null
        """,
    )
    print("stale_or_incomplete_orderbooks", stale_books)
    missing_ptb = scalar(conn, "select count(*) from markets where price_to_beat <= 0")
    print("missing_price_to_beat", missing_ptb)

    print("\nMicrostructure Missing Fields")
    print("=============================")
    for column in ("quote_age_ms", "repricing_lag_ms", "imbalance_ratio"):
        n = scalar(
            conn,
            f"select count(*) from signals where {column} is null",
        )
        print(f"missing_{column}", n)

    print("\nReconnects / Data Quality Events")
    print("================================")
    for row in conn.execute(
        """
        select source, event_type, severity, count(*) as n
        from data_quality_events
        group by source, event_type, severity
        order by n desc
        """
    ):
        print(dict(row))

    print("\nSettlement Sources")
    print("==================")
    for row in conn.execute(
        """
        select coalesce(settlement_source, 'missing') as source, count(*) as n
        from results
        group by source
        order by n desc
        """
    ):
        print(dict(row))


if __name__ == "__main__":
    main()
