from __future__ import annotations

import argparse
import sqlite3
from dataclasses import asdict
from statistics import mean

from app.backtest.analytics import (
    bool_bucket,
    metric_dict,
    numeric_bucket,
    summarize_bucket,
    theoretical_trade_pnl,
)
from app.backtest.filters import TimeFilters, add_common_report_args, signal_time_filter_sql
from app.backtest.replay import TickByTickReplay
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def bucket(value: float, size: int) -> str:
    start = int(value // size * size)
    return f"{start}-{start + size}"


def rows(conn: sqlite3.Connection, sql: str):
    conn.row_factory = sqlite3.Row
    return list(conn.execute(sql))


def print_section(title: str) -> None:
    print(f"\n{title}")
    print("=" * len(title))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", action="store_true")
    add_common_report_args(parser)
    args = parser.parse_args()
    settings = get_settings()
    path = settings.database_url.removeprefix("sqlite:///")
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    if args.summary:
        print_summary(conn, settings, filters)
        return

    print_section("Repricing Lag Histogram")
    lags = [
        float(row["repricing_lag_ms"])
        for row in rows(conn, "select repricing_lag_ms from microstructure_events")
    ]
    for label, count in histogram(lags, 250).items():
        print(label, count)

    print_section("Quote Staleness")
    ages = [
        float(row["quote_age_ms"])
        for row in rows(conn, "select quote_age_ms from microstructure_events")
    ]
    print_stats("quote_age_ms", ages)

    print_section("Imbalance Distribution")
    imbalances = [
        float(row["imbalance_ratio"])
        for row in rows(conn, "select imbalance_ratio from microstructure_events")
    ]
    print_stats("imbalance_ratio", imbalances)

    print_section("Signals By Inefficiency Score")
    for row in rows(
        conn,
        """
        select action, reason,
               round(coalesce(avg(inefficiency_score), 0), 4) as avg_score,
               round(coalesce(avg(edge), 0), 4) as avg_edge,
               count(*) as n
        from signals
        group by action, reason
        order by n desc
        """,
    ):
        print(dict(row))

    trade_rows = trade_level_rows(conn, filters)
    print_grouped("PnL by inefficiency_score", trade_rows, "inefficiency_score", 0.10)
    print_grouped("PnL by confidence", trade_rows, "confidence", 0.10)
    print_grouped("PnL by quote_age_ms", trade_rows, "quote_age_ms", 500)
    print_grouped("PnL by repricing_lag_ms", trade_rows, "repricing_lag_ms", 250)
    print_grouped("PnL by imbalance_ratio", trade_rows, "imbalance_ratio", 0.20)
    print_grouped_bool("PnL by liquidity_sweep", trade_rows, "liquidity_sweep")
    print_grouped("PnL by seconds_to_close", trade_rows, "seconds_to_close", 15)
    print_grouped_side("PnL by UP/DOWN side", trade_rows)
    print_grouped_side_window("PnL by side AND entry window", trade_rows)
    print_grouped("PnL by market_probability", trade_rows, "market_probability", 0.10)
    print_grouped("PnL by expected_edge", trade_rows, "expected_edge", 0.05)

    print_section("Execution Quality")
    for row in rows(
        conn,
        """
        select count(*) as fills,
               round(avg(signal_to_submit_ms), 2) as avg_signal_to_submit_ms,
               round(avg(submit_to_fill_ms), 2) as avg_submit_to_fill_ms,
               round(avg(total_fill_latency_ms), 2) as avg_total_fill_latency_ms,
               round(avg(expected_edge_at_signal), 4) as avg_expected_edge_at_signal,
               round(avg(expected_edge_at_submit), 4) as avg_expected_edge_at_submit,
               round(avg(realized_edge_after_fill), 4) as avg_realized_edge_after_fill,
               round(coalesce(avg(stale_fill), 0), 4) as stale_fill_rate
        from trades
        """,
    ):
        print(dict(row))

    print_section("Stale Fill Reasons")
    for row in rows(
        conn,
        """
        select coalesce(stale_reason, 'fresh') as stale_reason, count(*) as n
        from trades
        group by coalesce(stale_reason, 'fresh')
        order by n desc
        """,
    ):
        print(dict(row))

    print_section("Maker vs Taker Replay")
    replay = TickByTickReplay(conn)
    for mode in ("taker", "maker", "hybrid"):
        result = replay.run(
            latency_ms=250,
            slippage_bps=settings.slippage_bps,
            min_score=settings.min_inefficiency_score,
            min_confidence=settings.min_confidence,
            min_edge=settings.min_edge,
            stale_quote_ms=settings.stale_quote_ms,
            repricing_lag_ms=settings.repricing_lag_ms,
            include_hold_candidates=True,
            execution_mode=mode,
        )
        print({"mode": mode, **asdict(result)})

    print_section("Settlement")
    for row in rows(
        conn,
        """
        select count(*) as markets,
               coalesce(sum(trade_count), 0) as trades,
               round(coalesce(sum(pnl), 0), 4) as pnl,
               round(coalesce(avg(pnl), 0), 4) as avg_market_pnl
        from results
        where trade_count > 0
        """,
    ):
        print(dict(row))

    print_section("Data Quality")
    for row in rows(
        conn,
        """
        select source, event_type, severity, count(*) as n
        from data_quality_events
        group by source, event_type, severity
        order by n desc
        """,
    ):
        print(dict(row))

    for row in rows(
        conn,
        """
        select round(max(gap_seconds), 3) as max_btc_gap_seconds,
               round(avg(gap_seconds), 3) as avg_btc_gap_seconds
        from (
            select (julianday(timestamp) -
                    julianday(lag(timestamp) over (order by timestamp))) * 86400.0 as gap_seconds
            from btc_ticks
        )
        where gap_seconds is not null
        """,
    ):
        print(dict(row))


def histogram(values: list[float], size: int) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        label = bucket(value, size)
        out[label] = out.get(label, 0) + 1
    return dict(sorted(out.items(), key=lambda item: int(item[0].split("-")[0])))


def print_stats(name: str, values: list[float]) -> None:
    if not values:
        print(name, "no data")
        return
    values = sorted(values)
    p95 = values[int(len(values) * 0.95) - 1]
    print(
        {
            "metric": name,
            "count": len(values),
            "avg": round(mean(values), 4),
            "min": round(values[0], 4),
            "p95": round(p95, 4),
            "max": round(values[-1], 4),
        }
    )


def trade_level_rows(conn: sqlite3.Connection, filters: TimeFilters | None = None) -> list[dict]:
    filter_sql, params = signal_time_filter_sql("s", filters or TimeFilters())
    query = """
        select t.*, r.winning_token_id, s.inefficiency_score, s.confidence,
               s.quote_age_ms, s.repricing_lag_ms, s.imbalance_ratio,
               s.liquidity_sweep, s.market_probability, s.edge as signal_edge,
               (julianday(m.end_time) - julianday(t.timestamp)) * 86400.0 as seconds_to_close
        from trades t
        join results r on r.market_id = t.market_id
        join markets m on m.condition_id = t.market_id
        left join signals s on s.id = (
            select s2.id
            from signals s2
            where s2.market_id = t.market_id
              and s2.outcome = t.outcome
              and s2.timestamp <= t.timestamp
            order by s2.timestamp desc
            limit 1
        )
        where r.trade_count > 0
        """ + filter_sql + """
    """
    out = []
    for row in conn.execute(query, params):
        item = dict(row)
        item["pnl"] = theoretical_trade_pnl(
            row["token_id"],
            row["winning_token_id"],
            float(row["price"]),
            float(row["size"]),
            float(row["fee"]),
        )
        out.append(item)
    return out


def print_grouped(title: str, trade_rows: list[dict], column: str, step: float) -> None:
    print_section(title)
    grouped: dict[str, list[dict]] = {}
    for row in trade_rows:
        grouped.setdefault(numeric_bucket(row.get(column), step), []).append(row)
    print_metrics(grouped)


def print_grouped_bool(title: str, trade_rows: list[dict], column: str) -> None:
    print_section(title)
    grouped: dict[str, list[dict]] = {}
    for row in trade_rows:
        grouped.setdefault(bool_bucket(row.get(column)), []).append(row)
    print_metrics(grouped)


def print_grouped_side(title: str, trade_rows: list[dict]) -> None:
    print_section(title)
    grouped: dict[str, list[dict]] = {}
    for row in trade_rows:
        grouped.setdefault(row.get("outcome") or "missing", []).append(row)
    print_metrics(grouped)


def print_grouped_side_window(title: str, trade_rows: list[dict]) -> None:
    print_section(title)
    grouped: dict[str, list[dict]] = {}
    for row in trade_rows:
        side = row.get("outcome") or "missing"
        window = numeric_bucket(row.get("seconds_to_close"), 15)
        grouped.setdefault(f"{side}:{window}", []).append(row)
    print_metrics(grouped)


def print_metrics(grouped: dict[str, list[dict]]) -> None:
    settings = get_settings()
    for name, items in sorted(grouped.items()):
        pnls = [float(item["pnl"]) for item in items]
        fees = [float(item["fee"]) for item in items]
        metrics = summarize_bucket(
            name,
            pnls,
            fees,
            settings.min_reliable_trades_per_bucket,
        )
        print(metric_dict(metrics))


def print_summary(conn: sqlite3.Connection, settings, filters: TimeFilters) -> None:
    trade_rows = trade_level_rows(conn, filters)
    pnls = [float(row["pnl"]) for row in trade_rows]
    print("Microstructure Summary")
    print("======================")
    metrics = summarize_bucket(
        "complete",
        pnls,
        min_trades=settings.min_reliable_trades_per_bucket,
    )
    print(metric_dict(metrics))
    for title, column, step in (
        ("Best/Worst Entry Windows", "seconds_to_close", 15),
        ("Quote Age Buckets", "quote_age_ms", 500),
        ("Repricing Lag Buckets", "repricing_lag_ms", 250),
    ):
        print_section(title)
        grouped: dict[str, list[dict]] = {}
        for row in trade_rows:
            grouped.setdefault(numeric_bucket(row.get(column), step), []).append(row)
        bucket_metrics = (
            metric_dict(summarize_bucket(k, [float(i["pnl"]) for i in v]))
            for k, v in grouped.items()
        )
        ranked = sorted(
            bucket_metrics,
            key=lambda item: float(item["net_pnl"]),
            reverse=True,
        )
        for item in ranked[:3] + ranked[-3:]:
            print(item)
    print_grouped_side("UP vs DOWN", trade_rows)
    print_grouped_bool("Liquidity Sweep", trade_rows, "liquidity_sweep")
    print_section("Top 5 Conclusions")
    print("1. Ignore buckets marked reliable=False for automatic decisions.")
    print("2. Compare UP and DOWN separately; combined PnL can hide side asymmetry.")
    print("3. Treat quote_age above configured max as stale-trap risk.")
    print("4. Prefer entry windows with positive PF and controlled drawdown.")
    print("5. Live trading remains disabled.")


if __name__ == "__main__":
    main()
