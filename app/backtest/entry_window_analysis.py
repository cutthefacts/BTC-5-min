from __future__ import annotations

import sqlite3

from app.backtest.analytics import metric_dict, numeric_bucket, summarize_bucket
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        select s.timestamp, s.outcome, s.edge, s.imbalance_ratio, s.quote_age_ms,
               s.repricing_lag_ms, s.market_probability, r.winning_token_id,
               m.up_token_id, m.down_token_id,
               (julianday(m.end_time) - julianday(s.timestamp)) * 86400.0 as seconds_to_close
        from signals s
        join markets m on m.condition_id = s.market_id
        join results r on r.market_id = s.market_id
        where s.outcome is not null
          and s.inefficiency_score >= ?
          and s.confidence >= ?
          and s.edge >= ?
        order by s.timestamp
        """,
        (settings.min_inefficiency_score, settings.min_confidence, settings.min_edge),
    ).fetchall()
    grouped: dict[str, list[tuple[float, float]]] = {}
    lag_grouped: dict[str, list[float]] = {}
    imbalance_grouped: dict[str, list[float]] = {}
    for row in rows:
        token_id = row["up_token_id"] if row["outcome"] == "UP" else row["down_token_id"]
        fee = settings.fee_bps / 10_000
        pnl = (
            1.0 / float(row["market_probability"])
            if token_id == row["winning_token_id"]
            else 0.0
        ) - 1.0 - fee
        bucket = numeric_bucket(row["seconds_to_close"], 15)
        grouped.setdefault(bucket, []).append((pnl, fee))
        lag_grouped.setdefault(bucket, []).append(float(row["repricing_lag_ms"] or 0))
        imbalance_grouped.setdefault(bucket, []).append(float(row["imbalance_ratio"] or 0))
    print("Entry Window Analysis")
    print("=====================")
    best = None
    for bucket, pnl_fees in sorted(grouped.items()):
        pnls = [item[0] for item in pnl_fees]
        fees = [item[1] for item in pnl_fees]
        metrics = metric_dict(summarize_bucket(bucket, pnls, fees))
        avg_lag = avg(lag_grouped.get(bucket, []))
        avg_imbalance = avg(imbalance_grouped.get(bucket, []))
        row = {**metrics, "avg_repricing_lag_ms": avg_lag, "avg_imbalance": avg_imbalance}
        print(row)
        if metrics["trades"] >= 20 and (best is None or metrics["net_pnl"] > best["net_pnl"]):
            best = row
    print("\nBest Historical Window")
    print("======================")
    print(best or "not enough reliable windows")


def avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


if __name__ == "__main__":
    main()
