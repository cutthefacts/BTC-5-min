from __future__ import annotations

import sqlite3

from app.config import get_settings
from app.storage.sqlite import SQLiteStore

INTERVALS_MS = (250, 500, 1000, 2000)


def main() -> None:
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        select timestamp, market_id, outcome, expected_probability,
               market_probability, edge
        from signals
        where outcome is not null
          and edge >= ?
          and expected_probability > 0
          and market_probability > 0
        order by timestamp
        """,
        (settings.min_edge,),
    ).fetchall()
    vanished_by_interval = {interval: 0 for interval in INTERVALS_MS}
    samples_by_interval = {interval: 0 for interval in INTERVALS_MS}
    half_lives: list[int] = []
    for row in rows:
        token_id = token_for_signal(conn, row)
        if token_id is None:
            continue
        initial_edge = float(row["edge"])
        row_half_life: int | None = None
        for interval in INTERVALS_MS:
            ask = future_ask(conn, token_id, row["timestamp"], interval)
            if ask is None:
                continue
            samples_by_interval[interval] += 1
            fee_cost = ask * settings.fee_bps / 10_000
            edge = float(row["expected_probability"]) - ask - fee_cost
            if edge <= initial_edge * 0.5:
                vanished_by_interval[interval] += 1
                row_half_life = interval
                break
        if row_half_life is not None:
            half_lives.append(row_half_life)
    half_life = sorted(half_lives)[len(half_lives) // 2] if half_lives else None
    print("Edge Half Life")
    print("==============")
    print({"edge_half_life_ms": half_life, "samples": len(rows)})
    for interval in INTERVALS_MS:
        samples = samples_by_interval[interval]
        rate = vanished_by_interval[interval] / samples if samples else 0.0
        print({"interval_ms": interval, "half_life_hit_rate": round(rate, 4), "samples": samples})


def token_for_signal(conn: sqlite3.Connection, row: sqlite3.Row) -> str | None:
    market = conn.execute(
        "select up_token_id, down_token_id from markets where condition_id = ?",
        (row["market_id"],),
    ).fetchone()
    if market is None:
        return None
    return market["up_token_id"] if row["outcome"] == "UP" else market["down_token_id"]


def future_ask(conn: sqlite3.Connection, token_id: str, timestamp: str, interval_ms: int):
    from datetime import datetime, timedelta

    target = (
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        + timedelta(milliseconds=interval_ms)
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


if __name__ == "__main__":
    main()
