from __future__ import annotations

import sqlite3
from datetime import datetime

from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    settings = get_settings()
    store = SQLiteStore(settings.database_url)
    store.init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    conn.row_factory = sqlite3.Row
    store.clear_missed_opportunities()

    detected = detect_missed_opportunities(conn, store, settings.min_edge)
    print("Missed Opportunities")
    print("====================")
    print("detected", detected)
    for row in conn.execute(
        """
        select reason_not_traded, side, count(*) as n,
               round(sum(theoretical_pnl), 4) as theoretical_pnl,
               round(avg(inefficiency_score), 4) as avg_score,
               round(avg(confidence), 4) as avg_confidence
        from missed_opportunities
        group by reason_not_traded, side
        order by theoretical_pnl desc
        """
    ):
        print(dict(row))


def detect_missed_opportunities(
    conn: sqlite3.Connection,
    store: SQLiteStore,
    min_edge: float,
) -> int:
    rows = conn.execute(
        """
        select s.*, m.end_time, r.winning_outcome, r.winning_token_id
        from signals s
        join markets m on m.condition_id = s.market_id
        join results r on r.market_id = s.market_id
        where s.action = 'HOLD'
          and s.outcome is not null
          and s.outcome = r.winning_outcome
          and s.edge > 0
          and abs(s.edge) >= ?
        """,
        (min_edge,),
    ).fetchall()
    detected = 0
    for row in rows:
        seconds_to_close = (
            datetime.fromisoformat(row["end_time"]) - datetime.fromisoformat(row["timestamp"])
        ).total_seconds()
        market_price = float(row["market_probability"] or 0)
        theoretical_pnl = (
            1.0 - market_price if row["outcome"] == row["winning_outcome"] else -market_price
        )
        store.save_missed_opportunity(
            market_id=row["market_id"],
            timestamp=row["timestamp"],
            side=row["outcome"],
            seconds_to_close=seconds_to_close,
            market_price=market_price,
            final_outcome=row["winning_outcome"],
            theoretical_pnl=theoretical_pnl,
            reason_not_traded=row["reason"],
            inefficiency_score=float(row["inefficiency_score"] or 0),
            confidence=float(row["confidence"] or 0),
        )
        detected += 1
    return detected


if __name__ == "__main__":
    main()
