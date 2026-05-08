from __future__ import annotations

import sqlite3
from dataclasses import asdict

from app.backtest.replay import TickByTickReplay
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    replay = TickByTickReplay(conn)
    print("Latency Sensitivity")
    print("===================")
    for latency_ms in (50, 100, 250, 500, 1000):
        result = replay.run(
            latency_ms=latency_ms,
            slippage_bps=settings.slippage_bps,
            min_score=settings.min_inefficiency_score,
            min_confidence=settings.min_confidence,
            min_edge=settings.min_edge,
            stale_quote_ms=settings.stale_quote_ms,
            repricing_lag_ms=settings.repricing_lag_ms,
            include_hold_candidates=True,
            execution_mode="taker",
        )
        print({"latency_ms": latency_ms, **asdict(result)})


if __name__ == "__main__":
    main()
