from datetime import UTC, datetime, timedelta

from app.backtest.missed_opportunities_report import detect_missed_opportunities
from app.storage.sqlite import SQLiteStore


def test_missed_opportunity_detection(tmp_path) -> None:
    store = SQLiteStore(f"sqlite:///{tmp_path / 'test.sqlite3'}")
    store.init()
    conn = store.conn
    now = datetime.now(UTC)
    conn.execute(
        """
        insert into markets values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "m1",
            "q",
            "slug",
            now.isoformat(),
            (now + timedelta(minutes=5)).isoformat(),
            100,
            "up",
            "down",
            1,
        ),
    )
    conn.execute(
        """
        insert into results(
            market_id, winning_token_id, settled_at, pnl, winning_outcome,
            price_to_beat, final_price, gross_payout, cost_basis, fees, trade_count,
            settlement_source
        ) values ('m1', 'up', ?, 0, 'UP', 100, 101, 0, 0, 0, 0, 'binance')
        """,
        (now.isoformat(),),
    )
    conn.execute(
        """
        insert into signals(
            timestamp, market_id, action, outcome, expected_probability,
            market_probability, edge, strength, reason, inefficiency_score,
            confidence, quote_age_ms, repricing_lag_ms, imbalance_ratio, liquidity_sweep
        ) values (?, 'm1', 'HOLD', 'UP', 0.7, 0.5, 0.2, 0.8, 'low_confidence',
                  0.8, 0.4, 1000, 500, 0.5, 0)
        """,
        (now.isoformat(),),
    )
    conn.commit()
    assert detect_missed_opportunities(conn, store, 0.05) == 1
