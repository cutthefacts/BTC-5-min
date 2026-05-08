from datetime import UTC, datetime, timedelta

from app.models import Market, Outcome, Signal, SignalAction, Trade
from app.storage.sqlite import SQLiteStore


def test_sqlite_init_and_save_market(tmp_path) -> None:
    store = SQLiteStore(f"sqlite:///{tmp_path / 'test.sqlite3'}")
    store.init()
    now = datetime.now(UTC)
    market = Market(
        condition_id="m1",
        question="BTC Up Down 5m",
        slug="btc",
        start_time=now,
        end_time=now + timedelta(minutes=5),
        price_to_beat=100,
        up_token_id="up",
        down_token_id="down",
    )
    store.save_market(market)
    row = store.conn.execute("select * from markets where condition_id = 'm1'").fetchone()
    assert row["price_to_beat"] == 100


def test_settlement_calculates_market_pnl(tmp_path) -> None:
    store = SQLiteStore(f"sqlite:///{tmp_path / 'test.sqlite3'}")
    store.init()
    now = datetime.now(UTC)
    market = Market(
        condition_id="m1",
        question="BTC Up Down 5m",
        slug="btc-updown-5m-1",
        start_time=now - timedelta(minutes=5),
        end_time=now,
        price_to_beat=100,
        up_token_id="up",
        down_token_id="down",
    )
    store.save_market(market)
    store.save_btc_tick((now + timedelta(seconds=1)).isoformat(), 101)
    store.save_trade(
        Trade(
            order_id="o1",
            market_id="m1",
            token_id="up",
            outcome=Outcome.UP,
            price=0.60,
            size=10,
            fee=0.01,
        )
    )

    candidate = store.settlement_candidates((now + timedelta(seconds=20)).isoformat())[0]
    final_tick = store.final_btc_tick(now.isoformat(), max_lag_seconds=30)
    assert final_tick is not None
    result = store.settle_market(candidate, final_tick["price"], now.isoformat(), "binance")

    assert result["winning_outcome"] == "UP"
    assert result["pnl"] == 3.99
    row = store.conn.execute("select strategy_name from results where market_id = 'm1'").fetchone()
    assert row["strategy_name"] == "baseline"
    summary = store.result_summary()
    assert summary["settled_markets"] == 1
    assert summary["settled_trades"] == 1
    assert summary["pnl"] == 3.99


def test_save_market_does_not_overwrite_known_price_to_beat_with_zero(tmp_path) -> None:
    store = SQLiteStore(f"sqlite:///{tmp_path / 'test.sqlite3'}")
    store.init()
    now = datetime.now(UTC)
    market = Market(
        condition_id="m1",
        question="BTC Up Down 5m",
        slug="btc-updown-5m-1",
        start_time=now,
        end_time=now + timedelta(minutes=5),
        price_to_beat=100,
        up_token_id="up",
        down_token_id="down",
    )
    store.save_market(market)
    market.price_to_beat = 0
    store.save_market(market)

    row = store.conn.execute(
        "select price_to_beat from markets where condition_id = 'm1'"
    ).fetchone()
    assert row["price_to_beat"] == 100


def test_strategy_name_is_stored_for_signal_and_trade(tmp_path) -> None:
    store = SQLiteStore(f"sqlite:///{tmp_path / 'test.sqlite3'}")
    store.init()

    store.save_signal(
        Signal(
            market_id="m1",
            action=SignalAction.BUY_UP,
            outcome=Outcome.UP,
            expected_probability=0.6,
            market_probability=0.5,
            edge=0.1,
            strength=0.8,
            reason="test",
            strategy_name="candidate_v1",
        )
    )
    store.save_trade(
        Trade(
            order_id="o1",
            market_id="m1",
            token_id="up",
            outcome=Outcome.UP,
            price=0.5,
            size=1,
            fee=0.01,
            strategy_name="candidate_v1",
        )
    )

    signal = store.conn.execute("select strategy_name from signals").fetchone()
    trade = store.conn.execute("select strategy_name from trades").fetchone()
    summary = store.strategy_trade_summary("candidate_v1")
    assert signal["strategy_name"] == "candidate_v1"
    assert trade["strategy_name"] == "candidate_v1"
    assert summary["trades"] == 1


def test_signal_snapshot_columns_are_nullable(tmp_path) -> None:
    store = SQLiteStore(f"sqlite:///{tmp_path / 'test.sqlite3'}")
    store.init()
    store.save_signal(
        Signal(
            market_id="m1",
            action=SignalAction.HOLD,
            outcome=None,
            expected_probability=0,
            market_probability=0,
            edge=0,
            strength=0,
            reason="test",
        )
    )
    row = store.conn.execute(
        "select btc_price, regime_source, feature_schema_version from signals"
    ).fetchone()
    assert row["btc_price"] is None
    assert row["regime_source"] is None
    assert row["feature_schema_version"] is None


def test_regime_performance_memory_upsert(tmp_path) -> None:
    store = SQLiteStore(f"sqlite:///{tmp_path / 'test.sqlite3'}")
    store.init()
    store.save_regime_performance(
        updated_at=datetime.now(UTC).isoformat(),
        regime="compression",
        side="UP",
        entry_window="120-150",
        trades=40,
        profit_factor=1.5,
        max_drawdown=4.0,
        stale_fill_rate=0.0,
        missed_fill_rate=0.1,
        rolling_profit_factor=1.4,
        rolling_drawdown=5.0,
        bad_regime_score=0.2,
    )
    row = store.regime_performance("compression", "UP", "120-150")
    assert row is not None
    assert row["trades"] == 40
    assert row["bad_regime_score"] == 0.2
