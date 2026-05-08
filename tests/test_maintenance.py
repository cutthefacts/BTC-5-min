from datetime import UTC, datetime, timedelta

from app.storage.maintenance import prune_sqlite
from app.storage.sqlite import SQLiteStore


def test_prune_keeps_research_tables_and_deletes_raw_data(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'test.sqlite3'}"
    store = SQLiteStore(database_url)
    store.init()
    old = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
    new = datetime.now(UTC).isoformat()
    store.conn.execute(
        """
        insert into orderbooks(timestamp, market_id, token_id, raw_json)
        values (?, 'm1', 'up', '{}'), (?, 'm1', 'up', '{}')
        """,
        (old, new),
    )
    store.conn.execute(
        """
        insert into microstructure_events(
            timestamp, market_id, token_id, bid_volume, ask_volume,
            weighted_bid_volume, weighted_ask_volume, imbalance_ratio,
            imbalance_acceleration, quote_age_ms, repricing_lag_ms,
            rapid_ask_disappearance, rapid_bid_disappearance,
            disappearing_liquidity, liquidity_sweep, aggressive_repricing,
            liquidity_vacuum
        )
        values (?, 'm1', 'up', 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
               (?, 'm1', 'up', 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        """,
        (old, new),
    )
    store.conn.execute(
        """
        insert into signals(
            timestamp, market_id, action, expected_probability, market_probability,
            edge, strength, reason
        )
        values (?, 'm1', 'HOLD', 0, 0, 0, 0, 'test')
        """,
        (old,),
    )
    store.conn.commit()

    result = prune_sqlite(database_url, keep_hours=12, vacuum=False, dry_run=False)

    assert result.deleted == {"orderbooks": 1, "microstructure_events": 1}
    assert store.conn.execute("select count(*) from orderbooks").fetchone()[0] == 1
    assert store.conn.execute("select count(*) from microstructure_events").fetchone()[0] == 1
    assert store.conn.execute("select count(*) from signals").fetchone()[0] == 1
