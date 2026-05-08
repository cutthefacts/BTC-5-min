import sqlite3

from app.backtest.filters import TimeFilters
from app.backtest.replay import TickByTickReplay


def test_replay_latency_simulation_misses_when_no_future_quote(tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "test.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        create table signals(
            timestamp text, market_id text, action text, outcome text,
            inefficiency_score real, confidence real, edge real,
            market_probability real, expected_probability real, quote_age_ms real,
            repricing_lag_ms real, imbalance_ratio real, liquidity_sweep integer
        );
        create table markets(
            condition_id text, up_token_id text, down_token_id text, end_time text
        );
        create table results(market_id text, winning_token_id text);
        create table orderbooks(token_id text, timestamp text, best_ask real);
        insert into markets values ('m1', 'up', 'down', '2026-01-01T00:03:00+00:00');
        insert into results values ('m1', 'up');
        insert into signals values (
            '2026-01-01T00:00:00+00:00', 'm1', 'HOLD', 'UP',
            0.8, 0.8, 0.1, 0.5, 0.6, 2000, 1000, 0.5, 0
        );
        """
    )
    result = TickByTickReplay(conn).run(250, 5, 0.5, 0.5, 0.01, 1500, 750)
    assert result.trades == 0
    assert result.missed_fill_rate == 1


def test_replay_marks_stale_when_edge_disappears(tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "test.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        create table signals(
            timestamp text, market_id text, action text, outcome text,
            inefficiency_score real, confidence real, edge real,
            market_probability real, expected_probability real, quote_age_ms real,
            repricing_lag_ms real, imbalance_ratio real, liquidity_sweep integer
        );
        create table markets(
            condition_id text, up_token_id text, down_token_id text, end_time text
        );
        create table results(market_id text, winning_token_id text);
        create table orderbooks(token_id text, timestamp text, best_ask real);
        insert into markets values ('m1', 'up', 'down', '2026-01-01T00:03:00+00:00');
        insert into results values ('m1', 'up');
        insert into signals values (
            '2026-01-01T00:00:00+00:00', 'm1', 'HOLD', 'UP',
            0.8, 0.8, 0.035, 0.5, 0.535, 200, 1000, 0.5, 0
        );
        insert into orderbooks values ('up', '2026-01-01T00:00:00.250000+00:00', 0.525);
        """
    )
    result = TickByTickReplay(conn).run(250, 5, 0.5, 0.5, 0.01)
    assert result.trades == 1
    assert result.stale_fill_rate == 1
    assert result.stale_reasons == {"edge_disappeared_before_fill": 1}


def test_replay_only_complete_microstructure_filters_missing_rows(tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "test.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        create table signals(
            timestamp text, market_id text, action text, outcome text,
            inefficiency_score real, confidence real, edge real,
            market_probability real, expected_probability real, quote_age_ms real,
            repricing_lag_ms real, imbalance_ratio real, liquidity_sweep integer
        );
        create table markets(
            condition_id text, up_token_id text, down_token_id text, end_time text
        );
        create table results(market_id text, winning_token_id text);
        create table orderbooks(token_id text, timestamp text, best_ask real);
        insert into markets values ('m1', 'up', 'down', '2026-01-01T00:03:00+00:00');
        insert into results values ('m1', 'up');
        insert into signals values (
            '2026-01-01T00:00:00+00:00', 'm1', 'HOLD', 'UP',
            null, 0.8, 0.1, 0.5, 0.6, 200, 500, 0.5, 0
        );
        insert into orderbooks values ('up', '2026-01-01T00:00:00.250000+00:00', 0.5);
        """
    )
    result = TickByTickReplay(conn).run(
        250,
        5,
        0.0,
        0.0,
        0.0,
        include_hold_candidates=True,
        time_filters=TimeFilters(only_complete_microstructure=True),
    )
    assert result.trades == 0


def test_replay_soft_filters_penalize_instead_of_blocking(tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "test.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        create table signals(
            timestamp text, market_id text, action text, outcome text,
            inefficiency_score real, confidence real, edge real,
            market_probability real, expected_probability real, quote_age_ms real,
            repricing_lag_ms real, imbalance_ratio real, liquidity_sweep integer
        );
        create table markets(
            condition_id text, up_token_id text, down_token_id text, end_time text
        );
        create table results(market_id text, winning_token_id text);
        create table orderbooks(token_id text, timestamp text, best_ask real);
        insert into markets values ('m1', 'up', 'down', '2026-01-01T00:03:00+00:00');
        insert into results values ('m1', 'up');
        insert into signals values (
            '2026-01-01T00:00:00+00:00', 'm1', 'HOLD', 'UP',
            0.1, 0.1, 0.25, 0.5, 0.75, 2000, 1200, 0.05, 1
        );
        insert into orderbooks values ('up', '2026-01-01T00:00:00.250000+00:00', 0.5);
        """
    )
    hard = TickByTickReplay(conn).run(
        250,
        5,
        0.8,
        0.8,
        0.05,
        max_quote_age_ms=1000,
        max_repricing_lag_ms=750,
        avoid_liquidity_sweep=True,
    )
    soft = TickByTickReplay(conn).run(
        250,
        5,
        0.8,
        0.8,
        0.05,
        max_quote_age_ms=1000,
        max_repricing_lag_ms=750,
        avoid_liquidity_sweep=True,
        soft_filters=True,
    )
    assert hard.trades == 0
    assert soft.trades == 1
