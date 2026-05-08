import sqlite3

from app.backtest.diagnostics import apply_hard_filters, complete_candidates
from app.backtest.filters import TimeFilters
from app.backtest.presets import load_preset


def test_preset_loading() -> None:
    assert load_preset("balanced").name == "balanced"
    assert load_preset("exploratory").soft_filters is True


def test_filter_relaxation_counting(tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "test.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        create table signals(
            timestamp text, market_id text, action text, outcome text, reason text,
            inefficiency_score real, confidence real, edge real,
            market_probability real, expected_probability real, quote_age_ms real,
            repricing_lag_ms real, imbalance_ratio real, liquidity_sweep integer
        );
        create table markets(
            condition_id text, up_token_id text, down_token_id text, end_time text
        );
        create table results(market_id text, winning_outcome text, winning_token_id text);
        create table orderbooks(
            token_id text, timestamp text, spread real, liquidity real
        );
        insert into markets values ('m1', 'up', 'down', '2026-01-01T00:03:00+00:00');
        insert into results values ('m1', 'UP', 'up');
        insert into orderbooks values ('up', '2025-12-31T23:59:59+00:00', 0.01, 1000);
        insert into signals values (
            '2026-01-01T00:00:00+00:00', 'm1', 'HOLD', 'UP', 'x',
            0.5, 0.6, 0.06, 0.5, 0.6, 200, 500, 0.5, 0
        );
        insert into signals values (
            '2026-01-01T00:00:01+00:00', 'm1', 'HOLD', 'UP', 'x',
            0.5, 0.6, 0.06, 0.5, 0.6, 2000, 500, 0.5, 0
        );
        """
    )
    candidates = complete_candidates(conn, TimeFilters(only_complete_microstructure=True))
    kept, steps = apply_hard_filters(candidates, load_preset("strict"))
    assert len(candidates) == 2
    assert len(kept) <= 1
    assert any(step.reason == "quote_age range" and step.rejected for step in steps)
