from datetime import UTC, datetime, timedelta

from app.config import Settings
from app.models import (
    FeatureSnapshot,
    Market,
    MicrostructureSnapshot,
    OrderBook,
    OrderBookSide,
    SignalAction,
)
from app.strategy.reactive import ReactiveDirectionalStrategy


def book(token: str, bid: float = 0.55, ask: float = 0.57) -> OrderBook:
    return OrderBook(
        token_id=token,
        market_id="m1",
        bids=[OrderBookSide(bid, 500)],
        asks=[OrderBookSide(ask, 500)],
    )


def market() -> Market:
    now = datetime.now(UTC)
    return Market(
        condition_id="m1",
        question="Bitcoin Up or Down 5m",
        slug="btc-up-down-5m",
        start_time=now - timedelta(seconds=80),
        end_time=now + timedelta(seconds=220),
        price_to_beat=100_000,
        up_token_id="up",
        down_token_id="down",
    )


def micro(score_age: float = 3_000) -> MicrostructureSnapshot:
    return MicrostructureSnapshot(
        market_id="m1",
        token_id="up",
        bid_volume=500,
        ask_volume=100,
        weighted_bid_volume=500,
        weighted_ask_volume=100,
        imbalance_ratio=0.70,
        imbalance_acceleration=0.20,
        quote_age_ms=score_age,
        repricing_lag_ms=1_000,
        last_repricing_at=None,
        rapid_ask_disappearance=True,
        rapid_bid_disappearance=False,
        disappearing_liquidity=True,
        liquidity_sweep=True,
        aggressive_repricing=False,
        liquidity_vacuum=False,
    )


def test_strategy_requires_edge() -> None:
    settings = Settings(
        min_edge=0.20,
        up_min_edge=0.20,
        max_spread=0.05,
        min_liquidity_usd=10,
        min_inefficiency_score=0.10,
        up_min_inefficiency_score=0.10,
        min_confidence=0.10,
        up_min_confidence=0.10,
        up_market_probability_min=0.0,
        avoid_liquidity_sweep=False,
        up_allowed_entry_windows="0-300",
        blocked_entry_windows_seconds_to_close="",
        max_quote_age_ms_filter=5000,
        max_repricing_lag_ms=5000,
        max_reasonable_edge=1.0,
    )
    strategy = ReactiveDirectionalStrategy(settings)
    f = FeatureSnapshot(
        market=market(),
        btc_price=100_100,
        distance_bps=10,
        momentum_bps={5: 5, 15: 6, 30: 4, 60: 2},
        volatility_bps=5,
        up_book=book("up", bid=0.68, ask=0.70),
        down_book=book("down", bid=0.40, ask=0.43),
        microstructure=micro(),
    )
    signal = strategy.evaluate(f)
    assert signal.action == SignalAction.HOLD
    assert signal.reason == "negative_ev"


def test_strategy_buys_up_when_direction_and_edge_clear() -> None:
    settings = Settings(
        min_edge=0.02,
        up_min_edge=0.02,
        max_spread=0.05,
        min_liquidity_usd=10,
        min_inefficiency_score=0.10,
        up_min_inefficiency_score=0.10,
        min_confidence=0.10,
        up_min_confidence=0.10,
        up_market_probability_min=0.0,
        avoid_liquidity_sweep=False,
        up_allowed_entry_windows="0-300",
        blocked_entry_windows_seconds_to_close="",
        max_quote_age_ms_filter=5000,
        max_repricing_lag_ms=5000,
        max_reasonable_edge=1.0,
    )
    strategy = ReactiveDirectionalStrategy(settings)
    f = FeatureSnapshot(
        market=market(),
        btc_price=100_180,
        distance_bps=18,
        momentum_bps={5: 10, 15: 9, 30: 5, 60: 2},
        volatility_bps=5,
        up_book=book("up", bid=0.28, ask=0.30),
        down_book=book("down", bid=0.45, ask=0.47),
        microstructure=micro(),
    )
    signal = strategy.evaluate(f)
    assert signal.action == SignalAction.BUY_UP
    assert signal.edge > settings.min_edge


def test_strategy_blocks_wide_spread() -> None:
    settings = Settings(max_spread=0.02, min_liquidity_usd=10)
    strategy = ReactiveDirectionalStrategy(settings)
    f = FeatureSnapshot(
        market=market(),
        btc_price=100_180,
        distance_bps=18,
        momentum_bps={5: 10, 15: 9, 30: 5, 60: 2},
        volatility_bps=5,
        up_book=book("up", bid=0.40, ask=0.50),
        down_book=book("down", bid=0.45, ask=0.47),
        microstructure=micro(),
    )
    signal = strategy.evaluate(f)
    assert signal.action == SignalAction.HOLD
    assert signal.reason == "spread_too_wide"


def test_strategy_blocks_bad_entry_window() -> None:
    settings = Settings(
        max_spread=0.05,
        min_liquidity_usd=10,
        avoid_liquidity_sweep=False,
        up_allowed_entry_windows="90-180",
    )
    strategy = ReactiveDirectionalStrategy(settings)
    f = FeatureSnapshot(
        market=market(),
        btc_price=100_180,
        distance_bps=18,
        momentum_bps={5: 10, 15: 9, 30: 5, 60: 2},
        volatility_bps=5,
        up_book=book("up", bid=0.50, ask=0.52),
        down_book=book("down", bid=0.45, ask=0.47),
        microstructure=micro(),
    )
    signal = strategy.evaluate(f)
    assert signal.action == SignalAction.HOLD
    assert signal.reason == "entry_window_blocked"


def test_strategy_blocks_quote_and_lag_ranges() -> None:
    settings = Settings(
        max_spread=0.05,
        min_liquidity_usd=10,
        avoid_liquidity_sweep=False,
        up_allowed_entry_windows="0-300",
        blocked_entry_windows_seconds_to_close="",
        min_quote_age_ms=100,
        max_quote_age_ms_filter=500,
        min_repricing_lag_ms=250,
        max_repricing_lag_ms=750,
    )
    strategy = ReactiveDirectionalStrategy(settings)
    f = FeatureSnapshot(
        market=market(),
        btc_price=100_180,
        distance_bps=18,
        momentum_bps={5: 10, 15: 9, 30: 5, 60: 2},
        volatility_bps=5,
        up_book=book("up", bid=0.50, ask=0.52),
        down_book=book("down", bid=0.45, ask=0.47),
        microstructure=micro(score_age=2000),
    )
    signal = strategy.evaluate(f)
    assert signal.action == SignalAction.HOLD
    assert signal.reason == "quote_age_range"


def test_strategy_avoids_liquidity_sweep() -> None:
    settings = Settings(
        max_spread=0.05,
        min_liquidity_usd=10,
        up_allowed_entry_windows="0-300",
        blocked_entry_windows_seconds_to_close="",
    )
    strategy = ReactiveDirectionalStrategy(settings)
    f = FeatureSnapshot(
        market=market(),
        btc_price=100_180,
        distance_bps=18,
        momentum_bps={5: 10, 15: 9, 30: 5, 60: 2},
        volatility_bps=5,
        up_book=book("up", bid=0.50, ask=0.52),
        down_book=book("down", bid=0.45, ask=0.47),
        microstructure=micro(),
    )
    signal = strategy.evaluate(f)
    assert signal.action == SignalAction.HOLD
    assert signal.reason == "liquidity_sweep_risk"
