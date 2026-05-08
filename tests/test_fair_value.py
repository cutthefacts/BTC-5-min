from datetime import UTC, datetime, timedelta

from app.config import Settings
from app.models import (
    FeatureSnapshot,
    Market,
    MicrostructureSnapshot,
    OrderBook,
    OrderBookSide,
    Outcome,
)
from app.strategy.fair_value import FairValueEngine


def _features(seconds_to_end: int) -> FeatureSnapshot:
    now = datetime.now(UTC)
    market = Market(
        condition_id="m1",
        question="BTC Up Down",
        slug="btc-updown",
        start_time=now - timedelta(seconds=300 - seconds_to_end),
        end_time=now + timedelta(seconds=seconds_to_end),
        price_to_beat=100_000,
        up_token_id="up",
        down_token_id="down",
    )
    book = OrderBook(
        token_id="up",
        market_id="m1",
        bids=[OrderBookSide(0.50, 100)],
        asks=[OrderBookSide(0.52, 100)],
    )
    micro = MicrostructureSnapshot(
        market_id="m1",
        token_id="up",
        bid_volume=500,
        ask_volume=100,
        weighted_bid_volume=500,
        weighted_ask_volume=100,
        imbalance_ratio=0.7,
        imbalance_acceleration=0.2,
        quote_age_ms=2000,
        repricing_lag_ms=1000,
        last_repricing_at=None,
        rapid_ask_disappearance=True,
        rapid_bid_disappearance=False,
        disappearing_liquidity=True,
        liquidity_sweep=True,
        aggressive_repricing=False,
        liquidity_vacuum=False,
    )
    return FeatureSnapshot(
        market=market,
        btc_price=100_150,
        distance_bps=15,
        momentum_bps={5: 5, 15: 4, 30: 3},
        volatility_bps=5,
        up_book=book,
        down_book=book,
        microstructure=micro,
        timestamp=now,
    )


def test_fair_value_is_time_aware() -> None:
    engine = FairValueEngine(Settings())
    early = engine.fair_value_up(_features(250), 0.52)
    late = engine.fair_value_up(_features(5), 0.52)
    assert late.fair_price > early.fair_price
    assert late.outcome == Outcome.UP
