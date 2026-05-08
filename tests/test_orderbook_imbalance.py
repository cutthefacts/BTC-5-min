from datetime import UTC, datetime, timedelta

from app.config import Settings
from app.models import OrderBook, OrderBookSide
from app.strategy.orderbook_imbalance import OrderBookImbalanceEngine


def test_orderbook_imbalance_detects_liquidity_disappearance() -> None:
    engine = OrderBookImbalanceEngine(Settings(min_liquidity_usd=10))
    now = datetime.now(UTC)
    first = OrderBook(
        token_id="up",
        market_id="m1",
        bids=[OrderBookSide(0.50, 100)],
        asks=[OrderBookSide(0.52, 100)],
        timestamp=now,
    )
    second = OrderBook(
        token_id="up",
        market_id="m1",
        bids=[OrderBookSide(0.50, 100)],
        asks=[OrderBookSide(0.52, 20)],
        timestamp=now + timedelta(seconds=1),
    )

    engine.update(first, now)
    snapshot = engine.update(second, now + timedelta(seconds=1))

    assert snapshot.rapid_ask_disappearance
    assert snapshot.liquidity_sweep
    assert snapshot.imbalance_ratio > 0
