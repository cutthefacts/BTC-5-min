from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from app.config import Settings
from app.models import MicrostructureSnapshot, OrderBook


@dataclass(slots=True)
class _BookState:
    best_bid: float | None = None
    best_ask: float | None = None
    bid_volume: float = 0.0
    ask_volume: float = 0.0
    imbalance_ratio: float = 0.0
    last_seen_at: datetime | None = None
    last_repricing_at: datetime | None = None


class OrderBookImbalanceEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._state: dict[str, _BookState] = {}
        self._latest: dict[str, MicrostructureSnapshot] = {}

    def update(self, book: OrderBook, now: datetime | None = None) -> MicrostructureSnapshot:
        now = now or datetime.now(UTC)
        state = self._state.get(book.token_id) or _BookState()
        previous_bid_volume = state.bid_volume
        previous_ask_volume = state.ask_volume
        previous_imbalance = state.imbalance_ratio

        bid_volume = sum(level.size for level in book.bids[: self.settings.imbalance_levels])
        ask_volume = sum(level.size for level in book.asks[: self.settings.imbalance_levels])
        bid_levels = book.bids[: self.settings.imbalance_levels]
        ask_levels = book.asks[: self.settings.imbalance_levels]
        weighted_bid = sum(
            level.size / (idx + 1) for idx, level in enumerate(bid_levels)
        )
        weighted_ask = sum(
            level.size / (idx + 1) for idx, level in enumerate(ask_levels)
        )
        denominator = weighted_bid + weighted_ask
        imbalance = 0.0 if denominator <= 0 else (weighted_bid - weighted_ask) / denominator

        repriced = book.best_bid != state.best_bid or book.best_ask != state.best_ask
        last_repricing_at = (
            now if repriced or state.last_repricing_at is None else state.last_repricing_at
        )
        quote_age_ms = self._elapsed_ms(last_repricing_at, now)
        repricing_lag_ms = 0.0 if repriced else quote_age_ms

        ask_drop = self._drop_ratio(previous_ask_volume, ask_volume)
        bid_drop = self._drop_ratio(previous_bid_volume, bid_volume)
        rapid_ask_disappearance = ask_drop >= self.settings.liquidity_drop_ratio
        rapid_bid_disappearance = bid_drop >= self.settings.liquidity_drop_ratio
        liquidity_sweep = max(ask_drop, bid_drop) >= self.settings.sweep_drop_ratio
        liquidity_vacuum = bid_volume + ask_volume < self.settings.min_liquidity_usd

        snapshot = MicrostructureSnapshot(
            market_id=book.market_id,
            token_id=book.token_id,
            bid_volume=bid_volume,
            ask_volume=ask_volume,
            weighted_bid_volume=weighted_bid,
            weighted_ask_volume=weighted_ask,
            imbalance_ratio=imbalance,
            imbalance_acceleration=imbalance - previous_imbalance,
            quote_age_ms=quote_age_ms,
            repricing_lag_ms=repricing_lag_ms,
            last_repricing_at=last_repricing_at,
            rapid_ask_disappearance=rapid_ask_disappearance,
            rapid_bid_disappearance=rapid_bid_disappearance,
            disappearing_liquidity=rapid_ask_disappearance or rapid_bid_disappearance,
            liquidity_sweep=liquidity_sweep,
            aggressive_repricing=repriced and abs(imbalance - previous_imbalance) > 0.35,
            liquidity_vacuum=liquidity_vacuum,
            timestamp=now,
        )
        self._state[book.token_id] = _BookState(
            best_bid=book.best_bid,
            best_ask=book.best_ask,
            bid_volume=bid_volume,
            ask_volume=ask_volume,
            imbalance_ratio=imbalance,
            last_seen_at=now,
            last_repricing_at=last_repricing_at,
        )
        self._latest[book.token_id] = snapshot
        return snapshot

    def latest(self, token_id: str) -> MicrostructureSnapshot | None:
        return self._latest.get(token_id)

    def directional_snapshot(
        self,
        up_token_id: str,
        down_token_id: str,
        prefer_up: bool,
    ) -> MicrostructureSnapshot | None:
        return self.latest(up_token_id if prefer_up else down_token_id)

    @staticmethod
    def _elapsed_ms(start: datetime, end: datetime) -> float:
        return max(0.0, (end - start).total_seconds() * 1_000)

    @staticmethod
    def _drop_ratio(previous: float, current: float) -> float:
        if previous <= 0:
            return 0.0
        return max(0.0, (previous - current) / previous)
