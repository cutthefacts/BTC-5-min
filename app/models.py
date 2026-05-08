from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class Outcome(StrEnum):
    UP = "UP"
    DOWN = "DOWN"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TradingMode(StrEnum):
    PAPER = "paper"
    LIVE = "live"


class SignalAction(StrEnum):
    BUY_UP = "BUY_UP"
    BUY_DOWN = "BUY_DOWN"
    HOLD = "HOLD"
    EXIT = "EXIT"


@dataclass(slots=True)
class Market:
    condition_id: str
    question: str
    slug: str
    start_time: datetime
    end_time: datetime
    price_to_beat: float
    up_token_id: str
    down_token_id: str
    active: bool = True

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.end_time - self.start_time).total_seconds())

    def seconds_to_end(self, now: datetime | None = None) -> float:
        now = now or datetime.now(UTC)
        return max(0.0, (self.end_time - now).total_seconds())

    def seconds_from_start(self, now: datetime | None = None) -> float:
        now = now or datetime.now(UTC)
        return max(0.0, (now - self.start_time).total_seconds())


@dataclass(slots=True)
class OrderBookSide:
    price: float
    size: float


@dataclass(slots=True)
class OrderBook:
    token_id: str
    market_id: str
    bids: list[OrderBookSide] = field(default_factory=list)
    asks: list[OrderBookSide] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return max(0.0, self.best_ask - self.best_bid)

    @property
    def top_liquidity(self) -> float:
        bid = self.bids[0].price * self.bids[0].size if self.bids else 0.0
        ask = self.asks[0].price * self.asks[0].size if self.asks else 0.0
        return bid + ask


@dataclass(slots=True)
class BtcTick:
    price: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class FeatureSnapshot:
    market: Market
    btc_price: float
    distance_bps: float
    momentum_bps: dict[int, float]
    volatility_bps: float
    up_book: OrderBook
    down_book: OrderBook
    microstructure: MicrostructureSnapshot | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class Signal:
    market_id: str
    action: SignalAction
    outcome: Outcome | None
    expected_probability: float
    market_probability: float
    edge: float
    strength: float
    reason: str
    inefficiency_score: float = 0.0
    confidence: float = 0.0
    quote_age_ms: float = 0.0
    repricing_lag_ms: float = 0.0
    imbalance_ratio: float = 0.0
    liquidity_sweep: bool = False
    strategy_name: str = "baseline"
    btc_price: float | None = None
    price_to_beat: float | None = None
    distance_bps: float | None = None
    drift_15s_bps: float | None = None
    drift_30s_bps: float | None = None
    drift_60s_bps: float | None = None
    drift_180s_bps: float | None = None
    realized_vol_30s_bps: float | None = None
    realized_vol_60s_bps: float | None = None
    realized_vol_180s_bps: float | None = None
    seconds_to_close: float | None = None
    seconds_from_open: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    mid_price: float | None = None
    spread: float | None = None
    spread_bps: float | None = None
    top_bid_size: float | None = None
    top_ask_size: float | None = None
    bid_depth_3: float | None = None
    ask_depth_3: float | None = None
    bid_depth_5: float | None = None
    ask_depth_5: float | None = None
    imbalance_acceleration: float | None = None
    disappearing_liquidity: bool | None = None
    edge_quality_score: float | None = None
    extreme_edge_suspect: bool | None = None
    fair_value: float | None = None
    expected_edge: float | None = None
    realized_edge: float | None = None
    regime: str | None = None
    regime_confidence: float | None = None
    regime_source: str | None = None
    side_mode: str | None = None
    strategy_version: str | None = None
    feature_schema_version: int | None = None
    data_collection_started_at: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class Order:
    id: str
    market_id: str
    token_id: str
    outcome: Outcome
    side: Side
    price: float
    size: float
    status: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class Trade:
    order_id: str
    market_id: str
    token_id: str
    outcome: Outcome
    price: float
    size: float
    fee: float
    expected_edge: float | None = None
    signal_to_fill_delay_ms: float | None = None
    fill_latency_ms: float | None = None
    realized_edge: float | None = None
    post_fill_drift: float | None = None
    stale_fill: bool = False
    signal_timestamp: datetime | None = None
    order_submit_timestamp: datetime | None = None
    fill_timestamp: datetime | None = None
    signal_to_submit_ms: float | None = None
    submit_to_fill_ms: float | None = None
    total_fill_latency_ms: float | None = None
    expected_edge_at_signal: float | None = None
    expected_edge_at_submit: float | None = None
    realized_edge_after_fill: float | None = None
    stale_reason: str | None = None
    strategy_name: str = "baseline"
    strategy_version: str | None = None
    feature_schema_version: int | None = None
    data_collection_started_at: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class Position:
    market_id: str
    token_id: str
    outcome: Outcome
    size: float = 0.0
    avg_entry: float = 0.0
    fees: float = 0.0

    @property
    def cost_basis(self) -> float:
        return self.size * self.avg_entry + self.fees


@dataclass(slots=True)
class PortfolioStats:
    balance: float
    realized_pnl: float
    unrealized_pnl: float
    winrate: float
    trades: int
    consecutive_losses: int
    max_drawdown_pct: float


@dataclass(slots=True)
class MicrostructureSnapshot:
    market_id: str
    token_id: str
    bid_volume: float
    ask_volume: float
    weighted_bid_volume: float
    weighted_ask_volume: float
    imbalance_ratio: float
    imbalance_acceleration: float
    quote_age_ms: float
    repricing_lag_ms: float
    last_repricing_at: datetime | None
    rapid_ask_disappearance: bool
    rapid_bid_disappearance: bool
    disappearing_liquidity: bool
    liquidity_sweep: bool
    aggressive_repricing: bool
    liquidity_vacuum: bool
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
