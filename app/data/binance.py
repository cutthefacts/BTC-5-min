from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import websockets

from app.models import BtcTick

log = logging.getLogger(__name__)


class BtcPriceStore:
    def __init__(self, max_age_seconds: int = 180) -> None:
        self._ticks: deque[BtcTick] = deque()
        self._max_age = timedelta(seconds=max_age_seconds)

    def add(self, tick: BtcTick) -> None:
        self._ticks.append(tick)
        cutoff = tick.timestamp - self._max_age
        while self._ticks and self._ticks[0].timestamp < cutoff:
            self._ticks.popleft()

    @property
    def latest(self) -> BtcTick | None:
        return self._ticks[-1] if self._ticks else None

    def momentum_bps(self, windows: tuple[int, ...] = (5, 15, 30, 60)) -> dict[int, float]:
        latest = self.latest
        if latest is None:
            return {window: 0.0 for window in windows}
        out: dict[int, float] = {}
        for window in windows:
            cutoff = latest.timestamp - timedelta(seconds=window)
            ref = next((tick for tick in self._ticks if tick.timestamp >= cutoff), None)
            if ref is None or ref.price <= 0:
                out[window] = 0.0
            else:
                out[window] = (latest.price - ref.price) / ref.price * 10_000
        return out

    def volatility_bps(self, window_seconds: int = 30) -> float:
        latest = self.latest
        if latest is None:
            return 0.0
        cutoff = latest.timestamp - timedelta(seconds=window_seconds)
        prices = [tick.price for tick in self._ticks if tick.timestamp >= cutoff]
        if len(prices) < 2:
            return 0.0
        mid = sum(prices) / len(prices)
        if mid <= 0:
            return 0.0
        return (max(prices) - min(prices)) / mid * 10_000


class BinanceBtcWebSocket:
    def __init__(self, url: str, fallback_url: str = "") -> None:
        self.url = url
        self.fallback_url = fallback_url

    def _parse_binance_trade(self, raw: str) -> BtcTick:
        payload = json.loads(raw)
        price = float(payload.get("p") or payload.get("price"))
        ts_ms = int(payload.get("T") or payload.get("E") or 0)
        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=UTC) if ts_ms else datetime.now(UTC)
        return BtcTick(price=price, timestamp=timestamp)

    def _parse_coinbase_ticker(self, raw: str) -> BtcTick | None:
        payload = json.loads(raw)
        if payload.get("type") != "ticker":
            return None
        price = payload.get("price")
        if price is None:
            return None
        timestamp_raw = payload.get("time")
        timestamp = (
            datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
            if timestamp_raw
            else datetime.now(UTC)
        )
        return BtcTick(price=float(price), timestamp=timestamp)

    async def _binance_stream(self) -> AsyncIterator[BtcTick]:
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
            async for raw in ws:
                yield self._parse_binance_trade(raw)

    async def _coinbase_stream(self) -> AsyncIterator[BtcTick]:
        async with websockets.connect(self.fallback_url, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "product_ids": ["BTC-USD"],
                        "channels": ["ticker"],
                    }
                )
            )
            async for raw in ws:
                tick = self._parse_coinbase_ticker(raw)
                if tick is not None:
                    yield tick

    async def stream(self) -> AsyncIterator[BtcTick]:
        backoff = 1.0
        source = "binance"
        while True:
            try:
                stream = self._coinbase_stream() if source == "coinbase" else self._binance_stream()
                backoff = 1.0
                async for tick in stream:
                    yield tick
            except Exception as exc:
                if source == "binance" and self.fallback_url and "451" in str(exc):
                    source = "coinbase"
                    backoff = 1.0
                    log.warning(
                        "Binance websocket blocked (%s); switching BTC feed to Coinbase",
                        exc,
                    )
                else:
                    log.warning("%s websocket disconnected: %s", source.capitalize(), exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
