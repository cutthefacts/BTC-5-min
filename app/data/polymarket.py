from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp
import websockets

from app.models import Market, OrderBook, OrderBookSide

log = logging.getLogger(__name__)


BTC_5M_PATTERNS = (
    re.compile(r"\bbitcoin\b.*\bup\s+or\s+down\b", re.I),
    re.compile(r"\bbtc-updown-5m-\d+\b", re.I),
    re.compile(r"\bbtc\b.*\b(up|down)\b.*\b5\s*(minute|min|m)\b", re.I),
)
PRICE_TO_BEAT_RE = re.compile(r"\$?\s*([0-9][0-9,]*(?:\.\d+)?)")


def _parse_dt(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(UTC)
    normalized = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _extract_price_to_beat(payload: dict[str, Any]) -> float:
    for key in ("price_to_beat", "priceToBeat", "line"):
        value = payload.get(key)
        if value not in (None, ""):
            try:
                parsed = float(str(value).replace(",", ""))
                return parsed if parsed > 1_000 else 0.0
            except ValueError:
                pass
    text = " ".join(
        str(payload.get(key, "")) for key in ("question", "description", "groupItemTitle")
    )
    matches = PRICE_TO_BEAT_RE.findall(text)
    if matches:
        parsed = float(matches[-1].replace(",", ""))
        return parsed if parsed > 1_000 else 0.0
    return 0.0


def _window_from_slug(slug: str) -> tuple[datetime, datetime] | None:
    match = re.search(r"btc-updown-5m-(\d+)", slug)
    if not match:
        return None
    start_epoch = int(match.group(1))
    start = datetime.fromtimestamp(start_epoch, tz=UTC)
    return start, start + timedelta(minutes=5)


def _tokens(payload: dict[str, Any]) -> tuple[str, str] | None:
    raw_tokens = payload.get("tokens")
    if isinstance(raw_tokens, list):
        up = down = ""
        for token in raw_tokens:
            outcome = str(token.get("outcome", "")).upper()
            token_id = str(token.get("token_id") or token.get("tokenId") or "")
            if outcome in {"UP", "YES"} and not up:
                up = token_id
            elif outcome in {"DOWN", "NO"} and not down:
                down = token_id
        if up and down:
            return up, down

    clob_ids = payload.get("clobTokenIds") or payload.get("clob_token_ids")
    if isinstance(clob_ids, str):
        try:
            clob_ids = json.loads(clob_ids)
        except json.JSONDecodeError:
            clob_ids = []
    if isinstance(clob_ids, list) and len(clob_ids) >= 2:
        return str(clob_ids[0]), str(clob_ids[1])
    return None


class PolymarketClient:
    def __init__(self, gamma_url: str, clob_url: str, session: aiohttp.ClientSession | None = None):
        self.gamma_url = gamma_url.rstrip("/")
        self.clob_url = clob_url.rstrip("/")
        self.session = session
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "polymarket-btc-5m-edge/0.1",
        }

    async def _get_json(self, url: str, params: dict[str, Any]) -> Any:
        if self.session is not None:
            async with self.session.get(
                url,
                params=params,
                timeout=15,
                headers=self.headers,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with (
            aiohttp.ClientSession(connector=connector) as session,
            session.get(url, params=params, timeout=15, headers=self.headers) as resp,
        ):
            resp.raise_for_status()
            return await resp.json()

    async def discover_btc_5m_markets(self, limit: int = 100) -> list[Market]:
        markets = await self.discover_btc_5m_markets_by_slug()
        if markets:
            return markets

        params = {
            "active": "true",
            "closed": "false",
            "limit": str(limit),
            "order": "endDate",
            "ascending": "true",
        }
        data = await self._get_json(f"{self.gamma_url}/markets", params)
        rows = data if isinstance(data, list) else data.get("data", [])
        return self._parse_market_rows(rows)

    async def discover_btc_5m_markets_by_slug(self) -> list[Market]:
        now = datetime.now(UTC)
        current_epoch = int(now.timestamp() // 300 * 300)
        epochs = [current_epoch - 300, current_epoch, current_epoch + 300]
        markets: list[Market] = []
        for epoch in epochs:
            try:
                data = await self._get_json(
                    f"{self.gamma_url}/events/slug/btc-updown-5m-{epoch}",
                    {},
                )
            except aiohttp.ClientResponseError as exc:
                if exc.status == 404:
                    continue
                raise
            rows = data.get("markets", []) if isinstance(data, dict) else []
            markets.extend(self._parse_market_rows(rows))
        return markets

    def _parse_market_rows(self, rows: list[dict[str, Any]]) -> list[Market]:
        markets: list[Market] = []
        now = datetime.now(UTC)
        for row in rows:
            question = str(row.get("question") or row.get("title") or "")
            slug = str(row.get("slug") or row.get("market_slug") or "")
            haystack = f"{question} {slug} {row.get('description', '')}"
            if not any(pattern.search(haystack) for pattern in BTC_5M_PATTERNS):
                continue
            token_pair = _tokens(row)
            if token_pair is None:
                continue
            window = _window_from_slug(slug)
            if window:
                start_time, end_time = window
            else:
                end_time = _parse_dt(row.get("endDate") or row.get("end_date_iso"))
                start_time = _parse_dt(row.get("startDate") or row.get("game_start_time"))
                if start_time >= end_time:
                    start_time = end_time - timedelta(minutes=5)
            if end_time <= now:
                continue
            markets.append(
                Market(
                    condition_id=str(
                        row.get("conditionId") or row.get("condition_id") or row["id"]
                    ),
                    question=question,
                    slug=slug,
                    start_time=start_time,
                    end_time=end_time,
                    price_to_beat=_extract_price_to_beat(row),
                    up_token_id=token_pair[0],
                    down_token_id=token_pair[1],
                    active=bool(row.get("active", True)),
                )
            )
        return markets

    async def get_order_book(self, token_id: str) -> OrderBook:
        data = await self._get_json(f"{self.clob_url}/book", {"token_id": token_id})
        return parse_orderbook(data)


def parse_orderbook(payload: dict[str, Any]) -> OrderBook:
    def side(rows: list[dict[str, Any]], reverse: bool) -> list[OrderBookSide]:
        parsed = [
            OrderBookSide(price=float(row["price"]), size=float(row["size"]))
            for row in rows
            if float(row.get("size", 0)) > 0
        ]
        return sorted(parsed, key=lambda item: item.price, reverse=reverse)

    ts_raw = payload.get("timestamp")
    timestamp = datetime.now(UTC)
    if ts_raw:
        timestamp = datetime.fromtimestamp(int(ts_raw) / 1000, tz=UTC)
    return OrderBook(
        token_id=str(payload.get("asset_id") or payload.get("asset") or ""),
        market_id=str(payload.get("market") or ""),
        bids=side(payload.get("bids", []), reverse=True),
        asks=side(payload.get("asks", []), reverse=False),
        timestamp=timestamp,
    )


class PolymarketMarketWebSocket:
    def __init__(self, url: str) -> None:
        self.url = url

    async def stream(self, asset_ids: list[str]) -> AsyncIterator[dict[str, Any]]:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
                    backoff = 1.0
                    async for raw in ws:
                        if raw == "{}":
                            continue
                        payload = json.loads(raw)
                        if isinstance(payload, list):
                            for item in payload:
                                yield item
                        else:
                            yield payload
            except Exception as exc:
                log.warning("Polymarket websocket disconnected: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class OrderBookCache:
    def __init__(self) -> None:
        self.books: dict[str, OrderBook] = {}

    def apply_ws_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        if event_type == "book":
            book = parse_orderbook(event)
            self.books[book.token_id] = book
        elif event_type in {"price_change", "best_bid_ask"}:
            self._apply_quote_update(event)

    def _apply_quote_update(self, event: dict[str, Any]) -> None:
        changes = event.get("price_changes") or [event]
        for change in changes:
            token_id = str(change.get("asset_id") or "")
            if not token_id:
                continue
            book = self.books.get(token_id) or OrderBook(
                token_id=token_id,
                market_id=str(event.get("market", "")),
            )
            if "best_bid" in change and change["best_bid"] is not None:
                bid_size = book.bids[0].size if book.bids else 0.0
                book.bids = [OrderBookSide(price=float(change["best_bid"]), size=bid_size)]
            if "best_ask" in change and change["best_ask"] is not None:
                ask_size = book.asks[0].size if book.asks else 0.0
                book.asks = [OrderBookSide(price=float(change["best_ask"]), size=ask_size)]
            book.timestamp = datetime.now(UTC)
            self.books[token_id] = book
