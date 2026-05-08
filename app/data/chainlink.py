from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp

from app.config import Settings

log = logging.getLogger(__name__)


@dataclass(slots=True)
class ReferencePrice:
    price: float
    timestamp: datetime | None
    source: str


class ChainlinkReferenceClient:
    """Configurable Chainlink reference client.

    Chainlink Data Streams real-time reports require API access. The MVP supports a
    user-provided endpoint so we can plug in official Data Streams access without
    changing settlement logic. Expected response shapes are intentionally flexible:
    {"price": 123}, {"answer": 123}, {"value": 123}, or nested {"data": {...}}.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def price_at(self, timestamp: datetime) -> ReferencePrice | None:
        if (
            not self.settings.chainlink_reference_enabled
            or not self.settings.chainlink_reference_url
        ):
            return None
        headers = {"Accept": "application/json"}
        if self.settings.chainlink_api_key:
            headers["Authorization"] = f"Bearer {self.settings.chainlink_api_key}"
        params = {"symbol": "BTC/USD", "timestamp": int(timestamp.timestamp())}
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        try:
            async with (
                aiohttp.ClientSession(connector=connector, headers=headers) as session,
                session.get(
                    self.settings.chainlink_reference_url,
                    params=params,
                    timeout=self.settings.chainlink_timeout_seconds,
                ) as resp,
            ):
                resp.raise_for_status()
                payload = await resp.json()
        except Exception as exc:
            log.warning("Chainlink reference unavailable: %s", exc)
            return None

        price = _extract_price(payload)
        if price is None or price <= 0:
            log.warning("Chainlink reference response did not contain a usable price")
            return None
        return ReferencePrice(price=price, timestamp=timestamp, source="chainlink")


def _extract_price(payload: Any) -> float | None:
    if isinstance(payload, list) and payload:
        return _extract_price(payload[0])
    if not isinstance(payload, dict):
        return None
    for key in ("price", "answer", "value", "mid", "midPrice"):
        value = payload.get(key)
        if value not in (None, ""):
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    for key in ("data", "result", "report"):
        nested = payload.get(key)
        price = _extract_price(nested)
        if price is not None:
            return price
    return None
