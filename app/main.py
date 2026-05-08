from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from app.backtest.research import candidate_settings_overrides
from app.config import get_settings
from app.data.binance import BinanceBtcWebSocket, BtcPriceStore
from app.data.chainlink import ChainlinkReferenceClient
from app.data.polymarket import OrderBookCache, PolymarketClient, PolymarketMarketWebSocket
from app.execution.engine import PaperExecutionEngine
from app.models import FeatureSnapshot, Market, OrderBook, SignalAction
from app.portfolio.manager import Portfolio
from app.risk.manager import RiskManager
from app.storage.sqlite import SQLiteStore
from app.strategy.orderbook_imbalance import OrderBookImbalanceEngine
from app.strategy.reactive import ReactiveDirectionalStrategy
from app.telegram.bot import TelegramController

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)


class TradingService:
    def __init__(self, strategy_name: str = "baseline", paper_shadow: bool = False) -> None:
        base_settings = get_settings()
        self.settings = (
            base_settings.model_copy(update=candidate_settings_overrides())
            if strategy_name == "candidate_v1" and not paper_shadow
            else base_settings
        )
        self.store = SQLiteStore(self.settings.database_url)
        self.store.init()
        self.polymarket = PolymarketClient(
            self.settings.polymarket_gamma_url,
            self.settings.polymarket_clob_url,
        )
        self.chainlink = ChainlinkReferenceClient(self.settings)
        self.btc_store = BtcPriceStore()
        self.book_cache = OrderBookCache()
        self.microstructure = OrderBookImbalanceEngine(self.settings)
        self.strategy = ReactiveDirectionalStrategy(self.settings)
        self.portfolio = Portfolio(self.settings.paper_starting_balance)
        self.risk = RiskManager(self.settings)
        self.execution = PaperExecutionEngine(self.settings, self.portfolio)
        self.strategy_name = strategy_name
        self.paper_shadow = paper_shadow
        self.shadow_strategy = None
        self.shadow_execution = None
        self.shadow_portfolio = None
        self.shadow_risk = None
        if paper_shadow:
            candidate_settings = base_settings.model_copy(update=candidate_settings_overrides())
            self.shadow_strategy = ReactiveDirectionalStrategy(candidate_settings)
            self.shadow_portfolio = Portfolio(candidate_settings.paper_starting_balance)
            self.shadow_risk = RiskManager(candidate_settings)
            self.shadow_execution = PaperExecutionEngine(candidate_settings, self.shadow_portfolio)
        self.telegram = TelegramController(self.settings, self.portfolio, self.risk, self.store)
        self.markets: dict[str, Market] = {}

    async def run(self) -> None:
        try:
            await self.refresh_markets()
        except Exception:
            log.exception("initial market refresh failed; service will retry in background")
        tasks = [
            asyncio.create_task(self._supervise("btc", self._btc_loop)),
            asyncio.create_task(self._supervise("market_refresh", self._market_refresh_loop)),
            asyncio.create_task(self._supervise("polymarket_ws", self._polymarket_ws_loop)),
            asyncio.create_task(self._supervise("signals", self._signal_loop)),
            asyncio.create_task(self._supervise("settlement", self._settlement_loop)),
        ]
        if self.settings.telegram_polling_enabled:
            tasks.append(asyncio.create_task(self._supervise("telegram", self.telegram.run)))
        else:
            log.info("Telegram polling disabled by TELEGRAM_POLLING_ENABLED=false")
        await asyncio.gather(*tasks)

    async def _supervise(self, name: str, factory) -> None:
        backoff = 5.0
        while True:
            try:
                await factory()
                log.warning("%s task exited; restarting", name)
                backoff = 5.0
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s task crashed; restarting in %.1fs", name, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120.0)

    async def refresh_markets(self) -> None:
        markets = await self.polymarket.discover_btc_5m_markets()
        for market in markets:
            existing = self.markets.get(market.condition_id)
            if existing is not None and market.price_to_beat <= 0 < existing.price_to_beat:
                market.price_to_beat = existing.price_to_beat
            self.markets[market.condition_id] = market
            self.store.save_market(market)
        log.info("discovered %s active BTC 5m markets", len(markets))

    async def _market_refresh_loop(self) -> None:
        while True:
            try:
                await self.refresh_markets()
            except TimeoutError:
                log.warning("market refresh timed out")
                self._record_data_quality("polymarket_gamma", "timeout", "warning")
            except Exception:
                log.exception("market refresh failed")
                self._record_data_quality("polymarket_gamma", "refresh_failed", "error")
            await asyncio.sleep(self.settings.market_refresh_seconds)

    async def _btc_loop(self) -> None:
        async for tick in BinanceBtcWebSocket(
            self.settings.binance_ws_url,
            self.settings.btc_price_fallback_ws_url,
        ).stream():
            self.btc_store.add(tick)
            self.store.save_btc_tick(tick.timestamp.isoformat(), tick.price)

    async def _polymarket_ws_loop(self) -> None:
        while True:
            asset_ids = []
            for market in self.markets.values():
                asset_ids.extend([market.up_token_id, market.down_token_id])
            asset_ids = sorted(set(asset_ids))
            if not asset_ids:
                await asyncio.sleep(5)
                continue

            ws = PolymarketMarketWebSocket(self.settings.polymarket_ws_url)
            try:
                async with asyncio.timeout(60):
                    async for event in ws.stream(asset_ids):
                        self.book_cache.apply_ws_event(event)
                        self._persist_book_event(event)
                        self._update_microstructure(event)
            except TimeoutError:
                log.info("refreshing Polymarket websocket subscription")
                self._record_data_quality("polymarket_ws", "subscription_refresh", "info")
            except Exception:
                log.exception("Polymarket websocket loop failed")
                self._record_data_quality("polymarket_ws", "loop_failed", "error")
                await asyncio.sleep(5)

    async def _signal_loop(self) -> None:
        while True:
            try:
                await self._evaluate_once()
            except Exception:
                log.exception("signal evaluation failed")
            await asyncio.sleep(self.settings.service_tick_seconds)

    async def _settlement_loop(self) -> None:
        while True:
            try:
                await self._settle_finished_markets()
            except Exception:
                log.exception("settlement failed")
            await asyncio.sleep(self.settings.settlement_check_seconds)

    async def _settle_finished_markets(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(seconds=self.settings.settlement_delay_seconds)
        candidates = self.store.settlement_candidates(cutoff.isoformat())
        for market in candidates:
            end_time = datetime.fromisoformat(market["end_time"])
            reference = await self.chainlink.price_at(end_time)
            if reference is None:
                final_tick = self.store.final_btc_tick(
                    market["end_time"],
                    self.settings.settlement_max_tick_lag_seconds,
                )
                if final_tick is None:
                    continue
                final_price = float(final_tick["price"])
                settlement_source = "binance"
            else:
                final_price = reference.price
                settlement_source = reference.source
            if final_price <= 0:
                continue
            result = self.store.settle_market(
                market,
                final_price=final_price,
                settled_at=datetime.now(UTC).isoformat(),
                settlement_source=settlement_source,
            )
            log.info(
                "settled market=%s source=%s winner=%s final=%.2f ptb=%.2f pnl=%.2f trades=%s",
                market["slug"],
                result["settlement_source"],
                result["winning_outcome"],
                result["final_price"],
                result["price_to_beat"],
                result["pnl"],
                result["trade_count"],
            )

    async def _evaluate_once(self) -> None:
        latest = self.btc_store.latest
        if latest is None:
            return
        now = datetime.now(UTC)
        for market in list(self.markets.values()):
            if market.end_time <= now:
                continue
            up_book = self.book_cache.books.get(market.up_token_id)
            down_book = self.book_cache.books.get(market.down_token_id)
            if up_book is None or down_book is None:
                continue
            if market.price_to_beat <= 0:
                seconds_from_start = (latest.timestamp - market.start_time).total_seconds()
                if 0 <= seconds_from_start <= 10:
                    market.price_to_beat = latest.price
                    self.store.save_market(market)
                    log.info(
                        "captured price_to_beat market=%s price=%.2f",
                        market.slug,
                        market.price_to_beat,
                    )
                else:
                    continue
            distance_bps = (latest.price - market.price_to_beat) / market.price_to_beat * 10_000
            prefer_up = distance_bps >= 0
            features = FeatureSnapshot(
                market=market,
                btc_price=latest.price,
                distance_bps=distance_bps,
                momentum_bps=self.btc_store.momentum_bps(),
                volatility_bps=self.btc_store.volatility_bps(),
                up_book=up_book,
                down_book=down_book,
                microstructure=self.microstructure.directional_snapshot(
                    market.up_token_id,
                    market.down_token_id,
                    prefer_up=prefer_up,
                ),
            )
            signal = self.strategy.evaluate(features)
            signal.strategy_name = self.strategy_name
            self.store.save_signal(signal)
            decision = self.risk.evaluate(signal, self.portfolio)
            if decision.allowed:
                trade = await self.execution.execute(market, signal, decision.size_usd)
                if trade:
                    self.store.save_trade(trade)
                    log.info(
                        "paper fill market=%s outcome=%s size=%.2f price=%.3f",
                        trade.market_id,
                        trade.outcome,
                        trade.size,
                        trade.price,
                    )
            elif signal.action != SignalAction.HOLD:
                self.store.save_signal(
                    replace(
                        signal,
                        action=SignalAction.HOLD,
                        reason=f"risk_manager_blocked:{decision.reason}",
                        strategy_name=self.strategy_name,
                    )
                )
            if self.shadow_strategy and self.shadow_execution and self.shadow_risk:
                shadow_signal = self.shadow_strategy.evaluate(features)
                shadow_signal.strategy_name = "candidate_v1"
                self.store.save_signal(shadow_signal)
                shadow_decision = self.shadow_risk.evaluate(shadow_signal, self.shadow_portfolio)
                if shadow_decision.allowed:
                    shadow_trade = await self.shadow_execution.execute(
                        market,
                        shadow_signal,
                        shadow_decision.size_usd,
                    )
                    if shadow_trade:
                        shadow_trade.strategy_name = "candidate_v1"
                        self.store.save_trade(shadow_trade)

    def _persist_book_event(self, event: dict) -> None:
        token_id = str(event.get("asset_id") or "")
        if not token_id and event.get("price_changes"):
            token_id = str(event["price_changes"][0].get("asset_id") or "")
        book: OrderBook | None = self.book_cache.books.get(token_id)
        if book is None:
            return
        try:
            self.store.save_orderbook_snapshot(
                {
                    "timestamp": book.timestamp.isoformat(),
                    "market_id": book.market_id,
                    "token_id": book.token_id,
                    "best_bid": book.best_bid,
                    "best_ask": book.best_ask,
                    "spread": book.spread,
                    "liquidity": book.top_liquidity,
                }
            )
        except Exception:
            log.exception("failed to persist orderbook snapshot")

    def _update_microstructure(self, event: dict) -> None:
        token_ids = []
        token_id = str(event.get("asset_id") or "")
        if token_id:
            token_ids.append(token_id)
        for change in event.get("price_changes") or []:
            change_token_id = str(change.get("asset_id") or "")
            if change_token_id:
                token_ids.append(change_token_id)
        for token_id in sorted(set(token_ids)):
            book = self.book_cache.books.get(token_id)
            if book is None:
                continue
            snapshot = self.microstructure.update(book)
            self.store.save_microstructure_event(snapshot)

    def _record_data_quality(self, source: str, event_type: str, severity: str) -> None:
        try:
            self.store.save_data_quality_event(
                timestamp=datetime.now(UTC).isoformat(),
                source=source,
                event_type=event_type,
                severity=severity,
                details="",
            )
        except Exception:
            log.exception("failed to persist data quality event")


async def amain() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default="baseline", choices=["baseline", "candidate_v1"])
    parser.add_argument("--paper-shadow", action="store_true")
    args = parser.parse_args()
    await TradingService(args.strategy, args.paper_shadow).run()


if __name__ == "__main__":
    asyncio.run(amain())
