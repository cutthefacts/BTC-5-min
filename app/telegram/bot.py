from __future__ import annotations

import asyncio
import logging

import aiohttp
from aiogram import Bot, Dispatcher, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramConflictError, TelegramNetworkError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.config import Settings
from app.models import TradingMode
from app.portfolio.manager import Portfolio
from app.risk.manager import RiskManager
from app.storage.sqlite import SQLiteStore
from app.strategy.regime import MarketRegimeEngine

log = logging.getLogger(__name__)


class TelegramController:
    def __init__(
        self,
        settings: Settings,
        portfolio: Portfolio,
        risk: RiskManager,
        store: SQLiteStore,
    ) -> None:
        self.settings = settings
        self.portfolio = portfolio
        self.risk = risk
        self.store = store
        self.mode = TradingMode.PAPER
        self.router = Router()
        self._register()

    def _authorized(self, message: Message) -> bool:
        admins = self.settings.admin_id_set()
        return not admins or (message.from_user is not None and message.from_user.id in admins)

    def _register(self) -> None:
        @self.router.message(Command("start"))
        async def start(message: Message) -> None:
            await message.answer(
                "Polymarket BTC 5m edge system online. Default mode: paper.",
                reply_markup=self._keyboard(),
            )

        @self.router.message(Command("status"))
        async def status(message: Message) -> None:
            await message.answer(self._status_text(), reply_markup=self._keyboard())

        @self.router.message(Command("balance"))
        async def balance(message: Message) -> None:
            await message.answer(self._balance_text(), reply_markup=self._keyboard())

        @self.router.message(Command("positions"))
        async def positions(message: Message) -> None:
            await message.answer(self._positions_text(), reply_markup=self._keyboard())

        @self.router.message(Command("stats"))
        async def stats_cmd(message: Message) -> None:
            await message.answer(self._stats_text(), reply_markup=self._keyboard())

        @self.router.message(Command("risk"))
        async def risk_cmd(message: Message) -> None:
            await message.answer(self._risk_text(), reply_markup=self._keyboard())

        @self.router.message(Command("candidate"))
        async def candidate_cmd(message: Message) -> None:
            await message.answer(self._candidate_text(), reply_markup=self._keyboard())

        @self.router.message(Command("regime"))
        async def regime_cmd(message: Message) -> None:
            await message.answer(self._regime_text(), reply_markup=self._keyboard())

        @self.router.message(Command("research_gate"))
        async def gate_cmd(message: Message) -> None:
            await message.answer(self._research_gate_text(), reply_markup=self._keyboard())

        @self.router.message(Command("walkforward"))
        async def walkforward_cmd(message: Message) -> None:
            await message.answer(self._walkforward_text(), reply_markup=self._keyboard())

        @self.router.message(Command("shadow_stats"))
        async def shadow_cmd(message: Message) -> None:
            await message.answer(self._shadow_text(), reply_markup=self._keyboard())

        @self.router.message(Command("regime_gate"))
        async def regime_gate_cmd(message: Message) -> None:
            await message.answer(self._regime_gate_text(), reply_markup=self._keyboard())

        @self.router.message(Command("forward_validation"))
        async def forward_validation_cmd(message: Message) -> None:
            await message.answer(self._forward_validation_text(), reply_markup=self._keyboard())

        @self.router.message(Command("hourly"))
        async def hourly_cmd(message: Message) -> None:
            await message.answer(self._hourly_text(), reply_markup=self._keyboard())

        @self.router.message(Command("edge_quality"))
        async def edge_quality_cmd(message: Message) -> None:
            await message.answer(self._edge_quality_text(), reply_markup=self._keyboard())

        @self.router.message(Command("export_stats"))
        async def export_stats_cmd(message: Message) -> None:
            for chunk in self._split_message(self._export_stats_text()):
                await message.answer(chunk)

        @self.router.message(Command("db_status"))
        async def db_status_cmd(message: Message) -> None:
            await message.answer(self._db_status_text())

        @self.router.message(Command("signal_stats"))
        async def signal_stats_cmd(message: Message) -> None:
            await message.answer(self._signal_stats_text())

        @self.router.message(Command("pause"))
        async def pause(message: Message) -> None:
            if not self._authorized(message):
                return
            self.risk.pause()
            await message.answer("Paused.", reply_markup=self._keyboard())

        @self.router.message(Command("resume"))
        async def resume(message: Message) -> None:
            if not self._authorized(message):
                return
            self.risk.resume()
            await message.answer("Resumed.", reply_markup=self._keyboard())

        @self.router.message(Command("kill"))
        async def kill(message: Message) -> None:
            if not self._authorized(message):
                return
            self.risk.kill()
            await message.answer("Kill-switch enabled.", reply_markup=self._keyboard())

        @self.router.message(Command("mode"))
        async def mode(message: Message) -> None:
            if not self._authorized(message):
                return
            parts = (message.text or "").split()
            if len(parts) < 2 or parts[1] not in {"paper", "live"}:
                await message.answer("Usage: /mode paper|live", reply_markup=self._keyboard())
                return
            if parts[1] == "live":
                metrics = self.store.paper_gate_metrics()
                if metrics["completed_trades"] < self.settings.min_live_completed_trades:
                    await message.answer(
                        "Live denied: not enough completed paper trades.",
                        reply_markup=self._keyboard(),
                    )
                    return
                if metrics["net_pnl"] <= 0:
                    await message.answer(
                        "Live denied: paper PnL is not positive after fees.",
                        reply_markup=self._keyboard(),
                    )
                    return
                if self.settings.live_confirmation_phrase not in (message.text or ""):
                    await message.answer(
                        "Live denied: missing confirmation phrase.",
                        reply_markup=self._keyboard(),
                    )
                    return
                self.mode = TradingMode.LIVE
                await message.answer("Live mode enabled.", reply_markup=self._keyboard())
            else:
                self.mode = TradingMode.PAPER
                await message.answer("Paper mode enabled.", reply_markup=self._keyboard())

        @self.router.callback_query()
        async def callback(query: CallbackQuery) -> None:
            if not query.data:
                await query.answer()
                return
            text = await self._handle_callback(query)
            await query.message.answer(text, reply_markup=self._keyboard())
            await query.answer()

    async def _handle_callback(self, query: CallbackQuery) -> str:
        data = query.data or ""
        if data == "status":
            return self._status_text()
        if data == "balance":
            return self._balance_text()
        if data == "positions":
            return self._positions_text()
        if data == "stats":
            return self._stats_text()
        if data == "risk":
            return self._risk_text()
        if data == "candidate":
            return self._candidate_text()
        if data == "shadow_stats":
            return self._shadow_text()
        if data == "export_stats":
            return self._export_stats_text()
        if data == "db_status":
            return self._db_status_text()
        if data == "signal_stats":
            return self._signal_stats_text()
        if data in {"pause", "resume", "kill", "mode_paper", "mode_live"} and (
            not self._authorized_callback(query)
        ):
            return "Not authorized."
        if data == "pause":
            self.risk.pause()
            return "Paused."
        if data == "resume":
            self.risk.resume()
            return "Resumed."
        if data == "kill":
            self.risk.kill()
            return "Kill-switch enabled."
        if data == "mode_paper":
            self.mode = TradingMode.PAPER
            return "Paper mode enabled."
        if data == "mode_live":
            metrics = self.store.paper_gate_metrics()
            if metrics["completed_trades"] < self.settings.min_live_completed_trades:
                return "Live denied: not enough completed paper trades."
            if metrics["net_pnl"] <= 0:
                return "Live denied: paper PnL is not positive after fees."
            return (
                "Live requires explicit text confirmation:\n"
                f"/mode live {self.settings.live_confirmation_phrase}"
            )
        return "Unknown action."

    def _authorized_callback(self, query: CallbackQuery) -> bool:
        admins = self.settings.admin_id_set()
        return not admins or (query.from_user is not None and query.from_user.id in admins)

    def _keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Status", callback_data="status"),
                    InlineKeyboardButton(text="Stats", callback_data="stats"),
                ],
                [
                    InlineKeyboardButton(text="Balance", callback_data="balance"),
                    InlineKeyboardButton(text="Positions", callback_data="positions"),
                ],
                [
                    InlineKeyboardButton(text="Risk", callback_data="risk"),
                    InlineKeyboardButton(text="Candidate", callback_data="candidate"),
                    InlineKeyboardButton(text="Shadow", callback_data="shadow_stats"),
                ],
                [
                    InlineKeyboardButton(text="Export stats", callback_data="export_stats"),
                    InlineKeyboardButton(text="DB status", callback_data="db_status"),
                ],
                [InlineKeyboardButton(text="Signal stats", callback_data="signal_stats")],
                [
                    InlineKeyboardButton(text="Pause", callback_data="pause"),
                    InlineKeyboardButton(text="Resume", callback_data="resume"),
                ],
                [
                    InlineKeyboardButton(text="Paper", callback_data="mode_paper"),
                    InlineKeyboardButton(text="Live", callback_data="mode_live"),
                    InlineKeyboardButton(text="Kill", callback_data="kill"),
                ],
            ]
        )

    def _status_text(self) -> str:
        stats = self.portfolio.stats()
        persisted = self.store.trade_summary()
        return (
            f"mode={self.mode.value} paused={self.risk.paused} kill={self.risk.kill_switch} "
            f"session_trades={stats.trades} db_trades={persisted['trades']:.0f} "
            f"session_pnl={stats.realized_pnl:.2f}"
        )

    def _balance_text(self) -> str:
        return f"cash={self.portfolio.cash:.2f} equity={self.portfolio.equity():.2f}"

    def _positions_text(self) -> str:
        if not self.portfolio.positions:
            return "No open positions."
        lines = [
            f"{p.market_id[:10]} {p.outcome.value} size={p.size:.2f} avg={p.avg_entry:.3f}"
            for p in self.portfolio.positions.values()
        ]
        return "\n".join(lines)

    def _stats_text(self) -> str:
        stats = self.portfolio.stats()
        persisted = self.store.trade_summary()
        settled = self.store.result_summary()
        profit_factor = settled["profit_factor"]
        pf_text = f"{profit_factor:.2f}" if profit_factor is not None else "n/a"
        return (
            f"realized={stats.realized_pnl:.2f} unrealized={stats.unrealized_pnl:.2f} "
            f"winrate={stats.winrate:.2%} drawdown={stats.max_drawdown_pct:.2%} "
            f"db_trades={persisted['trades']:.0f} latest={persisted['latest_trade_at']}\n"
            f"settled_markets={settled['settled_markets']} "
            f"settled_trades={settled['settled_trades']} "
            f"settled_pnl={settled['pnl']:.2f} "
            f"settled_winrate={settled['winrate']:.2%} "
            f"profit_factor={pf_text} "
            f"max_dd={settled['max_drawdown']:.2f} "
            f"chainlink={settled['chainlink_markets']} binance={settled['binance_markets']}"
        )

    def _risk_text(self) -> str:
        return (
            f"max_trade={self.settings.max_trade_balance_pct:.2%} "
            f"daily_loss={self.settings.daily_loss_limit_pct:.2%} "
            f"max_losses={self.settings.max_consecutive_losses}"
        )

    def _candidate_text(self) -> str:
        summary = self.store.strategy_trade_summary("candidate_v1")
        return (
            "candidate_v1 "
            f"trades={summary['trades']:.0f} "
            f"avg_edge={summary['avg_edge']:.4f} "
            f"stale={summary['stale_fill_rate']:.2%} "
            f"latest={summary['latest_trade_at']}"
        )

    def _shadow_text(self) -> str:
        lines = [self._candidate_text(), "last trades:"]
        for row in self.store.last_trades("candidate_v1", 10):
            lines.append(
                f"{row['timestamp']} {row['outcome']} price={row['price']:.3f} "
                f"size={row['size']:.2f} stale={bool(row['stale_fill'])}"
            )
        return "\n".join(lines)

    def _export_stats_text(self) -> str:
        strategy_name = "candidate_v1"
        trades = self.store.strategy_trade_summary(strategy_name)
        settled = self.store.strategy_settled_summary(strategy_name)
        pf = settled["profit_factor"]
        pf_text = f"{pf:.2f}" if pf is not None else "n/a"
        lines = [
            "candidate_v1 export",
            f"trades={trades['trades']:.0f} latest={trades['latest_trade_at']}",
            f"notional_spent={trades['notional_spent']:.2f}",
            f"avg_edge={trades['avg_edge']:.4f} stale={trades['stale_fill_rate']:.2%}",
            (
                f"settled_markets={settled['settled_markets']} "
                f"settled_trades={settled['settled_trades']} "
                f"pnl={settled['pnl']:.2f} pf={pf_text} "
                f"winrate={settled['winrate']:.2%} dd={settled['max_drawdown']:.2f}"
            ),
            "",
            "side pnl:",
        ]
        side_rows = self.store.strategy_side_pnl(strategy_name)
        if side_rows:
            for row in side_rows:
                lines.append(
                    f"{row['side']}: trades={row['trades']} pnl={row['pnl']:.2f} "
                    f"avg_edge={row['avg_edge'] or 0:.4f} "
                    f"stale={row['stale_fill_rate'] or 0:.2%}"
                )
        else:
            lines.append("no settled side pnl yet")

        lines.extend(["", "regime signals:"])
        for row in self.store.strategy_regime_signal_counts(strategy_name):
            confidence = row["avg_confidence"]
            avg_edge = row["avg_edge"]
            lines.append(
                f"{row['regime']}:{row['regime_source']} signals={row['signals']} "
                f"conf={(confidence or 0):.2f} edge={(avg_edge or 0):.4f}"
            )

        lines.extend(["", "last trades:"])
        for row in self.store.last_trades(strategy_name, 10):
            lines.append(
                f"{row['timestamp']} {row['outcome']} price={row['price']:.3f} "
                f"size={row['size']:.2f} fee={row['fee']:.4f} "
                f"stale={bool(row['stale_fill'])}"
            )
        return "\n".join(lines)

    def _db_status_text(self) -> str:
        data = self.store.database_diagnostics()
        counts = data["counts"]
        lines = [
            "database status",
            f"path={data['path']}",
            f"exists={data['exists']} size_mb={data['size_bytes'] / 1_000_000:.2f}",
            (
                f"markets={counts['markets']} btc_ticks={counts['btc_ticks']} "
                f"signals={counts['signals']} trades={counts['trades']} "
                f"results={counts['results']}"
            ),
            f"signals first={data['signal_first_at']} latest={data['signal_latest_at']}",
            f"trades first={data['trade_first_at']} latest={data['trade_latest_at']}",
            "signal strategies:",
        ]
        signal_strategies = data["signal_strategies"] or []
        if signal_strategies:
            lines.extend(f"{name}: {count}" for name, count in signal_strategies)
        else:
            lines.append("none")
        lines.append("trade strategies:")
        trade_strategies = data["trade_strategies"] or []
        if trade_strategies:
            lines.extend(f"{name}: {count}" for name, count in trade_strategies)
        else:
            lines.append("none")
        return "\n".join(lines)

    def _signal_stats_text(self) -> str:
        rows = self.store.strategy_signal_breakdown("candidate_v1")
        lines = ["candidate_v1 signal stats"]
        if not rows:
            lines.append("no signals")
            return "\n".join(lines)
        for row in rows:
            lines.append(
                f"{row['action']} {row['outcome']} {row['reason']}: n={row['signals']} "
                f"edge={(row['avg_expected_edge'] or 0):.4f} "
                f"score={(row['avg_inefficiency_score'] or 0):.3f} "
                f"conf={(row['avg_confidence'] or 0):.3f} "
                f"stc={(row['avg_seconds_to_close'] or 0):.1f}s "
                f"qage={(row['avg_quote_age_ms'] or 0):.0f}ms "
                f"lag={(row['avg_repricing_lag_ms'] or 0):.0f}ms"
            )
        return "\n".join(lines)

    @staticmethod
    def _split_message(text: str, limit: int = 3900) -> list[str]:
        chunks: list[str] = []
        current = ""
        for line in text.splitlines():
            if len(current) + len(line) + 1 > limit:
                chunks.append(current)
                current = line
            else:
                current = f"{current}\n{line}" if current else line
        if current:
            chunks.append(current)
        return chunks

    def _regime_text(self) -> str:
        regime = MarketRegimeEngine().classify_values(0, 0, 0, 0, 0, 0, 0).regime
        return f"current_regime={regime} source=summary_placeholder"

    def _research_gate_text(self) -> str:
        return (
            "Run: python -m app.backtest.research_gate "
            "--preset candidate_v1 --only-complete-microstructure"
        )

    def _walkforward_text(self) -> str:
        return (
            "Run: python -m app.backtest.walk_forward "
            "--preset candidate_v1 --only-complete-microstructure"
        )

    def _regime_gate_text(self) -> str:
        return (
            "Run: python -m app.backtest.regime_memory_report --preset candidate_v1 "
            "--only-complete-microstructure --write"
        )

    def _forward_validation_text(self) -> str:
        return (
            "Run: python -m app.backtest.forward_validation_report "
            "--preset candidate_v1 --only-complete-microstructure"
        )

    def _hourly_text(self) -> str:
        return (
            "Run: python -m app.backtest.hourly_regime_report "
            "--preset candidate_v1 --only-complete-microstructure"
        )

    def _edge_quality_text(self) -> str:
        return (
            "Run: python -m app.backtest.edge_quality_report "
            "--preset candidate_v1 --only-complete-microstructure"
        )

    async def run(self) -> None:
        if not self.settings.telegram_bot_token:
            return
        dp = Dispatcher()
        dp.include_router(self.router)
        backoff = 5.0
        while True:
            session = AiohttpSession()
            session._connector_init["resolver"] = aiohttp.ThreadedResolver()
            bot = Bot(self.settings.telegram_bot_token, session=session)
            try:
                await dp.start_polling(bot)
                backoff = 5.0
            except TelegramConflictError:
                log.error(
                    "Telegram polling disabled: another process is already using this bot token"
                )
                return
            except TelegramNetworkError as exc:
                log.warning("Telegram network error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120.0)
            except Exception:
                log.exception("Telegram polling failed")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120.0)
            finally:
                await bot.session.close()
