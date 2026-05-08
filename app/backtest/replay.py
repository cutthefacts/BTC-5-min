from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from app.backtest.analytics import max_drawdown, profit_factor
from app.backtest.filters import (
    TimeFilters,
    add_common_report_args,
    allowed_by_windows,
    filters_from_args,
    signal_time_filter_sql,
)
from app.backtest.presets import PRESET_NAMES, load_preset
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


@dataclass(slots=True)
class ReplayResult:
    trades: int
    winrate: float
    net_pnl: float
    profit_factor: float | None
    max_drawdown: float
    average_edge: float
    average_realized_edge: float
    stale_fill_rate: float
    missed_fill_rate: float
    stale_reasons: dict[str, int] | None = None


class TickByTickReplay:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def run(
        self,
        latency_ms: int,
        slippage_bps: float,
        min_score: float,
        min_confidence: float,
        min_edge: float,
        stale_quote_ms: float | None = None,
        repricing_lag_ms: float | None = None,
        include_hold_candidates: bool = True,
        execution_mode: Literal["taker", "maker", "hybrid"] = "taker",
        side_mode: Literal["UP_ONLY", "DOWN_ONLY", "BOTH", "UP_PREFERRED"] = "BOTH",
        avoid_liquidity_sweep: bool | None = None,
        allowed_entry_windows: str | None = None,
        blocked_entry_windows: str | None = None,
        min_quote_age_ms: float | None = None,
        max_quote_age_ms: float | None = None,
        min_repricing_lag_ms: float | None = None,
        max_repricing_lag_ms: float | None = None,
        time_filters: TimeFilters | None = None,
        soft_filters: bool = False,
    ) -> ReplayResult:
        settings = get_settings()
        action_filter = "" if include_hold_candidates else "and s.action in ('BUY_UP', 'BUY_DOWN')"
        stale_filter = (
            "and s.quote_age_ms >= ?" if stale_quote_ms is not None and not soft_filters else ""
        )
        lag_filter = ""
        if repricing_lag_ms is not None and not soft_filters:
            lag_filter = "and s.repricing_lag_ms >= ?"
        side_filter = {
            "UP_ONLY": "and s.outcome = 'UP'",
            "DOWN_ONLY": "and s.outcome = 'DOWN'",
            "BOTH": "",
            "UP_PREFERRED": "",
        }[side_mode]
        complete_sql, complete_params = signal_time_filter_sql("s", time_filters or TimeFilters())
        sql_min_score = 0.0 if soft_filters else min_score
        sql_min_confidence = 0.0 if soft_filters else min_confidence
        sql_min_edge = -1.0 if soft_filters else min_edge
        params: list[float | str] = [sql_min_score, sql_min_confidence, sql_min_edge]
        if stale_quote_ms is not None and not soft_filters:
            params.append(stale_quote_ms)
        if repricing_lag_ms is not None and not soft_filters:
            params.append(repricing_lag_ms)
        params.extend(complete_params)
        signals = self.conn.execute(
            f"""
            select s.*, m.up_token_id, m.down_token_id, m.end_time, r.winning_token_id
            from signals s
            join markets m on m.condition_id = s.market_id
            join results r on r.market_id = s.market_id
            where s.outcome is not null
              {action_filter}
              and s.inefficiency_score >= ?
              and s.confidence >= ?
              and s.edge >= ?
              {stale_filter}
              {lag_filter}
              {side_filter}
              {complete_sql}
            order by s.timestamp
            """,
            params,
        ).fetchall()
        pnls: list[float] = []
        edges: list[float] = []
        realized_edges: list[float] = []
        stale_fills = 0
        stale_reasons: dict[str, int] = {}
        missed = 0
        for signal in signals:
            if (
                side_mode == "UP_PREFERRED"
                and signal["outcome"] == "DOWN"
                and (
                    float(signal["confidence"] or 0) < 0.65
                    or float(signal["edge"] or 0) < 0.10
                )
            ):
                continue
            if avoid_liquidity_sweep is None:
                avoid_liquidity_sweep = settings.avoid_liquidity_sweep
            if avoid_liquidity_sweep and bool(signal["liquidity_sweep"]) and not soft_filters:
                continue
            quote_age = float(signal["quote_age_ms"] or 0)
            lag = float(signal["repricing_lag_ms"] or 0)
            if min_quote_age_ms is not None and quote_age < min_quote_age_ms and not soft_filters:
                continue
            if max_quote_age_ms is not None and quote_age > max_quote_age_ms and not soft_filters:
                continue
            if (
                min_repricing_lag_ms is not None
                and lag < min_repricing_lag_ms
                and not soft_filters
            ):
                continue
            if (
                max_repricing_lag_ms is not None
                and lag > max_repricing_lag_ms
                and not soft_filters
            ):
                continue
            seconds_to_close = (
                sqlite_datetime(signal["end_time"]) - sqlite_datetime(signal["timestamp"])
            ).total_seconds()
            if not soft_filters and not allowed_by_windows(
                seconds_to_close,
                allowed_entry_windows,
                blocked_entry_windows,
            ):
                continue
            if soft_filters:
                penalty = self._soft_filter_penalty(
                    signal,
                    seconds_to_close,
                    allowed_entry_windows,
                    blocked_entry_windows,
                    min_quote_age_ms,
                    max_quote_age_ms,
                    min_repricing_lag_ms,
                    max_repricing_lag_ms,
                    avoid_liquidity_sweep,
                    min_score,
                    min_confidence,
                )
                if float(signal["edge"] or 0) - penalty < min_edge:
                    continue
            token_id = (
                signal["up_token_id"] if signal["outcome"] == "UP" else signal["down_token_id"]
            )
            ask = self.future_ask(token_id, signal["timestamp"], latency_ms)
            if ask is None:
                missed += 1
                continue
            fill_price = self._fill_price(signal, ask, slippage_bps, execution_mode)
            if fill_price is None:
                missed += 1
                continue
            reprice_threshold = float(signal["market_probability"]) + 0.03
            if fill_price > reprice_threshold:
                missed += 1
                continue
            size = 1.0 / fill_price
            fee = fill_price * size * settings.fee_bps / 10_000
            fee_cost = fill_price * settings.fee_bps / 10_000
            payout = size if token_id == signal["winning_token_id"] else 0.0
            pnl = payout - fill_price * size - fee
            pnls.append(pnl)
            edges.append(float(signal["edge"]))
            realized_edge = float(signal["expected_probability"]) - fill_price - fee_cost
            realized_edges.append(realized_edge)
            stale_reason = self._stale_reason(signal, fill_price, realized_edge)
            if stale_reason is not None:
                stale_fills += 1
                stale_reasons[stale_reason] = stale_reasons.get(stale_reason, 0) + 1
        trades = len(pnls)
        total_candidates = trades + missed
        return ReplayResult(
            trades=trades,
            winrate=sum(1 for pnl in pnls if pnl > 0) / trades if trades else 0.0,
            net_pnl=sum(pnls),
            profit_factor=profit_factor(pnls),
            max_drawdown=max_drawdown(pnls),
            average_edge=sum(edges) / len(edges) if edges else 0.0,
            average_realized_edge=(
                sum(realized_edges) / len(realized_edges) if realized_edges else 0.0
            ),
            stale_fill_rate=stale_fills / trades if trades else 0.0,
            missed_fill_rate=missed / total_candidates if total_candidates else 0.0,
            stale_reasons=stale_reasons or None,
        )

    def _fill_price(
        self,
        signal: sqlite3.Row,
        future_ask: float,
        slippage_bps: float,
        execution_mode: Literal["taker", "maker", "hybrid"],
    ) -> float | None:
        if execution_mode == "taker":
            return min(0.99, future_ask * (1 + slippage_bps / 10_000))
        signal_price = float(signal["market_probability"])
        if execution_mode == "maker":
            passive_price = max(0.01, signal_price - 0.01)
            return passive_price if future_ask <= passive_price else None
        passive_price = max(0.01, signal_price - 0.005)
        if future_ask <= passive_price:
            return passive_price
        return min(0.99, future_ask * (1 + slippage_bps / 10_000))

    def _stale_reason(
        self,
        signal: sqlite3.Row,
        fill_price: float,
        realized_edge: float,
    ) -> str | None:
        settings = get_settings()
        quote_age = float(signal["quote_age_ms"] or 0)
        if quote_age > settings.max_quote_age_ms:
            return "quote_age_exceeded"
        if fill_price > float(signal["market_probability"]) + settings.max_spread:
            return "repriced_beyond_threshold"
        if realized_edge < settings.min_edge:
            return "edge_disappeared_before_fill"
        return None

    def _soft_filter_penalty(
        self,
        signal: sqlite3.Row,
        seconds_to_close: float,
        allowed_entry_windows: str | None,
        blocked_entry_windows: str | None,
        min_quote_age_ms: float | None,
        max_quote_age_ms: float | None,
        min_repricing_lag_ms: float | None,
        max_repricing_lag_ms: float | None,
        avoid_liquidity_sweep: bool | None,
        min_score: float,
        min_confidence: float,
    ) -> float:
        penalty = 0.0
        score = float(signal["inefficiency_score"] or 0)
        confidence = float(signal["confidence"] or 0)
        quote_age = float(signal["quote_age_ms"] or 0)
        lag = float(signal["repricing_lag_ms"] or 0)
        imbalance = abs(float(signal["imbalance_ratio"] or 0))
        if score < min_score:
            penalty += min(0.04, (min_score - score) * 0.08)
        if confidence < min_confidence:
            penalty += min(0.04, (min_confidence - confidence) * 0.08)
        if min_quote_age_ms is not None and quote_age < min_quote_age_ms:
            penalty += 0.01
        if max_quote_age_ms is not None and quote_age > max_quote_age_ms:
            penalty += min(0.04, (quote_age - max_quote_age_ms) / 100_000)
        if min_repricing_lag_ms is not None and lag < min_repricing_lag_ms:
            penalty += 0.015
        if max_repricing_lag_ms is not None and lag > max_repricing_lag_ms:
            penalty += 0.02
        if imbalance < 0.15:
            penalty += 0.015
        if not allowed_by_windows(seconds_to_close, allowed_entry_windows, blocked_entry_windows):
            penalty += 0.025
        if avoid_liquidity_sweep and bool(signal["liquidity_sweep"]):
            penalty += 0.04
        return penalty

    def future_ask(self, token_id: str, timestamp: str, latency_ms: int) -> float | None:
        target = (
            sqlite_datetime(timestamp) + timedelta(milliseconds=latency_ms)
        ).isoformat()
        row = self.conn.execute(
            """
            select best_ask
            from orderbooks
            where token_id = ?
              and timestamp >= ?
              and best_ask is not null
            order by timestamp asc
            limit 1
            """,
            (token_id, target),
        ).fetchone()
        return float(row["best_ask"]) if row else None


def sqlite_datetime(raw: str):
    from datetime import datetime

    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latency-ms", type=int, default=250)
    parser.add_argument(
        "--preset",
        choices=PRESET_NAMES,
        default=None,
    )
    parser.add_argument("--soft-filters", action="store_true")
    parser.add_argument("--slippage-bps", type=float, default=get_settings().slippage_bps)
    parser.add_argument("--min-score", type=float, default=get_settings().min_inefficiency_score)
    parser.add_argument("--min-confidence", type=float, default=get_settings().min_confidence)
    parser.add_argument("--min-edge", type=float, default=get_settings().min_edge)
    parser.add_argument("--stale-quote-ms", type=float, default=get_settings().stale_quote_ms)
    parser.add_argument("--repricing-lag-ms", type=float, default=get_settings().repricing_lag_ms)
    parser.add_argument("--min-quote-age-ms", type=float, default=get_settings().min_quote_age_ms)
    parser.add_argument(
        "--max-quote-age-ms",
        type=float,
        default=get_settings().max_quote_age_ms_filter,
    )
    parser.add_argument(
        "--min-repricing-lag-ms",
        type=float,
        default=get_settings().min_repricing_lag_ms,
    )
    parser.add_argument(
        "--max-repricing-lag-ms",
        type=float,
        default=get_settings().max_repricing_lag_ms,
    )
    parser.add_argument("--allowed-entry-windows", default=None)
    parser.add_argument(
        "--blocked-entry-windows",
        default=get_settings().blocked_entry_windows_seconds_to_close,
    )
    parser.add_argument(
        "--side-mode",
        choices=["UP_ONLY", "DOWN_ONLY", "BOTH", "UP_PREFERRED"],
        default="BOTH",
    )
    parser.add_argument("--allow-liquidity-sweep", action="store_true")
    parser.add_argument(
        "--execution-mode",
        choices=["taker", "maker", "hybrid"],
        default="taker",
    )
    add_common_report_args(parser)
    parser.add_argument(
        "--executed-only",
        action="store_true",
        help="Replay only actual BUY signals. Default includes HOLD candidates.",
    )
    args = parser.parse_args()
    settings = get_settings()
    preset = load_preset(args.preset)
    if args.preset:
        args.min_score = preset.min_score
        args.min_confidence = preset.min_confidence
        args.min_edge = preset.min_edge
        args.side_mode = preset.side_mode
        args.allowed_entry_windows = preset.allowed_entry_windows
        args.blocked_entry_windows = preset.blocked_entry_windows
        args.min_quote_age_ms = preset.min_quote_age_ms
        args.max_quote_age_ms = preset.max_quote_age_ms
        args.min_repricing_lag_ms = preset.min_repricing_lag_ms
        args.max_repricing_lag_ms = preset.max_repricing_lag_ms
        args.allow_liquidity_sweep = not preset.avoid_liquidity_sweep
        args.soft_filters = args.soft_filters or preset.soft_filters
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    result = TickByTickReplay(conn).run(
        latency_ms=args.latency_ms,
        slippage_bps=args.slippage_bps,
        min_score=args.min_score,
        min_confidence=args.min_confidence,
        min_edge=args.min_edge,
        stale_quote_ms=args.stale_quote_ms,
        repricing_lag_ms=args.repricing_lag_ms,
        include_hold_candidates=not args.executed_only,
        execution_mode=args.execution_mode,
        side_mode=args.side_mode,
        avoid_liquidity_sweep=not args.allow_liquidity_sweep,
        allowed_entry_windows=args.allowed_entry_windows,
        blocked_entry_windows=args.blocked_entry_windows,
        min_quote_age_ms=args.min_quote_age_ms,
        max_quote_age_ms=args.max_quote_age_ms,
        min_repricing_lag_ms=args.min_repricing_lag_ms,
        max_repricing_lag_ms=args.max_repricing_lag_ms,
        time_filters=filters_from_args(args),
        soft_filters=args.soft_filters,
    )
    print(result)
    if result.trades == 0:
        print_zero_trade_hint(conn, args)


def print_zero_trade_hint(conn: sqlite3.Connection, args) -> None:
    from app.backtest.diagnostics import apply_hard_filters, complete_candidates

    filters = filters_from_args(args)
    candidates = complete_candidates(conn, filters)
    preset = load_preset(args.preset)
    preset = type(preset)(
        name=preset.name,
        min_score=args.min_score,
        min_confidence=args.min_confidence,
        min_edge=args.min_edge,
        side_mode=args.side_mode,
        allowed_entry_windows=args.allowed_entry_windows,
        blocked_entry_windows=args.blocked_entry_windows,
        min_quote_age_ms=args.min_quote_age_ms,
        max_quote_age_ms=args.max_quote_age_ms,
        min_repricing_lag_ms=args.min_repricing_lag_ms,
        max_repricing_lag_ms=args.max_repricing_lag_ms,
        avoid_liquidity_sweep=not args.allow_liquidity_sweep,
        soft_filters=args.soft_filters,
    )
    _, steps = apply_hard_filters(candidates, preset, args.soft_filters)
    print("Zero Trade Diagnostic")
    print("=====================")
    print({"total_complete_signals": len(candidates)})
    for step in steps:
        if step.rejected:
            print(
                {
                    "filter": step.reason,
                    "before": step.before,
                    "after": step.after,
                    "rejected": step.rejected,
                    "rejection_pct": round(step.rejection_pct, 4),
                }
            )
    print({"suggested_next_research_preset": "balanced"})


if __name__ == "__main__":
    main()
