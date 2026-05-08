from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from app.models import Market, Outcome, Signal, Trade

SIGNAL_SNAPSHOT_COLUMNS = {
    "btc_price": "real",
    "price_to_beat": "real",
    "distance_bps": "real",
    "drift_15s_bps": "real",
    "drift_30s_bps": "real",
    "drift_60s_bps": "real",
    "drift_180s_bps": "real",
    "realized_vol_30s_bps": "real",
    "realized_vol_60s_bps": "real",
    "realized_vol_180s_bps": "real",
    "seconds_to_close": "real",
    "seconds_from_open": "real",
    "best_bid": "real",
    "best_ask": "real",
    "mid_price": "real",
    "spread": "real",
    "spread_bps": "real",
    "top_bid_size": "real",
    "top_ask_size": "real",
    "bid_depth_3": "real",
    "ask_depth_3": "real",
    "bid_depth_5": "real",
    "ask_depth_5": "real",
    "imbalance_acceleration": "real",
    "disappearing_liquidity": "integer",
    "edge_quality_score": "real",
    "extreme_edge_suspect": "integer",
    "fair_value": "real",
    "expected_edge": "real",
    "realized_edge": "real",
    "regime": "text",
    "regime_confidence": "real",
    "regime_source": "text",
    "side_mode": "text",
    "strategy_version": "text",
    "feature_schema_version": "integer",
    "data_collection_started_at": "text",
}


class SQLiteStore:
    def __init__(self, database_url: str) -> None:
        if database_url.startswith("sqlite:///"):
            raw = database_url.removeprefix("sqlite:///")
        else:
            raw = database_url
        self.path = Path(raw)
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("pragma busy_timeout = 30000")
        self.conn.execute("pragma journal_mode = wal")
        self.conn.execute("pragma synchronous = normal")

    def _write(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        for attempt in range(4):
            try:
                with self._lock:
                    self.conn.execute(sql, params)
                    self.conn.commit()
                return
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower() or attempt == 3:
                    raise
                time.sleep(0.25 * (attempt + 1))

    def init(self) -> None:
        with self._lock:
            self.conn.executescript(
                """
                create table if not exists markets (
                    condition_id text primary key,
                    question text not null,
                    slug text not null,
                    start_time text not null,
                    end_time text not null,
                    price_to_beat real not null,
                    up_token_id text not null,
                    down_token_id text not null,
                    active integer not null
                );
                create table if not exists btc_ticks (
                    timestamp text primary key,
                    price real not null
                );
                create table if not exists orderbooks (
                    id integer primary key autoincrement,
                    timestamp text not null,
                    market_id text not null,
                    token_id text not null,
                    best_bid real,
                    best_ask real,
                    spread real,
                    liquidity real,
                    raw_json text not null
                );
                create table if not exists signals (
                    id integer primary key autoincrement,
                    timestamp text not null,
                    market_id text not null,
                    action text not null,
                    outcome text,
                    expected_probability real not null,
                    market_probability real not null,
                    edge real not null,
                    strength real not null,
                    reason text not null,
                    inefficiency_score real,
                    confidence real,
                    quote_age_ms real,
                    repricing_lag_ms real,
                    imbalance_ratio real,
                    liquidity_sweep integer,
                    strategy_name text
                );
                create table if not exists trades (
                    id integer primary key autoincrement,
                    timestamp text not null,
                    order_id text not null,
                    market_id text not null,
                    token_id text not null,
                    outcome text not null,
                    price real not null,
                    size real not null,
                    fee real not null,
                    expected_edge real,
                    signal_to_fill_delay_ms real,
                    fill_latency_ms real,
                    realized_edge real,
                    post_fill_drift real,
                    stale_fill integer,
                    signal_timestamp text,
                    order_submit_timestamp text,
                    fill_timestamp text,
                    signal_to_submit_ms real,
                    submit_to_fill_ms real,
                    total_fill_latency_ms real,
                    expected_edge_at_signal real,
                    expected_edge_at_submit real,
                    realized_edge_after_fill real,
                    stale_reason text,
                    strategy_name text
                );
                create table if not exists microstructure_events (
                    id integer primary key autoincrement,
                    timestamp text not null,
                    market_id text not null,
                    token_id text not null,
                    bid_volume real not null,
                    ask_volume real not null,
                    weighted_bid_volume real not null,
                    weighted_ask_volume real not null,
                    imbalance_ratio real not null,
                    imbalance_acceleration real not null,
                    quote_age_ms real not null,
                    repricing_lag_ms real not null,
                    rapid_ask_disappearance integer not null,
                    rapid_bid_disappearance integer not null,
                    disappearing_liquidity integer not null,
                    liquidity_sweep integer not null,
                    aggressive_repricing integer not null,
                    liquidity_vacuum integer not null
                );
                create table if not exists data_quality_events (
                    id integer primary key autoincrement,
                    timestamp text not null,
                    source text not null,
                    event_type text not null,
                    severity text not null,
                    details text not null
                );
                create table if not exists missed_opportunities (
                    id integer primary key autoincrement,
                    market_id text not null,
                    timestamp text not null,
                    side text not null,
                    seconds_to_close real not null,
                    market_price real not null,
                    final_outcome text not null,
                    theoretical_pnl real not null,
                    reason_not_traded text not null,
                    inefficiency_score real not null,
                    confidence real not null
                );
                create table if not exists optimization_runs (
                    id integer primary key autoincrement,
                    created_at text not null,
                    config text not null,
                    trades integer not null,
                    net_pnl real not null,
                    profit_factor real,
                    max_drawdown real not null,
                    winrate real not null,
                    avg_pnl real not null,
                    stale_fill_rate real not null,
                    missed_fill_rate real not null,
                    reliable integer not null
                );
                create table if not exists regime_performance (
                    id integer primary key autoincrement,
                    updated_at text not null,
                    regime text not null,
                    side text not null,
                    entry_window text not null,
                    trades integer not null,
                    profit_factor real,
                    max_drawdown real not null,
                    stale_fill_rate real not null,
                    missed_fill_rate real not null,
                    rolling_profit_factor real,
                    rolling_drawdown real not null,
                    bad_regime_score real not null,
                    unique(regime, side, entry_window)
                );
                create index if not exists idx_btc_ticks_timestamp
                    on btc_ticks(timestamp);
                create index if not exists idx_orderbooks_token_timestamp
                    on orderbooks(token_id, timestamp);
                create index if not exists idx_signals_market_timestamp
                    on signals(market_id, timestamp);
                create index if not exists idx_trades_market_timestamp
                    on trades(market_id, timestamp);
                create index if not exists idx_microstructure_token_timestamp
                    on microstructure_events(token_id, timestamp);
                create table if not exists results (
                    market_id text primary key,
                    winning_token_id text not null,
                    settled_at text not null,
                    pnl real not null,
                    winning_outcome text,
                    price_to_beat real,
                    final_price real,
                    gross_payout real,
                    cost_basis real,
                    fees real,
                    trade_count integer,
                    settlement_source text,
                    strategy_name text
                );
                """
            )
            self.conn.commit()
            self._ensure_signal_columns()
            self._ensure_trade_columns()
            self._ensure_result_columns()
            self._ensure_regime_performance_columns()

    def _ensure_signal_columns(self) -> None:
        columns = {
            "inefficiency_score": "real",
            "confidence": "real",
            "quote_age_ms": "real",
            "repricing_lag_ms": "real",
            "imbalance_ratio": "real",
            "liquidity_sweep": "integer",
            "strategy_name": "text",
        }
        columns.update(SIGNAL_SNAPSHOT_COLUMNS)
        self._ensure_columns("signals", columns)

    def _ensure_trade_columns(self) -> None:
        columns = {
            "expected_edge": "real",
            "signal_to_fill_delay_ms": "real",
            "fill_latency_ms": "real",
            "realized_edge": "real",
            "post_fill_drift": "real",
            "stale_fill": "integer",
            "signal_timestamp": "text",
            "order_submit_timestamp": "text",
            "fill_timestamp": "text",
            "signal_to_submit_ms": "real",
            "submit_to_fill_ms": "real",
            "total_fill_latency_ms": "real",
            "expected_edge_at_signal": "real",
            "expected_edge_at_submit": "real",
            "realized_edge_after_fill": "real",
            "stale_reason": "text",
            "strategy_name": "text",
            "strategy_version": "text",
            "feature_schema_version": "integer",
            "data_collection_started_at": "text",
        }
        self._ensure_columns("trades", columns)

    def _ensure_result_columns(self) -> None:
        columns = {
            "winning_outcome": "text",
            "price_to_beat": "real",
            "final_price": "real",
            "gross_payout": "real",
            "cost_basis": "real",
            "fees": "real",
            "trade_count": "integer",
            "settlement_source": "text",
            "strategy_name": "text",
            "strategy_version": "text",
            "feature_schema_version": "integer",
            "data_collection_started_at": "text",
        }
        self._ensure_columns("results", columns)
        self.conn.execute(
            """
            update results
            set settlement_source = 'binance'
            where settlement_source is null
            """
        )
        self.conn.commit()

    def _ensure_regime_performance_columns(self) -> None:
        columns = {
            "rolling_profit_factor": "real",
            "rolling_drawdown": "real default 0",
            "bad_regime_score": "real default 0",
        }
        self._ensure_columns("regime_performance", columns)

    def _ensure_columns(self, table: str, columns: dict[str, str]) -> None:
        existing = {
            row["name"]
            for row in self.conn.execute(f"pragma table_info({table})").fetchall()
        }
        for name, column_type in columns.items():
            if name not in existing:
                self.conn.execute(f"alter table {table} add column {name} {column_type}")
        self.conn.commit()

    def save_market(self, market: Market) -> None:
        price_to_beat = market.price_to_beat
        if price_to_beat <= 0:
            with self._lock:
                existing = self.conn.execute(
                    "select price_to_beat from markets where condition_id = ?",
                    (market.condition_id,),
                ).fetchone()
            if existing is not None and float(existing["price_to_beat"]) > 0:
                price_to_beat = float(existing["price_to_beat"])
        self._write(
            """
            insert or replace into markets values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market.condition_id,
                market.question,
                market.slug,
                market.start_time.isoformat(),
                market.end_time.isoformat(),
                price_to_beat,
                market.up_token_id,
                market.down_token_id,
                int(market.active),
            ),
        )

    def save_btc_tick(self, timestamp: str, price: float) -> None:
        self._write(
            "insert or replace into btc_ticks(timestamp, price) values (?, ?)",
            (timestamp, price),
        )

    def save_orderbook_snapshot(self, payload: dict[str, Any]) -> None:
        self._write(
            """
            insert into orderbooks(
                timestamp, market_id, token_id, best_bid, best_ask, spread, liquidity, raw_json
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["timestamp"],
                payload["market_id"],
                payload["token_id"],
                payload.get("best_bid"),
                payload.get("best_ask"),
                payload.get("spread"),
                payload.get("liquidity"),
                json.dumps(payload, separators=(",", ":")),
            ),
        )

    def save_signal(self, signal: Signal) -> None:
        snapshot_columns = list(SIGNAL_SNAPSHOT_COLUMNS)
        snapshot_values = [self._signal_value(signal, column) for column in snapshot_columns]
        columns_sql = ", ".join(
            [
                "timestamp",
                "market_id",
                "action",
                "outcome",
                "expected_probability",
                "market_probability",
                "edge",
                "strength",
                "reason",
                "inefficiency_score",
                "confidence",
                "quote_age_ms",
                "repricing_lag_ms",
                "imbalance_ratio",
                "liquidity_sweep",
                "strategy_name",
                *snapshot_columns,
            ]
        )
        placeholders = ", ".join("?" for _ in range(16 + len(snapshot_columns)))
        self._write(
            f"insert into signals({columns_sql}) values ({placeholders})",
            (
                signal.timestamp.isoformat(),
                signal.market_id,
                signal.action.value,
                signal.outcome.value if signal.outcome else None,
                signal.expected_probability,
                signal.market_probability,
                signal.edge,
                signal.strength,
                signal.reason,
                signal.inefficiency_score,
                signal.confidence,
                signal.quote_age_ms,
                signal.repricing_lag_ms,
                signal.imbalance_ratio,
                int(signal.liquidity_sweep),
                signal.strategy_name,
                *snapshot_values,
            ),
        )

    @staticmethod
    def _signal_value(signal: Signal, column: str):
        value = getattr(signal, column)
        if isinstance(value, bool):
            return int(value)
        return value

    def save_trade(self, trade: Trade) -> None:
        self._write(
            """
            insert into trades(
                timestamp, order_id, market_id, token_id, outcome, price, size, fee,
                expected_edge, signal_to_fill_delay_ms, fill_latency_ms, realized_edge,
                post_fill_drift, stale_fill, signal_timestamp, order_submit_timestamp,
                fill_timestamp, signal_to_submit_ms, submit_to_fill_ms, total_fill_latency_ms,
                expected_edge_at_signal, expected_edge_at_submit, realized_edge_after_fill,
                stale_reason, strategy_name, strategy_version, feature_schema_version,
                data_collection_started_at
            )
            values (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                trade.timestamp.isoformat(),
                trade.order_id,
                trade.market_id,
                trade.token_id,
                trade.outcome.value,
                trade.price,
                trade.size,
                trade.fee,
                trade.expected_edge,
                trade.signal_to_fill_delay_ms,
                trade.fill_latency_ms,
                trade.realized_edge,
                trade.post_fill_drift,
                int(trade.stale_fill),
                trade.signal_timestamp.isoformat() if trade.signal_timestamp else None,
                trade.order_submit_timestamp.isoformat()
                if trade.order_submit_timestamp
                else None,
                trade.fill_timestamp.isoformat() if trade.fill_timestamp else None,
                trade.signal_to_submit_ms,
                trade.submit_to_fill_ms,
                trade.total_fill_latency_ms,
                trade.expected_edge_at_signal,
                trade.expected_edge_at_submit,
                trade.realized_edge_after_fill,
                trade.stale_reason,
                trade.strategy_name,
                trade.strategy_version,
                trade.feature_schema_version,
                trade.data_collection_started_at,
            ),
        )

    def save_microstructure_event(self, snapshot) -> None:
        self._write(
            """
            insert into microstructure_events(
                timestamp, market_id, token_id, bid_volume, ask_volume,
                weighted_bid_volume, weighted_ask_volume, imbalance_ratio,
                imbalance_acceleration, quote_age_ms, repricing_lag_ms,
                rapid_ask_disappearance, rapid_bid_disappearance,
                disappearing_liquidity, liquidity_sweep, aggressive_repricing,
                liquidity_vacuum
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp.isoformat(),
                snapshot.market_id,
                snapshot.token_id,
                snapshot.bid_volume,
                snapshot.ask_volume,
                snapshot.weighted_bid_volume,
                snapshot.weighted_ask_volume,
                snapshot.imbalance_ratio,
                snapshot.imbalance_acceleration,
                snapshot.quote_age_ms,
                snapshot.repricing_lag_ms,
                int(snapshot.rapid_ask_disappearance),
                int(snapshot.rapid_bid_disappearance),
                int(snapshot.disappearing_liquidity),
                int(snapshot.liquidity_sweep),
                int(snapshot.aggressive_repricing),
                int(snapshot.liquidity_vacuum),
            ),
        )

    def save_data_quality_event(
        self,
        timestamp: str,
        source: str,
        event_type: str,
        severity: str,
        details: str,
    ) -> None:
        self._write(
            """
            insert into data_quality_events(timestamp, source, event_type, severity, details)
            values (?, ?, ?, ?, ?)
            """,
            (timestamp, source, event_type, severity, details),
        )

    def clear_missed_opportunities(self) -> None:
        self._write("delete from missed_opportunities")

    def save_missed_opportunity(
        self,
        market_id: str,
        timestamp: str,
        side: str,
        seconds_to_close: float,
        market_price: float,
        final_outcome: str,
        theoretical_pnl: float,
        reason_not_traded: str,
        inefficiency_score: float,
        confidence: float,
    ) -> None:
        self._write(
            """
            insert into missed_opportunities(
                market_id, timestamp, side, seconds_to_close, market_price,
                final_outcome, theoretical_pnl, reason_not_traded,
                inefficiency_score, confidence
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market_id,
                timestamp,
                side,
                seconds_to_close,
                market_price,
                final_outcome,
                theoretical_pnl,
                reason_not_traded,
                inefficiency_score,
                confidence,
            ),
        )

    def save_optimization_run(
        self,
        created_at: str,
        config: str,
        trades: int,
        net_pnl: float,
        profit_factor: float | None,
        max_drawdown: float,
        winrate: float,
        avg_pnl: float,
        stale_fill_rate: float,
        missed_fill_rate: float,
        reliable: bool,
    ) -> None:
        self._write(
            """
            insert into optimization_runs(
                created_at, config, trades, net_pnl, profit_factor, max_drawdown,
                winrate, avg_pnl, stale_fill_rate, missed_fill_rate, reliable
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                config,
                trades,
                net_pnl,
                profit_factor,
                max_drawdown,
                winrate,
                avg_pnl,
                stale_fill_rate,
                missed_fill_rate,
                int(reliable),
            ),
        )

    def save_regime_performance(
        self,
        updated_at: str,
        regime: str,
        side: str,
        entry_window: str,
        trades: int,
        profit_factor: float | None,
        max_drawdown: float,
        stale_fill_rate: float,
        missed_fill_rate: float,
        rolling_profit_factor: float | None,
        rolling_drawdown: float,
        bad_regime_score: float,
    ) -> None:
        self._write(
            """
            insert into regime_performance(
                updated_at, regime, side, entry_window, trades, profit_factor,
                max_drawdown, stale_fill_rate, missed_fill_rate, rolling_profit_factor,
                rolling_drawdown, bad_regime_score
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(regime, side, entry_window) do update set
                updated_at = excluded.updated_at,
                trades = excluded.trades,
                profit_factor = excluded.profit_factor,
                max_drawdown = excluded.max_drawdown,
                stale_fill_rate = excluded.stale_fill_rate,
                missed_fill_rate = excluded.missed_fill_rate,
                rolling_profit_factor = excluded.rolling_profit_factor,
                rolling_drawdown = excluded.rolling_drawdown,
                bad_regime_score = excluded.bad_regime_score
            """,
            (
                updated_at,
                regime,
                side,
                entry_window,
                trades,
                profit_factor,
                max_drawdown,
                stale_fill_rate,
                missed_fill_rate,
                rolling_profit_factor,
                rolling_drawdown,
                bad_regime_score,
            ),
        )

    def regime_performance(self, regime: str, side: str, entry_window: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                """
                select *
                from regime_performance
                where regime = ? and side = ? and entry_window = ?
                """,
                (regime, side, entry_window),
            ).fetchone()

    def paper_gate_metrics(self) -> dict[str, float]:
        with self._lock:
            row = self.conn.execute(
                """
                select coalesce(sum(trade_count), 0) as trades,
                       coalesce(sum(pnl), 0) as pnl
                from results
                where trade_count > 0
                """
            ).fetchone()
        return {
            "completed_trades": float(row["trades"]),
            "net_pnl": float(row["pnl"]),
            "drawdown": self.result_summary()["max_drawdown"],
        }

    def table_counts(self) -> dict[str, int]:
        tables = (
            "markets",
            "btc_ticks",
            "orderbooks",
            "signals",
            "trades",
            "microstructure_events",
            "data_quality_events",
            "missed_opportunities",
            "optimization_runs",
        )
        with self._lock:
            return {
                table: int(
                    self.conn.execute(f"select count(*) from {table}").fetchone()[0]
                )
                for table in tables
            }

    def trade_summary(self) -> dict[str, float | str | None]:
        with self._lock:
            row = self.conn.execute(
                """
                select count(*) as trades,
                       coalesce(sum(price * size + fee), 0) as notional_spent,
                       max(timestamp) as latest_trade_at
                from trades
                """
            ).fetchone()
        return {
            "trades": float(row["trades"]),
            "notional_spent": float(row["notional_spent"]),
            "latest_trade_at": row["latest_trade_at"],
        }

    def strategy_trade_summary(self, strategy_name: str) -> dict[str, float | str | None]:
        with self._lock:
            row = self.conn.execute(
                """
                select count(*) as trades,
                       coalesce(sum(price * size + fee), 0) as notional_spent,
                       coalesce(avg(expected_edge), 0) as avg_edge,
                       coalesce(avg(stale_fill), 0) as stale_fill_rate,
                       max(timestamp) as latest_trade_at
                from trades
                where coalesce(strategy_name, 'baseline') = ?
                """,
                (strategy_name,),
            ).fetchone()
        return {
            "trades": float(row["trades"]),
            "notional_spent": float(row["notional_spent"]),
            "avg_edge": float(row["avg_edge"]),
            "stale_fill_rate": float(row["stale_fill_rate"]),
            "latest_trade_at": row["latest_trade_at"],
        }

    def last_trades(self, strategy_name: str, limit: int = 10) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    select timestamp, market_id, outcome, price, size, fee, stale_fill
                    from trades
                    where coalesce(strategy_name, 'baseline') = ?
                    order by timestamp desc
                    limit ?
                    """,
                    (strategy_name, limit),
                ).fetchall()
            )

    def settlement_candidates(self, cutoff_iso: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    select m.*
                    from markets m
                    left join results r on r.market_id = m.condition_id
                    where r.market_id is null
                      and m.price_to_beat > 0
                      and m.end_time <= ?
                    order by m.end_time asc
                    """,
                    (cutoff_iso,),
                ).fetchall()
            )

    def final_btc_tick(self, end_time_iso: str, max_lag_seconds: int) -> sqlite3.Row | None:
        return self.nearest_btc_tick(end_time_iso, max_lag_seconds)

    def nearest_btc_tick(self, timestamp_iso: str, max_lag_seconds: int) -> sqlite3.Row | None:
        with self._lock:
            after = self.conn.execute(
                """
                select timestamp, price
                from btc_ticks
                where timestamp >= ?
                order by timestamp asc
                limit 1
                """,
                (timestamp_iso,),
            ).fetchone()
            before = self.conn.execute(
                """
                select timestamp, price
                from btc_ticks
                where timestamp <= ?
                order by timestamp desc
                limit 1
                """,
                (timestamp_iso,),
            ).fetchone()
        return self._nearest_tick(timestamp_iso, max_lag_seconds, before, after)

    def repair_missing_price_to_beat(self, max_lag_seconds: int) -> int:
        with self._lock:
            rows = self.conn.execute(
                """
                select condition_id, start_time
                from markets
                where price_to_beat <= 0
                """
            ).fetchall()
        repaired = 0
        for row in rows:
            tick = self.nearest_btc_tick(row["start_time"], max_lag_seconds)
            if tick is None:
                continue
            self._write(
                "update markets set price_to_beat = ? where condition_id = ?",
                (float(tick["price"]), row["condition_id"]),
            )
            repaired += 1
        return repaired

    def _nearest_tick(
        self,
        end_time_iso: str,
        max_lag_seconds: int,
        before: sqlite3.Row | None,
        after: sqlite3.Row | None,
    ) -> sqlite3.Row | None:
        end_ts = self._parse_iso(end_time_iso)
        candidates = []
        for row in (before, after):
            if row is None:
                continue
            lag = abs((self._parse_iso(row["timestamp"]) - end_ts).total_seconds())
            if lag <= max_lag_seconds:
                candidates.append((lag, row))
        if not candidates:
            return None
        return min(candidates, key=lambda item: item[0])[1]

    def settle_market(
        self,
        market: sqlite3.Row,
        final_price: float,
        settled_at: str,
        settlement_source: str,
    ) -> dict[str, float | str]:
        winning_outcome = Outcome.UP if final_price >= market["price_to_beat"] else Outcome.DOWN
        winning_token_id = (
            market["up_token_id"] if winning_outcome == Outcome.UP else market["down_token_id"]
        )
        with self._lock:
            row = self.conn.execute(
                """
                select count(*) as trade_count,
                       coalesce(sum(price * size), 0) as entry_cost,
                       coalesce(sum(fee), 0) as fees,
                       coalesce(sum(case when token_id = ? then size else 0 end), 0) as payout,
                       min(coalesce(strategy_name, 'baseline')) as min_strategy_name,
                       max(coalesce(strategy_name, 'baseline')) as max_strategy_name
                from trades
                where market_id = ?
                """,
                (winning_token_id, market["condition_id"]),
            ).fetchone()
        cost_basis = float(row["entry_cost"]) + float(row["fees"])
        gross_payout = float(row["payout"])
        pnl = gross_payout - cost_basis
        strategy_name = (
            row["min_strategy_name"]
            if row["min_strategy_name"] and row["min_strategy_name"] == row["max_strategy_name"]
            else "mixed"
        )
        self._write(
            """
            insert or replace into results(
                market_id, winning_token_id, settled_at, pnl, winning_outcome,
                price_to_beat, final_price, gross_payout, cost_basis, fees, trade_count,
                settlement_source, strategy_name
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market["condition_id"],
                winning_token_id,
                settled_at,
                pnl,
                winning_outcome.value,
                market["price_to_beat"],
                final_price,
                gross_payout,
                cost_basis,
                float(row["fees"]),
                int(row["trade_count"]),
                settlement_source,
                strategy_name,
            ),
        )
        return {
            "market_id": market["condition_id"],
            "winning_outcome": winning_outcome.value,
            "final_price": final_price,
            "price_to_beat": float(market["price_to_beat"]),
            "pnl": pnl,
            "trade_count": int(row["trade_count"]),
            "settlement_source": settlement_source,
        }

    def result_summary(self) -> dict[str, float | int | None]:
        with self._lock:
            rows = self.conn.execute(
                """
                select settled_at, pnl, trade_count, settlement_source
                from results
                where trade_count > 0
                order by settled_at asc
                """
            ).fetchall()
        total_pnl = sum(float(row["pnl"]) for row in rows)
        wins = sum(1 for row in rows if float(row["pnl"]) > 0)
        losses = sum(1 for row in rows if float(row["pnl"]) < 0)
        gross_profit = sum(float(row["pnl"]) for row in rows if float(row["pnl"]) > 0)
        gross_loss = -sum(float(row["pnl"]) for row in rows if float(row["pnl"]) < 0)
        trade_count = sum(int(row["trade_count"]) for row in rows)
        chainlink_markets = sum(1 for row in rows if row["settlement_source"] == "chainlink")
        binance_markets = sum(1 for row in rows if row["settlement_source"] == "binance")
        equity = 0.0
        high_water = 0.0
        max_drawdown = 0.0
        for row in rows:
            equity += float(row["pnl"])
            high_water = max(high_water, equity)
            drawdown = high_water - equity
            max_drawdown = max(max_drawdown, drawdown)
        return {
            "settled_markets": len(rows),
            "settled_trades": trade_count,
            "pnl": total_pnl,
            "winrate": wins / len(rows) if rows else 0.0,
            "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
            "max_drawdown": max_drawdown,
            "wins": wins,
            "losses": losses,
            "chainlink_markets": chainlink_markets,
            "binance_markets": binance_markets,
        }

    @staticmethod
    def _parse_iso(raw: str):
        from datetime import datetime

        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
