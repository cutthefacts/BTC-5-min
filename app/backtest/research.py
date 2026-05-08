from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import datetime, timedelta

from app.backtest.analytics import metric_dict, numeric_bucket, summarize_bucket
from app.backtest.diagnostics import apply_hard_filters, complete_candidates, theoretical_pnl
from app.backtest.filters import TimeFilters
from app.backtest.presets import ResearchPreset
from app.backtest.replay import ReplayResult, TickByTickReplay
from app.config import get_settings
from app.storage.sqlite import SQLiteStore
from app.strategy.regime import MarketRegimeEngine


def open_conn() -> sqlite3.Connection:
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    conn.row_factory = sqlite3.Row
    return conn


def run_replay(
    conn: sqlite3.Connection,
    preset: ResearchPreset,
    filters: TimeFilters,
    latency_ms: int = 250,
    side_mode: str | None = None,
    execution_mode: str = "taker",
) -> ReplayResult:
    settings = get_settings()
    return TickByTickReplay(conn).run(
        latency_ms=latency_ms,
        slippage_bps=settings.slippage_bps,
        min_score=preset.min_score,
        min_confidence=preset.min_confidence,
        min_edge=preset.min_edge,
        include_hold_candidates=True,
        execution_mode=execution_mode,
        side_mode=side_mode or preset.side_mode,
        avoid_liquidity_sweep=preset.avoid_liquidity_sweep,
        allowed_entry_windows=preset.allowed_entry_windows,
        blocked_entry_windows=preset.blocked_entry_windows,
        min_quote_age_ms=preset.min_quote_age_ms,
        max_quote_age_ms=preset.max_quote_age_ms,
        min_repricing_lag_ms=preset.min_repricing_lag_ms,
        max_repricing_lag_ms=preset.max_repricing_lag_ms,
        time_filters=filters,
        soft_filters=preset.soft_filters,
    )


def candidate_settings_overrides() -> dict:
    return {
        "min_edge": 0.08,
        "min_confidence": 0.55,
        "min_inefficiency_score": 0.40,
        "up_min_edge": 0.08,
        "up_min_confidence": 0.55,
        "up_min_inefficiency_score": 0.40,
        "up_allowed_entry_windows": "90-180",
        "down_min_edge": 0.10,
        "down_min_confidence": 0.65,
        "down_min_inefficiency_score": 0.45,
        "down_allowed_entry_windows": "120-180",
        "min_quote_age_ms": 500,
        "max_quote_age_ms_filter": 1000,
        "min_repricing_lag_ms": 500,
        "max_repricing_lag_ms": 1000,
        "avoid_liquidity_sweep": True,
        "up_market_probability_min": 0.40,
        "up_market_probability_max": 0.70,
        "down_market_probability_min": 0.40,
        "down_market_probability_max": 0.60,
    }


def replay_dict(result: ReplayResult) -> dict:
    data = asdict(result)
    data["profit_factor"] = round(result.profit_factor, 4) if result.profit_factor else None
    data["net_pnl"] = round(result.net_pnl, 4)
    data["max_drawdown"] = round(result.max_drawdown, 4)
    data["winrate"] = round(result.winrate, 4)
    data["stale_fill_rate"] = round(result.stale_fill_rate, 4)
    data["missed_fill_rate"] = round(result.missed_fill_rate, 4)
    return data


def filtered_candidate_rows(
    conn: sqlite3.Connection,
    preset: ResearchPreset,
    filters: TimeFilters,
) -> list[dict]:
    candidates = complete_candidates(conn, filters)
    kept, _ = apply_hard_filters(candidates, preset, preset.soft_filters)
    rows = []
    for candidate in kept:
        row = dict(candidate.row)
        row["seconds_to_close"] = candidate.seconds_to_close
        row["pnl"] = theoretical_pnl(candidate)
        row["fee"] = 0.0
        rows.append(row)
    return rows


def grouped_metrics(
    rows: list[dict],
    column: str,
    step: float | None = None,
) -> list[dict]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        bucket = str(row.get(column)) if step is None else numeric_bucket(row.get(column), step)
        grouped.setdefault(bucket, []).append(float(row["pnl"]))
    min_trades = get_settings().min_reliable_trades_per_bucket
    return [
        metric_dict(summarize_bucket(bucket, pnls, min_trades=min_trades))
        for bucket, pnls in sorted(grouped.items())
    ]


def classify_research_regime(row: dict) -> str:
    if row.get("regime_source") == "snapshot" and row.get("regime"):
        return str(row["regime"])
    return MarketRegimeEngine().classify_values(
        volatility_bps=float(row.get("volatility_bps") or 0.0),
        drift_60s_bps=float(row.get("drift_60s_bps") or 0.0),
        drift_180s_bps=float(row.get("drift_180s_bps") or 0.0),
        distance_bps=float(row.get("distance_bps") or 0.0),
        imbalance=float(row.get("imbalance_ratio") or 0.0),
        repricing_lag_ms=float(row.get("repricing_lag_ms") or 0.0),
        quote_age_ms=float(row.get("quote_age_ms") or 0.0),
    ).regime


def regime_source(row: dict) -> str:
    return "snapshot" if row.get("regime_source") == "snapshot" else "proxy"


def entry_window_bucket(seconds_to_close: float | None, step: int = 30) -> str:
    if seconds_to_close is None:
        return "missing"
    start = int(float(seconds_to_close) // step) * step
    return f"{start}-{start + step}"


def time_bounds(conn: sqlite3.Connection, filters: TimeFilters) -> tuple[datetime, datetime] | None:
    clauses = [
        "inefficiency_score is not null",
        "confidence is not null",
        "quote_age_ms is not null",
        "repricing_lag_ms is not null",
        "imbalance_ratio is not null",
        "edge is not null",
    ]
    params = []
    if filters.from_timestamp:
        clauses.append("timestamp >= ?")
        params.append(filters.from_timestamp)
    if filters.to_timestamp:
        clauses.append("timestamp <= ?")
        params.append(filters.to_timestamp)
    if filters.regime_source == "snapshot":
        clauses.append("regime_source = 'snapshot'")
    elif filters.regime_source == "proxy":
        clauses.append("(regime_source is null or regime_source = 'proxy')")
    if filters.strategy_name:
        clauses.append("coalesce(strategy_name, 'baseline') = ?")
        params.append(filters.strategy_name)
    if filters.strategy_version:
        clauses.append("strategy_version = ?")
        params.append(filters.strategy_version)
    if filters.feature_schema_version is not None:
        clauses.append("feature_schema_version = ?")
        params.append(filters.feature_schema_version)
    if filters.forward_only:
        clauses.extend(
            [
                "data_collection_started_at is not null",
                "regime_source = 'snapshot'",
                "feature_schema_version is not null",
            ]
        )
    where = " and ".join(clauses)
    row = conn.execute(
        f"select min(timestamp) as start, max(timestamp) as end from signals where {where}",
        params,
    ).fetchone()
    if not row or not row["start"] or not row["end"]:
        return None
    return parse_dt(row["start"]), parse_dt(row["end"])


def parse_dt(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def iter_time_windows(start: datetime, end: datetime, hours: int):
    cursor = start
    delta = timedelta(hours=hours)
    while cursor < end:
        right = min(end, cursor + delta)
        yield cursor, right
        cursor = right
