from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from app.backtest.filters import TimeFilters, allowed_by_windows, signal_time_filter_sql
from app.backtest.presets import ResearchPreset
from app.config import get_settings


@dataclass(slots=True)
class Candidate:
    row: sqlite3.Row
    seconds_to_close: float


@dataclass(slots=True)
class FilterStep:
    reason: str
    before: int
    after: int
    rejected: int
    rejection_pct: float
    rejected_pnl: float


def complete_candidates(
    conn: sqlite3.Connection,
    filters: TimeFilters,
) -> list[Candidate]:
    conn.row_factory = sqlite3.Row
    filter_sql, params = signal_time_filter_sql("s", filters)
    rows = conn.execute(
        """
        select s.*, m.up_token_id, m.down_token_id, m.end_time,
               r.winning_outcome, r.winning_token_id,
               (
                   select ob.spread
                   from orderbooks ob
                   where ob.token_id = case
                       when s.outcome = 'UP' then m.up_token_id
                       else m.down_token_id
                   end
                     and ob.timestamp <= s.timestamp
                   order by ob.timestamp desc
                   limit 1
               ) as spread,
               (
                   select ob.liquidity
                   from orderbooks ob
                   where ob.token_id = case
                       when s.outcome = 'UP' then m.up_token_id
                       else m.down_token_id
                   end
                     and ob.timestamp <= s.timestamp
                   order by ob.timestamp desc
                   limit 1
               ) as liquidity,
               (julianday(m.end_time) - julianday(s.timestamp)) * 86400.0 as seconds_to_close
        from signals s
        join markets m on m.condition_id = s.market_id
        join results r on r.market_id = s.market_id
        where s.outcome is not null
        """ + filter_sql + """
        order by s.timestamp
        """,
        params,
    ).fetchall()
    return [
        Candidate(row=row, seconds_to_close=float(row["seconds_to_close"] or 0))
        for row in rows
    ]


def apply_hard_filters(
    candidates: list[Candidate],
    preset: ResearchPreset,
    soft_filters: bool = False,
) -> tuple[list[Candidate], list[FilterStep]]:
    current = candidates
    steps: list[FilterStep] = []
    checks = [
        ("side thresholds", lambda c: side_threshold_ok(c, preset, soft_filters)),
        ("allowed entry windows", lambda c: allowed_window_ok(c, preset, soft_filters)),
        ("blocked entry windows", lambda c: blocked_window_ok(c, preset, soft_filters)),
        ("quote_age range", lambda c: quote_age_ok(c, preset, soft_filters)),
        ("repricing_lag range", lambda c: repricing_lag_ok(c, preset, soft_filters)),
        ("liquidity_sweep avoidance", lambda c: liquidity_sweep_ok(c, preset, soft_filters)),
        ("min_edge", lambda c: adjusted_edge(c, preset, soft_filters) >= preset.min_edge),
        (
            "min_confidence",
            lambda c: soft_filters
            or float(c.row["confidence"] or 0) >= preset.min_confidence,
        ),
        (
            "min_inefficiency_score",
            lambda c: soft_filters
            or float(c.row["inefficiency_score"] or 0) >= preset.min_score,
        ),
        ("max_spread", spread_ok),
        ("min_liquidity", liquidity_ok),
    ]
    for reason, check in checks:
        before = len(current)
        kept = [candidate for candidate in current if check(candidate)]
        rejected = [candidate for candidate in current if not check(candidate)]
        steps.append(
            FilterStep(
                reason=reason,
                before=before,
                after=len(kept),
                rejected=len(rejected),
                rejection_pct=(len(rejected) / before if before else 0.0),
                rejected_pnl=sum(theoretical_pnl(candidate) for candidate in rejected),
            )
        )
        current = kept
    return current, steps


def adjusted_edge(candidate: Candidate, preset: ResearchPreset, soft_filters: bool) -> float:
    edge = float(candidate.row["edge"] or 0)
    if not soft_filters:
        return edge
    penalty = 0.0
    score = float(candidate.row["inefficiency_score"] or 0)
    confidence = float(candidate.row["confidence"] or 0)
    quote_age = float(candidate.row["quote_age_ms"] or 0)
    lag = float(candidate.row["repricing_lag_ms"] or 0)
    imbalance = abs(float(candidate.row["imbalance_ratio"] or 0))
    if score < preset.min_score:
        penalty += min(0.04, (preset.min_score - score) * 0.08)
    if confidence < preset.min_confidence:
        penalty += min(0.04, (preset.min_confidence - confidence) * 0.08)
    if preset.max_quote_age_ms is not None and quote_age > preset.max_quote_age_ms:
        penalty += min(0.04, (quote_age - preset.max_quote_age_ms) / 100_000)
    if preset.min_repricing_lag_ms is not None and lag < preset.min_repricing_lag_ms:
        penalty += 0.015
    if preset.max_repricing_lag_ms is not None and lag > preset.max_repricing_lag_ms:
        penalty += 0.02
    if imbalance < 0.15:
        penalty += 0.015
    if not allowed_by_windows(
        candidate.seconds_to_close,
        preset.allowed_entry_windows,
        preset.blocked_entry_windows,
    ):
        penalty += 0.025
    if bool(candidate.row["liquidity_sweep"]):
        penalty += 0.04
    return edge - penalty


def theoretical_pnl(candidate: Candidate) -> float:
    row = candidate.row
    price = float(row["market_probability"] or 0)
    if price <= 0:
        return 0.0
    token_id = row["up_token_id"] if row["outcome"] == "UP" else row["down_token_id"]
    return (1.0 / price if token_id == row["winning_token_id"] else 0.0) - 1.0


def side_threshold_ok(candidate: Candidate, preset: ResearchPreset, soft_filters: bool) -> bool:
    if preset.side_mode == "UP_ONLY" and candidate.row["outcome"] != "UP":
        return False
    return not (preset.side_mode == "DOWN_ONLY" and candidate.row["outcome"] != "DOWN")


def allowed_window_ok(candidate: Candidate, preset: ResearchPreset, soft_filters: bool) -> bool:
    if soft_filters:
        return True
    allowed = preset.allowed_entry_windows
    return allowed_by_windows(candidate.seconds_to_close, allowed, None)


def blocked_window_ok(candidate: Candidate, preset: ResearchPreset, soft_filters: bool) -> bool:
    if soft_filters:
        return True
    return allowed_by_windows(candidate.seconds_to_close, None, preset.blocked_entry_windows)


def quote_age_ok(candidate: Candidate, preset: ResearchPreset, soft_filters: bool) -> bool:
    if soft_filters:
        return True
    value = float(candidate.row["quote_age_ms"] or 0)
    if preset.min_quote_age_ms is not None and value < preset.min_quote_age_ms:
        return False
    return not (preset.max_quote_age_ms is not None and value > preset.max_quote_age_ms)


def repricing_lag_ok(candidate: Candidate, preset: ResearchPreset, soft_filters: bool) -> bool:
    if soft_filters:
        return True
    value = float(candidate.row["repricing_lag_ms"] or 0)
    if preset.min_repricing_lag_ms is not None and value < preset.min_repricing_lag_ms:
        return False
    return not (preset.max_repricing_lag_ms is not None and value > preset.max_repricing_lag_ms)


def liquidity_sweep_ok(candidate: Candidate, preset: ResearchPreset, soft_filters: bool) -> bool:
    if soft_filters:
        return True
    return not (preset.avoid_liquidity_sweep and bool(candidate.row["liquidity_sweep"]))


def spread_ok(candidate: Candidate) -> bool:
    spread = candidate.row["spread"]
    if spread is None:
        return True
    return float(spread) <= get_settings().max_spread


def liquidity_ok(candidate: Candidate) -> bool:
    liquidity = candidate.row["liquidity"]
    if liquidity is None:
        return True
    return float(liquidity) >= get_settings().min_liquidity_usd
