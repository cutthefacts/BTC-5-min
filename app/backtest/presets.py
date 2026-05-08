from __future__ import annotations

from dataclasses import dataclass

from app.config import get_settings


@dataclass(frozen=True, slots=True)
class ResearchPreset:
    name: str
    min_score: float
    min_confidence: float
    min_edge: float
    side_mode: str = "BOTH"
    allowed_entry_windows: str | None = None
    blocked_entry_windows: str | None = None
    min_quote_age_ms: float | None = None
    max_quote_age_ms: float | None = None
    min_repricing_lag_ms: float | None = None
    max_repricing_lag_ms: float | None = None
    avoid_liquidity_sweep: bool = True
    soft_filters: bool = False


PRESET_NAMES = (
    "strict",
    "balanced",
    "exploratory",
    "down_only",
    "best_window_120_180",
    "candidate_v1",
)


def load_preset(name: str | None) -> ResearchPreset:
    settings = get_settings()
    presets = {
        "strict": ResearchPreset(
            name="strict",
            min_score=settings.min_inefficiency_score,
            min_confidence=settings.min_confidence,
            min_edge=settings.min_edge,
            allowed_entry_windows=settings.down_allowed_entry_windows,
            blocked_entry_windows=settings.blocked_entry_windows_seconds_to_close,
            min_quote_age_ms=settings.min_quote_age_ms,
            max_quote_age_ms=settings.max_quote_age_ms_filter,
            min_repricing_lag_ms=settings.min_repricing_lag_ms,
            max_repricing_lag_ms=settings.max_repricing_lag_ms,
            avoid_liquidity_sweep=settings.avoid_liquidity_sweep,
        ),
        "balanced": ResearchPreset(
            name="balanced",
            min_score=0.40,
            min_confidence=0.55,
            min_edge=0.035,
            allowed_entry_windows="75-195",
            blocked_entry_windows="270-300",
            min_quote_age_ms=0,
            max_quote_age_ms=1500,
            min_repricing_lag_ms=100,
            max_repricing_lag_ms=1000,
            avoid_liquidity_sweep=True,
        ),
        "exploratory": ResearchPreset(
            name="exploratory",
            min_score=0.30,
            min_confidence=0.45,
            min_edge=0.02,
            allowed_entry_windows="45-240",
            blocked_entry_windows="",
            min_quote_age_ms=0,
            max_quote_age_ms=2500,
            min_repricing_lag_ms=0,
            max_repricing_lag_ms=1500,
            avoid_liquidity_sweep=False,
            soft_filters=True,
        ),
        "down_only": ResearchPreset(
            name="down_only",
            min_score=settings.down_min_inefficiency_score,
            min_confidence=settings.down_min_confidence,
            min_edge=settings.down_min_edge,
            side_mode="DOWN_ONLY",
            allowed_entry_windows=settings.down_allowed_entry_windows,
            blocked_entry_windows=settings.blocked_entry_windows_seconds_to_close,
            min_quote_age_ms=settings.min_quote_age_ms,
            max_quote_age_ms=settings.max_quote_age_ms_filter,
            min_repricing_lag_ms=settings.min_repricing_lag_ms,
            max_repricing_lag_ms=settings.max_repricing_lag_ms,
            avoid_liquidity_sweep=settings.avoid_liquidity_sweep,
        ),
        "best_window_120_180": ResearchPreset(
            name="best_window_120_180",
            min_score=0.40,
            min_confidence=0.55,
            min_edge=0.035,
            allowed_entry_windows="120-180",
            blocked_entry_windows=settings.blocked_entry_windows_seconds_to_close,
            min_quote_age_ms=0,
            max_quote_age_ms=1500,
            min_repricing_lag_ms=100,
            max_repricing_lag_ms=1000,
            avoid_liquidity_sweep=True,
        ),
        "candidate_v1": ResearchPreset(
            name="candidate_v1",
            min_score=0.40,
            min_confidence=0.55,
            min_edge=0.08,
            side_mode="BOTH",
            allowed_entry_windows="120-180",
            blocked_entry_windows=settings.blocked_entry_windows_seconds_to_close,
            min_quote_age_ms=500,
            max_quote_age_ms=1000,
            min_repricing_lag_ms=500,
            max_repricing_lag_ms=1000,
            avoid_liquidity_sweep=True,
        ),
    }
    key = name or "strict"
    if key not in presets:
        raise ValueError(f"unknown preset: {key}")
    return presets[key]
