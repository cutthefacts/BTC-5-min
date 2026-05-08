from __future__ import annotations

from app.config import Settings


def adaptive_max_reasonable_edge(
    expected_edge: float,
    quote_age_ms: float,
    repricing_lag_ms: float,
    volatility_bps: float,
    regime: str,
    settings: Settings | None = None,
) -> float:
    settings = settings or Settings()
    cap = settings.max_reasonable_edge
    if quote_age_ms > settings.max_quote_age_ms_filter:
        cap *= 0.65
    if repricing_lag_ms > settings.max_repricing_lag_ms:
        cap *= 0.75
    if volatility_bps > settings.max_volatility_bps * 0.8:
        cap *= 0.80
    if regime in {"high_volatility", "post_spike"}:
        cap *= 0.70
    if regime == "compression":
        cap *= 1.05
    return max(0.08, min(settings.max_reasonable_edge, cap, expected_edge + 1.0))


def edge_quality_score(
    expected_edge: float,
    realized_edge: float | None,
    quote_age_ms: float,
    repricing_lag_ms: float,
    regime_bad_score: float,
    historical_bucket_pf: float | None,
    settings: Settings | None = None,
) -> float:
    settings = settings or Settings()
    moderate_edge = 1.0 - min(
        1.0,
        abs(expected_edge - 0.14) / max(settings.max_reasonable_edge, 0.01),
    )
    realized = 0.5
    if realized_edge is not None:
        realized = max(0.0, min(1.0, realized_edge / max(expected_edge, 0.01)))
    freshness = 1.0 - min(1.0, quote_age_ms / max(settings.max_quote_age_ms, 1.0))
    repricing = 1.0 - min(1.0, abs(repricing_lag_ms - 750.0) / 2500.0)
    regime = 1.0 - max(0.0, min(1.0, regime_bad_score))
    bucket = 0.5 if historical_bucket_pf is None else min(1.0, historical_bucket_pf / 2.0)
    return max(
        0.0,
        min(
            1.0,
            0.25 * moderate_edge
            + 0.20 * realized
            + 0.20 * freshness
            + 0.15 * repricing
            + 0.10 * regime
            + 0.10 * bucket,
        ),
    )


def extreme_edge_reason(
    expected_edge: float,
    quote_age_ms: float,
    repricing_lag_ms: float,
    volatility_bps: float,
    regime: str,
    settings: Settings | None = None,
) -> str | None:
    cap = adaptive_max_reasonable_edge(
        expected_edge,
        quote_age_ms,
        repricing_lag_ms,
        volatility_bps,
        regime,
        settings,
    )
    if expected_edge > cap:
        return "extreme_edge_suspect"
    return None
