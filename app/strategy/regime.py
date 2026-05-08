from __future__ import annotations

from dataclasses import dataclass

from app.models import FeatureSnapshot


@dataclass(frozen=True, slots=True)
class RegimeSnapshot:
    regime: str
    confidence: float
    volatility_bps: float
    drift_60s_bps: float
    drift_180s_bps: float
    distance_bps: float
    imbalance: float
    repricing_lag_ms: float
    quote_age_ms: float
    source: str = "snapshot"


class MarketRegimeEngine:
    def classify_snapshot(self, features: FeatureSnapshot) -> RegimeSnapshot:
        micro = features.microstructure
        return self.classify_values(
            volatility_bps=features.volatility_bps,
            drift_60s_bps=features.momentum_bps.get(60, 0.0),
            drift_180s_bps=features.momentum_bps.get(180, features.momentum_bps.get(60, 0.0)),
            distance_bps=features.distance_bps,
            imbalance=micro.imbalance_ratio if micro else 0.0,
            repricing_lag_ms=micro.repricing_lag_ms if micro else 0.0,
            quote_age_ms=micro.quote_age_ms if micro else 0.0,
        )

    def classify_values(
        self,
        volatility_bps: float,
        drift_60s_bps: float,
        drift_180s_bps: float,
        distance_bps: float,
        imbalance: float,
        repricing_lag_ms: float,
        quote_age_ms: float,
    ) -> RegimeSnapshot:
        abs_drift = max(abs(drift_60s_bps), abs(drift_180s_bps))
        abs_distance = abs(distance_bps)
        confidence = 0.55
        if volatility_bps >= 25 or abs_drift >= 12:
            regime = "high_volatility"
            confidence = min(1.0, max(volatility_bps / 35, abs_drift / 18))
        elif (
            volatility_bps <= 7
            and abs(drift_60s_bps) <= 4
            and 2 <= abs_distance <= 25
            and abs(imbalance) <= 0.85
            and quote_age_ms <= 1_500
        ):
            regime = "compression"
            confidence = 0.75
        elif volatility_bps <= 7 and abs_distance <= 4:
            regime = "low_volatility"
            confidence = 0.65
        elif drift_60s_bps >= 5 and drift_180s_bps >= 4:
            regime = "trending_up"
            confidence = min(1.0, (drift_60s_bps + drift_180s_bps) / 24)
        elif drift_60s_bps <= -5 and drift_180s_bps <= -4:
            regime = "trending_down"
            confidence = min(1.0, abs(drift_60s_bps + drift_180s_bps) / 24)
        elif abs_drift >= 8 and abs_distance <= 8:
            regime = "post_spike"
            confidence = 0.70
        elif drift_60s_bps * drift_180s_bps < 0:
            regime = "choppy"
            confidence = 0.70
        else:
            regime = "choppy"
        return RegimeSnapshot(
            regime=regime,
            confidence=confidence,
            volatility_bps=volatility_bps,
            drift_60s_bps=drift_60s_bps,
            drift_180s_bps=drift_180s_bps,
            distance_bps=distance_bps,
            imbalance=imbalance,
            repricing_lag_ms=repricing_lag_ms,
            quote_age_ms=quote_age_ms,
        )
