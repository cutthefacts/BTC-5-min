import sqlite3

from app.backtest.analytics import profit_factor
from app.backtest.fees import net_pnl_after_costs
from app.backtest.position_sizing_report import risk_of_ruin, simulate
from app.backtest.presets import PRESET_NAMES, load_preset
from app.backtest.replay import ReplayResult
from app.backtest.research import iter_time_windows, replay_dict
from app.backtest.research_gate import gate_reasons
from app.config import Settings
from app.strategy.edge_quality import edge_quality_score, extreme_edge_reason
from app.strategy.regime import MarketRegimeEngine
from app.strategy.regime_gate import RegimeGate, RegimePerformance, bad_regime_score


def replay_result(**overrides) -> ReplayResult:
    data = {
        "trades": 100,
        "winrate": 0.6,
        "net_pnl": 10.0,
        "profit_factor": 1.4,
        "max_drawdown": 20.0,
        "average_edge": 0.1,
        "average_realized_edge": 0.09,
        "stale_fill_rate": 0.1,
        "missed_fill_rate": 0.1,
        "stale_reasons": None,
    }
    data.update(overrides)
    return ReplayResult(**data)


def test_best_window_preset() -> None:
    preset = load_preset("best_window_120_180")
    assert preset.allowed_entry_windows == "120-180"
    assert preset.avoid_liquidity_sweep is True


def test_candidate_v1_preset() -> None:
    preset = load_preset("candidate_v1")
    assert preset.allowed_entry_windows == "120-180"
    assert preset.min_quote_age_ms == 500
    assert preset.max_quote_age_ms == 1000
    assert preset.avoid_liquidity_sweep is True


def test_candidate_v1_in_preset_registry() -> None:
    assert "candidate_v1" in PRESET_NAMES


def test_research_gate_rejects_stale_trap() -> None:
    reasons = gate_reasons(replay_result(stale_fill_rate=0.7))
    assert "stale_fill_rate_too_high" in reasons


def test_position_sizing_simulation() -> None:
    rows = [{"pnl": 1.0, "edge": 0.1, "imbalance_ratio": 0.2}, {"pnl": -1.0, "edge": 0.02}]
    fixed = simulate(rows, "fixed")
    edge_weighted = simulate(rows, "edge_weighted")
    assert fixed != edge_weighted
    assert 0 <= risk_of_ruin([1.0, -0.5, 1.0]) <= 1


def test_replay_dict_formats_metrics() -> None:
    data = replay_dict(replay_result())
    assert data["profit_factor"] == 1.4


def test_profit_factor_helper_for_report_inputs() -> None:
    assert profit_factor([1, -0.5]) == 2


def test_net_pnl_after_fees_and_slippage() -> None:
    assert net_pnl_after_costs(gross_pnl=10.0, fees=1.5, slippage=0.5) == 8.0


def test_regime_classification() -> None:
    engine = MarketRegimeEngine()
    high_vol = engine.classify_values(30, 2, 1, 5, 0, 500, 500)
    trending = engine.classify_values(10, 6, 5, 12, 0.2, 500, 500)
    compression = engine.classify_values(5, 1, 1, 8, 0.1, 700, 700)
    assert high_vol.regime == "high_volatility"
    assert trending.regime == "trending_up"
    assert compression.regime == "compression"
    assert compression.confidence > 0


def test_regime_gate_blocks_bad_memory() -> None:
    settings = Settings(regime_gate_enabled=True, regime_gate_min_trades=10, regime_gate_min_pf=1.2)
    perf = RegimePerformance(
        regime="compression",
        side="UP",
        entry_window="120-150",
        trades=20,
        profit_factor=0.7,
        max_drawdown=3,
        stale_fill_rate=0.0,
        missed_fill_rate=0.0,
    )
    decision = RegimeGate(settings).evaluate(perf)
    assert decision.allowed is False
    assert decision.reason == "regime_pf_below_threshold"
    assert bad_regime_score(perf, settings) > 0


def test_edge_quality_and_extreme_edge_filter() -> None:
    settings = Settings(max_reasonable_edge=0.20, max_quote_age_ms_filter=1000)
    quality = edge_quality_score(0.12, 0.11, 500, 700, 0.0, 1.5, settings)
    reason = extreme_edge_reason(0.50, 1500, 1500, 5, "compression", settings)
    assert 0 <= quality <= 1
    assert reason == "extreme_edge_suspect"


def test_walk_forward_window_iterator() -> None:
    start = sqlite3.connect(":memory:").execute("select '2026-01-01T00:00:00+00:00'").fetchone()[0]
    from datetime import datetime

    left = datetime.fromisoformat(start)
    right = datetime.fromisoformat("2026-01-01T06:00:00+00:00")
    assert len(list(iter_time_windows(left, right, 3))) == 2
