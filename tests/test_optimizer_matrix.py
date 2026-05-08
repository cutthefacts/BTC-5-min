from app.backtest.optimize import reliability_reject_reasons, row_for
from app.backtest.replay import ReplayResult
from app.backtest.replay_matrix import WINDOW_MODES
from app.config import get_settings


def result_with_trades(trades: int) -> ReplayResult:
    return ReplayResult(
        trades=trades,
        winrate=0.6,
        net_pnl=float(trades),
        profit_factor=1.2,
        max_drawdown=10.0,
        average_edge=0.1,
        average_realized_edge=0.09,
        stale_fill_rate=0.1,
        missed_fill_rate=0.1,
    )


def test_optimizer_minimum_viable_edge_trade_target() -> None:
    settings = get_settings()
    config = {"SIDE_MODE": "DOWN_ONLY"}
    assert row_for(config, result_with_trades(30), settings, min_trades=30)["reliable"] == "True"
    assert row_for(config, result_with_trades(29), settings, min_trades=30)["reliable"] == "False"


def test_optimizer_explains_stale_rejection() -> None:
    settings = get_settings()
    result = result_with_trades(100)
    result.stale_fill_rate = 0.7
    assert "stale_fill_rate_too_high" in reliability_reject_reasons(result, settings, 30)


def test_replay_matrix_has_best_candidate_window_mode() -> None:
    assert WINDOW_MODES["only_best_candidate_windows"] == "120-180"
