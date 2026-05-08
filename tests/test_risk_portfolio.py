from app.config import Settings
from app.models import Order, Outcome, Position, Side, Signal, SignalAction, Trade
from app.portfolio.manager import Portfolio
from app.risk.manager import RiskManager


def signal() -> Signal:
    return Signal(
        market_id="m1",
        action=SignalAction.BUY_UP,
        outcome=Outcome.UP,
        expected_probability=0.6,
        market_probability=0.52,
        edge=0.08,
        strength=10,
        reason="edge accepted",
    )


def test_risk_sizes_trade_by_balance_pct() -> None:
    settings = Settings(max_trade_balance_pct=0.02, max_market_balance_pct=0.05)
    portfolio = Portfolio(1000)
    decision = RiskManager(settings).evaluate(signal(), portfolio)
    assert decision.allowed
    assert decision.size_usd == 20


def test_kill_switch_blocks() -> None:
    risk = RiskManager(Settings())
    risk.kill()
    decision = risk.evaluate(signal(), Portfolio(1000))
    assert not decision.allowed
    assert decision.reason == "kill-switch"


def test_risk_blocks_max_trades_per_market() -> None:
    settings = Settings(max_trades_per_market=1, market_trade_cooldown_seconds=0)
    portfolio = Portfolio(1000)
    trade = Trade(
        order_id="o1",
        market_id="m1",
        token_id="up",
        outcome=Outcome.UP,
        price=0.5,
        size=1,
        fee=0,
    )
    order = Order("o1", "m1", "up", Outcome.UP, Side.BUY, 0.5, 1, "filled")
    portfolio.apply_fill(order, trade)
    decision = RiskManager(settings).evaluate(signal(), portfolio)
    assert not decision.allowed
    assert decision.reason == "max trades per market"


def test_risk_blocks_opposite_position() -> None:
    settings = Settings(max_trades_per_market=5)
    portfolio = Portfolio(1000)
    portfolio.positions[("m1", "down")] = Position("m1", "down", Outcome.DOWN, size=1)
    decision = RiskManager(settings).evaluate(signal(), portfolio)
    assert not decision.allowed
    assert decision.reason == "opposite position"
