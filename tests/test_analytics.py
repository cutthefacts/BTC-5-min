from app.backtest.analytics import max_drawdown, numeric_bucket, profit_factor


def test_bucket_grouping() -> None:
    assert numeric_bucket(0.27, 0.10) == "0.20-0.30"
    assert numeric_bucket(None, 0.10) == "missing"


def test_profit_factor_and_drawdown() -> None:
    assert profit_factor([2, -1, 3]) == 5
    assert max_drawdown([2, -1, -3, 1]) == 4
