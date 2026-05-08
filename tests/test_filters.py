from app.backtest.filters import allowed_by_windows, parse_windows


def test_entry_window_parser_and_blocking() -> None:
    assert parse_windows("90-180,240-260") == [(90.0, 180.0), (240.0, 260.0)]
    assert allowed_by_windows(120, "90-180", "195-225")
    assert not allowed_by_windows(210, None, "195-225,270-300")
    assert not allowed_by_windows(80, "90-180", "")
