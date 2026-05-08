from app.data.binance import BinanceBtcWebSocket


def test_parse_binance_trade() -> None:
    feed = BinanceBtcWebSocket("wss://example.test")
    tick = feed._parse_binance_trade('{"p":"100.5","T":1710000000000}')

    assert tick.price == 100.5
    assert tick.timestamp.year == 2024


def test_parse_coinbase_ticker() -> None:
    feed = BinanceBtcWebSocket("wss://example.test", "wss://fallback.test")
    tick = feed._parse_coinbase_ticker(
        '{"type":"ticker","price":"100.5","time":"2026-05-08T22:10:17.000000Z"}'
    )

    assert tick is not None
    assert tick.price == 100.5
    assert tick.timestamp.isoformat() == "2026-05-08T22:10:17+00:00"


def test_parse_coinbase_ignores_subscription_events() -> None:
    feed = BinanceBtcWebSocket("wss://example.test", "wss://fallback.test")

    assert feed._parse_coinbase_ticker('{"type":"subscriptions"}') is None
