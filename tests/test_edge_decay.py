import sqlite3

from app.backtest.edge_decay_report import future_ask


def test_edge_decay_finds_future_ask(tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "test.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.execute("create table orderbooks(timestamp text, token_id text, best_ask real)")
    conn.execute("insert into orderbooks values ('2026-01-01T00:00:00+00:00', 'up', 0.5)")
    conn.execute("insert into orderbooks values ('2026-01-01T00:00:01+00:00', 'up', 0.6)")
    assert future_ask(conn, "up", "2026-01-01T00:00:00+00:00", 500) == 0.6
