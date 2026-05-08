from __future__ import annotations

import sqlite3

from app.backtest.research import open_conn


def main() -> None:
    conn = open_conn()
    conn.row_factory = sqlite3.Row
    print("Settlement Source Report")
    print("========================")
    for row in conn.execute(
        """
        select settlement_source, count(*) as markets,
               sum(case when abs(final_price - price_to_beat) / price_to_beat * 10000 <= 5
                        then 1 else 0 end) as settlement_sensitive
        from results
        group by settlement_source
        """
    ):
        print(dict(row))
    print("\nclose_to_price_to_beat")
    for row in conn.execute(
        """
        select market_id, settled_at, settlement_source, price_to_beat, final_price,
               round(abs(final_price - price_to_beat) / price_to_beat * 10000, 4) as distance_bps,
               winning_outcome
        from results
        where price_to_beat > 0
          and abs(final_price - price_to_beat) / price_to_beat * 10000 <= 5
        order by settled_at desc
        limit 25
        """
    ):
        print({**dict(row), "settlement_sensitive": True})


if __name__ == "__main__":
    main()
