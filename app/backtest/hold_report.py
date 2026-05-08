from __future__ import annotations

import argparse
import sqlite3

from app.backtest.filters import TimeFilters, add_common_report_args, signal_time_filter_sql
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    add_common_report_args(parser)
    args = parser.parse_args()
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    conn.row_factory = sqlite3.Row

    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    filter_sql, params = signal_time_filter_sql("s", filters)
    print("Hold Reasons")
    print("============")
    for row in conn.execute(
        """
        select reason, count(*) as n,
               round(avg(inefficiency_score), 4) as avg_score,
               round(avg(confidence), 4) as avg_confidence,
               round(avg(edge), 4) as avg_edge
        from signals
        where action = 'HOLD'
        """.replace("from signals", "from signals s")
        + filter_sql
        + """
        group by reason
        order by n desc
        """,
        params,
    ):
        print(dict(row))

    print("\nHOLD Near Settled Winning Outcome")
    print("=================================")
    filter_sql, params = signal_time_filter_sql("s", filters)
    for row in conn.execute(
        """
        select s.reason, s.outcome, r.winning_outcome,
               count(*) as n,
               round(avg(s.edge), 4) as avg_edge,
               round(avg(s.inefficiency_score), 4) as avg_score
        from signals s
        join results r on r.market_id = s.market_id
        where s.action = 'HOLD'
          and s.outcome = r.winning_outcome
          and r.trade_count >= 0
        """ + filter_sql + """
        group by s.reason, s.outcome, r.winning_outcome
        order by n desc
        """,
        params,
    ):
        print(dict(row))


if __name__ == "__main__":
    main()
