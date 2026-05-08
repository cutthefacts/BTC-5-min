from __future__ import annotations

import argparse
from dataclasses import replace

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import open_conn, replay_dict, run_replay

QUOTE_RANGES = [(0, 500), (0, 750), (0, 1000), (0, 1250), (0, 1500), (250, 1000), (500, 1500)]
LAG_RANGES = [(0, 500), (100, 750), (250, 750), (250, 1000), (500, 1500), (750, 2000)]
WINDOWS = ["90-180", "120-180", "120-210", "90-210", "120-180"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="balanced")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    base = load_preset(args.preset)
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    print("Filter Range Optimizer")
    print("======================")
    for quote_range in QUOTE_RANGES:
        for lag_range in LAG_RANGES:
            for windows in WINDOWS:
                preset = replace(
                    base,
                    allowed_entry_windows=windows,
                    min_quote_age_ms=quote_range[0],
                    max_quote_age_ms=quote_range[1],
                    min_repricing_lag_ms=lag_range[0],
                    max_repricing_lag_ms=lag_range[1],
                )
                for latency in (50, 100, 250, 500):
                    result = run_replay(conn, preset, filters, latency_ms=latency)
                    accepted = (
                        result.trades >= 40
                        and (result.profit_factor or 0) >= 1.25
                        and result.stale_fill_rate <= 0.15
                    )
                    print(
                        {
                            "quote_age_range": quote_range,
                            "repricing_lag_range": lag_range,
                            "entry_windows": windows,
                            "latency_ms": latency,
                            "accepted": accepted,
                            **replay_dict(result),
                        }
                    )


if __name__ == "__main__":
    main()
