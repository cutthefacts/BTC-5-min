from __future__ import annotations

import argparse

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import open_conn, replay_dict, run_replay


def main() -> None:
    parser = argparse.ArgumentParser()
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset("best_window_120_180")
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    print("Best Window Validation")
    print("======================")
    for side in ("UP_ONLY", "DOWN_ONLY", "BOTH"):
        for latency in (50, 100, 250, 500, 1000):
            result = run_replay(conn, preset, filters, latency_ms=latency, side_mode=side)
            print({"side_mode": side, "latency_ms": latency, **replay_dict(result)})


if __name__ == "__main__":
    main()
