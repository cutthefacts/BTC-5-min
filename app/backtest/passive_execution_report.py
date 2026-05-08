from __future__ import annotations

import argparse

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import open_conn, replay_dict, run_replay


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="balanced")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    print("Passive Execution Report")
    print("========================")
    for mode in ("taker", "maker", "hybrid"):
        result = run_replay(conn, preset, filters, execution_mode=mode)
        print(
            {
                "execution_mode": mode,
                "fill_probability": round(1.0 - result.missed_fill_rate, 4),
                "spread_capture_model": mode in {"maker", "hybrid"},
                **replay_dict(result),
            }
        )


if __name__ == "__main__":
    main()
