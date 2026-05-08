from __future__ import annotations

import argparse
from dataclasses import replace

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import open_conn, replay_dict, run_replay


def main() -> None:
    parser = argparse.ArgumentParser()
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    print("Stale Trap Report")
    print("=================")
    for preset_name in ("strict", "balanced", "exploratory", "best_window_120_180"):
        preset = load_preset(preset_name)
        print({"preset": preset_name, **replay_dict(run_replay(conn, preset, filters))})

    print("\nquote_age thresholds")
    for quote_range in ((0, 500), (0, 750), (0, 1000), (0, 1250), (0, 1500), (0, 2500)):
        preset = load_preset("balanced")
        preset = replace(preset, max_quote_age_ms=quote_range[1])
        result = run_replay(conn, preset, filters)
        print({"quote_age_range": quote_range, **replay_dict(result)})

    print("\nrepricing_lag thresholds")
    for lag_range in ((0, 500), (100, 750), (250, 750), (250, 1000), (500, 1500)):
        preset = load_preset("balanced")
        preset = replace(
            preset,
            min_repricing_lag_ms=lag_range[0],
            max_repricing_lag_ms=lag_range[1],
        )
        result = run_replay(conn, preset, filters)
        print({"repricing_lag_range": lag_range, **replay_dict(result)})


if __name__ == "__main__":
    main()
