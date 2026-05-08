from __future__ import annotations

import argparse

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import iter_time_windows, open_conn, replay_dict, run_replay, time_bounds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="balanced")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    bounds = time_bounds(conn, filters)
    print("Walk Forward")
    print("============")
    if bounds is None:
        print({"error": "no complete microstructure data"})
        return
    start, end = bounds
    midpoint = start + (end - start) / 2
    train = run_replay(conn, preset, TimeFilters(True, start.isoformat(), midpoint.isoformat()))
    validation = run_replay(conn, preset, TimeFilters(True, midpoint.isoformat(), end.isoformat()))
    train_pf = train.profit_factor or 0.0
    val_pf = validation.profit_factor or 0.0
    degradation = (train_pf - val_pf) / train_pf if train_pf else 0.0
    print(
        {
            "split": "first_50_vs_second_50",
            "train": replay_dict(train),
            "validation": replay_dict(validation),
            "degradation": round(degradation, 4),
            "stability_score": round(max(0.0, 1.0 - degradation), 4),
        }
    )
    print({"degradation_driver_hint": "inspect side/window/regime sections below"})
    for side in ("UP_ONLY", "DOWN_ONLY"):
        side_train = run_replay(
            conn, preset, TimeFilters(True, start.isoformat(), midpoint.isoformat()), side_mode=side
        )
        side_val = run_replay(
            conn, preset, TimeFilters(True, midpoint.isoformat(), end.isoformat()), side_mode=side
        )
        print({"side": side, "train": replay_dict(side_train), "validation": replay_dict(side_val)})
    for hours in (3, 6, 24):
        print(f"\nrolling_{hours}h")
        for left, right in iter_time_windows(start, end, hours):
            result = run_replay(
                conn,
                preset,
                TimeFilters(True, left.isoformat(), right.isoformat()),
            )
            if result.trades:
                print({"from": left.isoformat(), "to": right.isoformat(), **replay_dict(result)})


if __name__ == "__main__":
    main()
