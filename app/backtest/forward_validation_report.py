from __future__ import annotations

import argparse

from app.backtest.filters import add_common_report_args, filters_from_args
from app.backtest.presets import load_preset
from app.backtest.research import open_conn, replay_dict, run_replay
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="candidate_v1")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = filters_from_args(args)
    replay = run_replay(conn, preset, filters)
    store = SQLiteStore(get_settings().database_url)
    store.init()
    forward = store.strategy_trade_summary(preset.name)
    print("Forward Validation Report")
    print("=========================")
    print({"replay": replay_dict(replay)})
    print({"forward_paper": forward})
    forward_trades = float(forward["trades"] or 0)
    print(
        {
            "degradation_replay_to_forward": None
            if forward_trades <= 0
            else "requires settled strategy-level pnl",
            "regime_difference": "use regime_report on same timestamp window",
            "side_difference": "use side_performance_report on same timestamp window",
            "fill_difference": {
                "replay_stale_fill_rate": round(replay.stale_fill_rate, 4),
                "forward_stale_fill_rate": round(float(forward["stale_fill_rate"] or 0), 4),
            },
        }
    )


if __name__ == "__main__":
    main()
