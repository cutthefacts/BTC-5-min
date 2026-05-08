from __future__ import annotations

import argparse

from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import open_conn, replay_dict, run_replay


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="candidate_v1")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    filters = TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp)
    print("Side Performance")
    print("================")
    for side in ("UP_ONLY", "DOWN_ONLY", "BOTH"):
        print({"side_mode": side, **replay_dict(run_replay(conn, preset, filters, side_mode=side))})


if __name__ == "__main__":
    main()
