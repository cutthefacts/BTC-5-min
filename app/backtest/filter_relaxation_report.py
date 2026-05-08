from __future__ import annotations

import argparse
import sqlite3

from app.backtest.diagnostics import apply_hard_filters, complete_candidates
from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="strict")
    parser.add_argument("--soft-filters", action="store_true")
    add_common_report_args(parser)
    args = parser.parse_args()
    settings = get_settings()
    SQLiteStore(settings.database_url).init()
    conn = sqlite3.connect(settings.database_url.removeprefix("sqlite:///"))
    filters = TimeFilters(
        only_complete_microstructure=args.only_complete_microstructure,
        from_timestamp=args.from_timestamp,
        to_timestamp=args.to_timestamp,
    )
    candidates = complete_candidates(conn, filters)
    preset = load_preset(args.preset)
    kept, steps = apply_hard_filters(candidates, preset, args.soft_filters)
    print("Filter Relaxation Report")
    print("========================")
    print(
        {
            "preset": preset.name,
            "total_candidates": len(candidates),
            "final_candidates": len(kept),
        }
    )
    for step in steps:
        print(
            {
                "filter": step.reason,
                "candidates_before": step.before,
                "candidates_after": step.after,
                "rejected_count": step.rejected,
                "rejection_pct": round(step.rejection_pct, 4),
                "net_pnl_rejected_candidates": round(step.rejected_pnl, 4),
                "reason": step.reason,
            }
        )
    print({"next_step": "try --preset balanced, then --preset exploratory --soft-filters"})


if __name__ == "__main__":
    main()
