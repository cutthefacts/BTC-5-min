from __future__ import annotations

import argparse
import sqlite3

from app.backtest.diagnostics import (
    Candidate,
    apply_hard_filters,
    complete_candidates,
    theoretical_pnl,
)
from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.config import get_settings
from app.storage.sqlite import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="strict")
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
    kept, _ = apply_hard_filters(candidates, preset)
    rejected = [candidate for candidate in candidates if candidate not in kept]
    print("Candidate Signal Report")
    print("=======================")
    print({"preset": preset.name, "candidates": len(candidates), "rejected": len(rejected)})
    print_top("Top rejected by expected_edge", rejected, "edge")
    print_top("Top rejected by inefficiency_score", rejected, "inefficiency_score")
    print_top("Top rejected by confidence", rejected, "confidence")


def print_top(title: str, candidates: list[Candidate], column: str) -> None:
    print(f"\n{title}")
    print("=" * len(title))
    for candidate in sorted(
        candidates,
        key=lambda item: float(item.row[column] or 0),
        reverse=True,
    )[:20]:
        print(candidate_dict(candidate))


def candidate_dict(candidate: Candidate) -> dict:
    row = candidate.row
    return {
        "side": row["outcome"],
        "seconds_to_close": round(candidate.seconds_to_close, 2),
        "inefficiency_score": row["inefficiency_score"],
        "confidence": row["confidence"],
        "expected_edge": row["edge"],
        "quote_age_ms": row["quote_age_ms"],
        "repricing_lag_ms": row["repricing_lag_ms"],
        "spread": row["spread"],
        "liquidity": row["liquidity"],
        "rejection_reason": row["reason"],
        "final_outcome": row["winning_outcome"],
        "theoretical_pnl": round(theoretical_pnl(candidate), 4),
    }


if __name__ == "__main__":
    main()
