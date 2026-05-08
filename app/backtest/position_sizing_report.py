from __future__ import annotations

import argparse
import math

from app.backtest.analytics import max_drawdown, profit_factor
from app.backtest.filters import TimeFilters, add_common_report_args
from app.backtest.presets import load_preset
from app.backtest.research import filtered_candidate_rows, open_conn


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="balanced")
    add_common_report_args(parser)
    args = parser.parse_args()
    conn = open_conn()
    preset = load_preset(args.preset)
    rows = filtered_candidate_rows(
        conn,
        preset,
        TimeFilters(args.only_complete_microstructure, args.from_timestamp, args.to_timestamp),
    )
    print("Position Sizing Report")
    print("======================")
    for mode in ("fixed", "volatility_adjusted", "edge_weighted", "drawdown_capped"):
        pnls = simulate(rows, mode)
        print(
            {
                "mode": mode,
                "trades": len(pnls),
                "net_pnl": round(sum(pnls), 4),
                "max_drawdown": round(max_drawdown(pnls), 4),
                "profit_factor": round(profit_factor(pnls) or 0, 4),
                "risk_of_ruin_approx": round(risk_of_ruin(pnls), 4),
            }
        )


def simulate(rows: list[dict], mode: str) -> list[float]:
    out = []
    equity = 1000.0
    high = equity
    for row in rows:
        size = 1.0
        if mode == "edge_weighted":
            size = max(0.25, min(2.0, float(row.get("edge") or 0) / 0.05))
        elif mode == "volatility_adjusted":
            size = max(0.25, min(1.5, 1.0 - abs(float(row.get("imbalance_ratio") or 0)) * 0.25))
        elif mode == "drawdown_capped":
            dd = high - equity
            size = 0.5 if dd > 30 else 1.0
        pnl = float(row["pnl"]) * size
        equity += pnl
        high = max(high, equity)
        out.append(pnl)
    return out


def risk_of_ruin(pnls: list[float]) -> float:
    if not pnls:
        return 1.0
    wins = sum(1 for pnl in pnls if pnl > 0)
    winrate = wins / len(pnls)
    edge = sum(pnls) / max(len(pnls), 1)
    if edge <= 0:
        return 1.0
    return math.exp(-2 * edge * winrate)


if __name__ == "__main__":
    main()
