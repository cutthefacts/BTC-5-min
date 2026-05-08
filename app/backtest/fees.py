from __future__ import annotations


def net_pnl_after_costs(
    gross_pnl: float,
    fees: float,
    slippage: float,
) -> float:
    return gross_pnl - fees - slippage
