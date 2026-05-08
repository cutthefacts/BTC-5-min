from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class GroupMetrics:
    bucket: str
    trades: int
    wins: int
    gross_pnl: float
    fees: float
    net_pnl: float
    avg_pnl: float
    winrate: float
    profit_factor: float | None
    max_drawdown: float
    reliable: bool


def numeric_bucket(value: float | None, step: float, precision: int = 2) -> str:
    if value is None:
        return "missing"
    start = int(value // step) * step
    end = start + step
    return f"{start:.{precision}f}-{end:.{precision}f}"


def bool_bucket(value: bool | int | None) -> str:
    if value is None:
        return "missing"
    return "true" if bool(value) else "false"


def side_bucket(value: str | None) -> str:
    return value or "missing"


def profit_factor(pnls: list[float]) -> float | None:
    gross_profit = sum(pnl for pnl in pnls if pnl > 0)
    gross_loss = -sum(pnl for pnl in pnls if pnl < 0)
    if gross_loss <= 0:
        return None
    return gross_profit / gross_loss


def max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    high_water = 0.0
    drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        high_water = max(high_water, equity)
        drawdown = max(drawdown, high_water - equity)
    return drawdown


def summarize_bucket(
    bucket_name: str,
    pnls: list[float],
    fees: list[float] | None = None,
    min_trades: int = 50,
) -> GroupMetrics:
    fees = fees or [0.0 for _ in pnls]
    trades = len(pnls)
    wins = sum(1 for pnl in pnls if pnl > 0)
    net = sum(pnls)
    return GroupMetrics(
        bucket=bucket_name,
        trades=trades,
        wins=wins,
        gross_pnl=sum(pnl + fee for pnl, fee in zip(pnls, fees, strict=False)),
        fees=sum(fees),
        net_pnl=net,
        avg_pnl=net / trades if trades else 0.0,
        winrate=wins / trades if trades else 0.0,
        profit_factor=profit_factor(pnls),
        max_drawdown=max_drawdown(pnls),
        reliable=trades >= min_trades,
    )


def metric_dict(metrics: GroupMetrics) -> dict[str, float | int | str | bool | None]:
    return {
        "bucket": metrics.bucket,
        "trades": metrics.trades,
        "winrate": round(metrics.winrate, 4),
        "gross_pnl": round(metrics.gross_pnl, 4),
        "fees": round(metrics.fees, 4),
        "net_pnl": round(metrics.net_pnl, 4),
        "avg_pnl": round(metrics.avg_pnl, 4),
        "profit_factor": round(metrics.profit_factor, 4)
        if metrics.profit_factor is not None
        else None,
        "max_drawdown": round(metrics.max_drawdown, 4),
        "reliable": metrics.reliable,
    }


def theoretical_trade_pnl(
    token_id: str,
    winning_token_id: str,
    price: float,
    size: float,
    fee: float,
) -> float:
    payout = size if token_id == winning_token_id else 0.0
    return payout - price * size - fee
