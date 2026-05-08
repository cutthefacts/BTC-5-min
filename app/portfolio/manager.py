from __future__ import annotations

from datetime import UTC, datetime

from app.models import Order, PortfolioStats, Position, Trade


class Portfolio:
    def __init__(self, starting_balance: float) -> None:
        self.starting_balance = starting_balance
        self.cash = starting_balance
        self.realized_pnl = 0.0
        self.positions: dict[tuple[str, str], Position] = {}
        self.trades: list[Trade] = []
        self.closed_pnls: list[float] = []
        self.equity_high_water = starting_balance

    def apply_fill(self, order: Order, trade: Trade) -> None:
        cost = trade.price * trade.size + trade.fee
        if cost > self.cash + 1e-9:
            raise ValueError("insufficient paper cash")
        self.cash -= cost
        key = (trade.market_id, trade.token_id)
        position = self.positions.get(key)
        if position is None:
            position = Position(trade.market_id, trade.token_id, trade.outcome)
            self.positions[key] = position
        new_size = position.size + trade.size
        position.avg_entry = (
            ((position.avg_entry * position.size) + (trade.price * trade.size)) / new_size
            if new_size
            else 0.0
        )
        position.size = new_size
        position.fees += trade.fee
        self.trades.append(trade)
        self._mark_high_water()

    def settle_market(self, market_id: str, winning_token_id: str) -> None:
        market_pnl = 0.0
        for key, position in list(self.positions.items()):
            if position.market_id != market_id:
                continue
            payout = position.size if position.token_id == winning_token_id else 0.0
            pnl = payout - position.cost_basis
            market_pnl += pnl
            self.cash += payout
            del self.positions[key]
        if market_pnl:
            self.realized_pnl += market_pnl
            self.closed_pnls.append(market_pnl)
        self._mark_high_water()

    def equity(self, marks: dict[str, float] | None = None) -> float:
        marks = marks or {}
        value = self.cash
        for position in self.positions.values():
            value += position.size * marks.get(position.token_id, position.avg_entry)
        return value

    def market_exposure(self, market_id: str) -> float:
        return sum(p.cost_basis for p in self.positions.values() if p.market_id == market_id)

    def daily_realized_pnl(self) -> float:
        # MVP keeps one in-memory session day; persisted reports are in storage.
        return self.realized_pnl

    def stats(self) -> PortfolioStats:
        wins = sum(1 for pnl in self.closed_pnls if pnl > 0)
        trades = len(self.closed_pnls)
        consecutive_losses = 0
        for pnl in reversed(self.closed_pnls):
            if pnl < 0:
                consecutive_losses += 1
            else:
                break
        eq = self.equity()
        self.equity_high_water = max(self.equity_high_water, eq)
        drawdown = (
            0.0
            if self.equity_high_water <= 0
            else (self.equity_high_water - eq) / self.equity_high_water
        )
        return PortfolioStats(
            balance=self.cash,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=eq - self.cash,
            winrate=wins / trades if trades else 0.0,
            trades=len(self.trades),
            consecutive_losses=consecutive_losses,
            max_drawdown_pct=drawdown,
        )

    def _mark_high_water(self) -> None:
        self.equity_high_water = max(self.equity_high_water, self.equity())


def now_utc() -> datetime:
    return datetime.now(UTC)
