from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TimeFilters:
    only_complete_microstructure: bool = False
    from_timestamp: str | None = None
    to_timestamp: str | None = None
    regime_source: str = "all"
    strategy_name: str | None = None
    strategy_version: str | None = None
    feature_schema_version: int | None = None
    forward_only: bool = False


def parse_windows(raw: str | None) -> list[tuple[float, float]]:
    if not raw:
        return []
    windows: list[tuple[float, float]] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        left, right = chunk.split("-", maxsplit=1)
        start = float(left)
        end = float(right)
        if end < start:
            raise ValueError(f"invalid window: {chunk}")
        windows.append((start, end))
    return windows


def in_windows(value: float, windows: list[tuple[float, float]]) -> bool:
    return any(start <= value <= end for start, end in windows)


def allowed_by_windows(
    seconds_to_close: float,
    allowed_windows: str | None,
    blocked_windows: str | None,
) -> bool:
    allowed = parse_windows(allowed_windows)
    blocked = parse_windows(blocked_windows)
    if allowed and not in_windows(seconds_to_close, allowed):
        return False
    return not in_windows(seconds_to_close, blocked)


def add_common_report_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--only-complete-microstructure", action="store_true")
    parser.add_argument("--from-timestamp", default=None)
    parser.add_argument("--to-timestamp", default=None)
    parser.add_argument("--regime-source", choices=["snapshot", "proxy", "all"], default="all")
    parser.add_argument("--strategy-name", default=None)
    parser.add_argument("--strategy-version", default=None)
    parser.add_argument("--feature-schema-version", type=int, default=None)
    parser.add_argument("--forward-only", action="store_true")


def filters_from_args(args) -> TimeFilters:
    return TimeFilters(
        only_complete_microstructure=args.only_complete_microstructure,
        from_timestamp=args.from_timestamp,
        to_timestamp=args.to_timestamp,
        regime_source=args.regime_source,
        strategy_name=args.strategy_name,
        strategy_version=args.strategy_version,
        feature_schema_version=args.feature_schema_version,
        forward_only=args.forward_only,
    )


def signal_time_filter_sql(alias: str, filters: TimeFilters) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if filters.only_complete_microstructure:
        clauses.extend(
            [
                f"{alias}.inefficiency_score is not null",
                f"{alias}.confidence is not null",
                f"{alias}.quote_age_ms is not null",
                f"{alias}.repricing_lag_ms is not null",
                f"{alias}.imbalance_ratio is not null",
                f"{alias}.edge is not null",
            ]
        )
    if filters.from_timestamp:
        clauses.append(f"{alias}.timestamp >= ?")
        params.append(filters.from_timestamp)
    if filters.to_timestamp:
        clauses.append(f"{alias}.timestamp <= ?")
        params.append(filters.to_timestamp)
    if filters.regime_source == "snapshot":
        clauses.append(f"{alias}.regime_source = 'snapshot'")
    elif filters.regime_source == "proxy":
        clauses.append(f"({alias}.regime_source is null or {alias}.regime_source = 'proxy')")
    if filters.strategy_name:
        clauses.append(f"coalesce({alias}.strategy_name, 'baseline') = ?")
        params.append(filters.strategy_name)
    if filters.strategy_version:
        clauses.append(f"{alias}.strategy_version = ?")
        params.append(filters.strategy_version)
    if filters.feature_schema_version is not None:
        clauses.append(f"{alias}.feature_schema_version = ?")
        params.append(filters.feature_schema_version)
    if filters.forward_only:
        clauses.extend(
            [
                f"{alias}.data_collection_started_at is not null",
                f"{alias}.regime_source = 'snapshot'",
                f"{alias}.feature_schema_version is not null",
            ]
        )
    return (" and " + " and ".join(clauses) if clauses else "", params)
