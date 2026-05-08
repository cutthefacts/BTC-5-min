from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.config import get_settings
from app.storage.sqlite import SQLiteStore

PRUNABLE_TABLES = ("orderbooks", "microstructure_events")


@dataclass(slots=True)
class MaintenanceResult:
    cutoff: str
    dry_run: bool
    deleted: dict[str, int]
    before_bytes: int
    after_bytes: int
    vacuumed: bool


def database_path(database_url: str) -> Path:
    if database_url.startswith("sqlite:///"):
        return Path(database_url.removeprefix("sqlite:///"))
    return Path(database_url)


def prune_sqlite(
    database_url: str,
    keep_hours: float,
    vacuum: bool,
    dry_run: bool,
    keep_recent_rows: int | None = None,
) -> MaintenanceResult:
    store = SQLiteStore(database_url)
    store.init()
    path = database_path(database_url)
    before_bytes = path.stat().st_size if path.exists() else 0
    cutoff = (datetime.now(UTC) - timedelta(hours=keep_hours)).isoformat()
    deleted: dict[str, int] = {}
    conn = store.conn
    for table in PRUNABLE_TABLES:
        boundary_id = (
            _recent_rows_boundary_id(conn, table, keep_recent_rows)
            if keep_recent_rows is not None
            else _last_old_id(conn, table, cutoff)
        )
        deleted[table] = boundary_id or 0
        if not dry_run and deleted[table] > 0:
            conn.execute(f"delete from {table} where id <= ?", (boundary_id,))
            conn.commit()
    vacuumed = False
    if not dry_run:
        conn.execute("pragma wal_checkpoint(TRUNCATE)")
        if vacuum:
            conn.execute("vacuum")
            vacuumed = True
    after_bytes = path.stat().st_size if path.exists() else 0
    return MaintenanceResult(
        cutoff=cutoff,
        dry_run=dry_run,
        deleted=deleted,
        before_bytes=before_bytes,
        after_bytes=after_bytes,
        vacuumed=vacuumed,
    )


def _last_old_id(conn, table: str, cutoff: str) -> int | None:
    bounds = conn.execute(f"select min(id) as min_id, max(id) as max_id from {table}").fetchone()
    if bounds is None or bounds["min_id"] is None or bounds["max_id"] is None:
        return None
    low = int(bounds["min_id"])
    high = int(bounds["max_id"])
    best: int | None = None
    while low <= high:
        mid = (low + high) // 2
        row = conn.execute(
            f"""
            select id, timestamp
            from {table}
            where id <= ?
            order by id desc
            limit 1
            """,
            (mid,),
        ).fetchone()
        if row is None:
            low = mid + 1
            continue
        row_id = int(row["id"])
        if row["timestamp"] < cutoff:
            best = row_id
            low = mid + 1
        else:
            high = row_id - 1
    return best


def _recent_rows_boundary_id(conn, table: str, keep_recent_rows: int) -> int | None:
    if keep_recent_rows <= 0:
        raise ValueError("keep_recent_rows must be positive")
    row = conn.execute(
        "select seq from sqlite_sequence where name = ?",
        (table,),
    ).fetchone()
    if row is None:
        return None
    boundary = int(row["seq"]) - keep_recent_rows
    return boundary if boundary > 0 else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Prune heavy raw market-data tables while preserving signals, trades, and results."
        ),
    )
    parser.add_argument("--keep-hours", type=float, default=12.0)
    parser.add_argument(
        "--keep-recent-rows",
        type=int,
        default=None,
        help="Fast size-control mode: keep only the latest N raw rows per heavy table.",
    )
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = prune_sqlite(
        database_url=get_settings().database_url,
        keep_hours=args.keep_hours,
        vacuum=args.vacuum,
        dry_run=args.dry_run,
        keep_recent_rows=args.keep_recent_rows,
    )
    print("SQLite Maintenance")
    print("==================")
    print(
        {
            "cutoff": result.cutoff,
            "dry_run": result.dry_run,
            "deleted": result.deleted,
            "before_gb": round(result.before_bytes / 1024 / 1024 / 1024, 3),
            "after_gb": round(result.after_bytes / 1024 / 1024 / 1024, 3),
            "vacuumed": result.vacuumed,
        }
    )


if __name__ == "__main__":
    main()
