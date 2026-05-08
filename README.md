# Polymarket BTC 5m Edge MVP

Python 3.11+ service for researching and paper-trading BTC Up/Down 5 minute Polymarket markets. The default mode is paper trading. Live trading is blocked until the system has at least 300 completed paper trades, positive net PnL after fees, and drawdown within the configured risk limit.

Official API assumptions checked against Polymarket docs on 2026-05-05:

- Gamma API discovers markets: `https://gamma-api.polymarket.com`
- CLOB API exposes public orderbooks/prices: `https://clob.polymarket.com`
- Market WebSocket subscribes by `assets_ids` and emits `book`, `price_change`, `best_bid_ask`, lifecycle events.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
Copy-Item .env.example .env
```

Edit `.env`. Keep keys in `.env` only.

## Run

```powershell
python -m app.main
```

The service:

1. discovers active BTC 5m markets,
2. subscribes to Polymarket CLOB orderbook updates,
3. streams BTC price from Binance,
4. evaluates Reactive Directional + Market Microstructure signals,
5. stores markets, ticks, signals, orders, trades, and snapshots in SQLite,
6. paper-trades only unless live gates pass and Telegram confirmation is supplied.

## Telegram

Supported commands:

`/start`, `/status`, `/balance`, `/positions`, `/stats`, `/risk`, `/candidate`, `/regime`, `/regime_gate`, `/research_gate`, `/walkforward`, `/forward_validation`, `/hourly`, `/edge_quality`, `/shadow_stats`, `/pause`, `/resume`, `/kill`, `/mode paper|live`

Live mode requires the configured confirmation phrase and the live safety gate to pass.

## Research Workflow

```powershell
python -m app.main
```

Collect data for at least 30-60 minutes, then run:

```powershell
python -m app.backtest.data_quality_report
python -m app.backtest.microstructure_report
python -m app.backtest.microstructure_report --summary --only-complete-microstructure
python -m app.backtest.hold_report
python -m app.backtest.missed_opportunities_report
python -m app.backtest.edge_decay_report
python -m app.backtest.edge_half_life_report
python -m app.backtest.liquidity_sweep_report --only-complete-microstructure
python -m app.backtest.entry_window_analysis
python -m app.backtest.latency_sensitivity_report
python -m app.backtest.replay --latency-ms 250 --only-complete-microstructure
python -m app.backtest.filter_relaxation_report --only-complete-microstructure
python -m app.backtest.candidate_signal_report --only-complete-microstructure
python -m app.backtest.baseline_report --preset balanced --only-complete-microstructure
python -m app.backtest.validate_best_window --only-complete-microstructure
python -m app.backtest.stale_trap_report --only-complete-microstructure
python -m app.backtest.walk_forward --preset balanced --only-complete-microstructure
python -m app.backtest.filter_range_optimizer --preset balanced --only-complete-microstructure
python -m app.backtest.position_sizing_report --preset balanced --only-complete-microstructure
python -m app.backtest.passive_execution_report --preset balanced --only-complete-microstructure
python -m app.backtest.research_gate --preset balanced --only-complete-microstructure
python -m app.backtest.side_performance_report --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.regime_report --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.regime_memory_report --preset candidate_v1 --only-complete-microstructure --write
python -m app.backtest.compression_report --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.hourly_regime_report --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.edge_quality_report --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.forward_validation_report --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.compression_validation_report --preset candidate_v1 --regime-source snapshot --forward-only
python -m app.backtest.walk_forward --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.research_gate --preset candidate_v1 --only-complete-microstructure
python -m app.backtest.settlement_source_report
python -m app.backtest.replay --preset balanced --latency-ms 250 --only-complete-microstructure
python -m app.backtest.replay --preset candidate_v1 --latency-ms 250 --only-complete-microstructure
python -m app.backtest.replay --preset exploratory --soft-filters --latency-ms 250 --only-complete-microstructure
python -m app.backtest.replay_matrix --summary --only-complete-microstructure
python -m app.backtest.replay --latency-ms 250 --execution-mode maker
python -m app.backtest.replay --latency-ms 250 --execution-mode hybrid
python -m app.backtest.optimize --mode minimum_viable_edge --preset exploratory --only-complete-microstructure
```

Use these reports to validate whether a real microstructure edge exists, where it
appears, how long it survives, which entry windows work, and whether taker,
maker, or hybrid execution can capture it. Do not tune parameters from net PnL
alone; reject configurations with too few trades, weak profit factor, high
drawdown, unstable bucket performance, stale fills, excessive missed fills, or
poor data quality.

Live trading remains disabled until paper results are statistically meaningful
and pass the configured live gate.

Reports and replay support:

```powershell
--only-complete-microstructure
--from-timestamp 2026-05-08T00:00:00+00:00
--to-timestamp 2026-05-08T04:00:00+00:00
--preset strict|balanced|exploratory|down_only|best_window_120_180|candidate_v1
--soft-filters
```

Use these flags to avoid mixing legacy rows without microstructure fields with
new paper/replay data.

Current research status: live trading is disabled; `balanced` has passed the
research gate on current data; `candidate_v1` is the promoted research candidate
based on `best_window_120_180`; `exploratory` is treated as stale-trap despite
positive raw PnL. The main blocker is walk-forward degradation and regime
instability, so `regime_report`, `walk_forward`, and `research_gate` must be run
before treating any configuration as promising. Taker execution is the current
baseline. Maker/passive execution remains research-only because fill probability
is low and missed fills are high. Settlement still needs Chainlink/reference
validation beyond the Binance fallback.

Regime gating is research/paper-only. Populate `regime_performance` with
`regime_memory_report --write`, inspect bad regime scores, then test
`REGIME_GATE_ENABLED=true` only in paper/shadow mode. Extreme edges are treated
as suspect; `edge_quality_report` checks whether PnL depends on fake/stale edge
buckets instead of moderate stable edge.

`candidate_v1` is promising in replay, but live remains disabled. Earlier regime
detection was proxy-based because old signals did not store full market-state
snapshots. New forward signals store snapshot fields, `regime_source=snapshot`,
`strategy_version`, and `feature_schema_version`. Research Gate V4 should be run
on forward snapshot data:

```powershell
python -m app.main --strategy candidate_v1 --paper-shadow
python -m app.backtest.data_quality_report --strategy-name candidate_v1 --forward-only
python -m app.backtest.compression_validation_report --preset candidate_v1 --regime-source snapshot --forward-only
python -m app.backtest.research_gate --preset candidate_v1 --forward-only
```

If forward snapshot trades are below the required sample, the gate reports
`INSUFFICIENT_FORWARD_DATA` rather than a strategy failure.

Presets are research-only. `strict` keeps the current hard filters, `balanced`
widens quote age and repricing lag ranges, `exploratory` uses softer penalties,
`down_only` isolates one side, and `best_window_120_180` validates the current
best candidate window, and `candidate_v1` adds side-aware research defaults
around the `120-180` seconds-to-close window. None of these presets can enable
live trading.

Candidate shadow mode is paper-only and stores `strategy_name` so baseline and
candidate trades can be compared on the same markets:

```powershell
python -m app.main --strategy candidate_v1 --paper-shadow
```

## SQLite Maintenance

Raw `orderbooks` and `microstructure_events` grow very quickly. Keep research
tables (`signals`, `trades`, `results`) and periodically prune raw market data:

```powershell
python -m app.storage.maintenance --keep-recent-rows 2000000 --dry-run
python -m app.storage.maintenance --keep-recent-rows 2000000 --vacuum
```

Stop `python -m app.main` before running `--vacuum`.

## Tests

```powershell
pytest
ruff check .
```

## Safety

- No martingale.
- No uncontrolled averaging.
- Kill-switch has priority over everything.
- Real execution is implemented as a guarded stub. Wire authenticated CLOB signing only after the paper gate passes and after reviewing exchange/legal requirements for your jurisdiction.
