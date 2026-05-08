from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "dev"
    database_url: str = "sqlite:///./data/trading.sqlite3"
    trading_mode: Literal["paper", "live"] = "paper"

    polymarket_gamma_url: str = "https://gamma-api.polymarket.com"
    polymarket_clob_url: str = "https://clob.polymarket.com"
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    polymarket_private_key: str = ""
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""
    polymarket_api_passphrase: str = ""

    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    btc_price_fallback_ws_url: str = "wss://ws-feed.exchange.coinbase.com"

    chainlink_reference_enabled: bool = False
    chainlink_reference_url: str = ""
    chainlink_api_key: str = ""
    chainlink_timeout_seconds: float = 5.0

    telegram_bot_token: str = ""
    telegram_admin_ids: str = ""
    telegram_polling_enabled: bool = True
    live_confirmation_phrase: str = "ENABLE_LIVE_TRADING"

    paper_starting_balance: float = 1_000.0
    fee_bps: float = 10.0
    slippage_bps: float = 5.0

    min_edge: float = 0.035
    min_confidence: float = 0.60
    min_inefficiency_score: float = 0.65
    up_min_edge: float = 0.08
    up_min_confidence: float = 0.70
    up_min_inefficiency_score: float = 0.55
    up_allowed_entry_windows: str = "90-180"
    up_disabled_for_research: bool = False
    down_min_edge: float = 0.05
    down_min_confidence: float = 0.60
    down_min_inefficiency_score: float = 0.45
    down_allowed_entry_windows: str = "90-180"
    up_market_probability_min: float = 0.40
    up_market_probability_max: float = 0.70
    down_market_probability_min: float = 0.40
    down_market_probability_max: float = 0.60
    blocked_entry_windows_seconds_to_close: str = "195-225,270-300"
    min_quote_age_ms: float = 0.0
    max_quote_age_ms_filter: float = 1_000.0
    min_repricing_lag_ms: float = 250.0
    max_repricing_lag_ms: float = 750.0
    avoid_liquidity_sweep: bool = True
    min_reliable_trades_per_bucket: int = 30
    stale_quote_ms: int = 1_500
    repricing_lag_ms: int = 750
    liquidity_drop_ratio: float = 0.35
    sweep_drop_ratio: float = 0.60
    imbalance_levels: int = 3
    min_distance_bps: float = 2.0
    max_spread: float = 0.04
    min_liquidity_usd: float = 100.0
    max_volatility_bps: float = 25.0
    disabled_regimes: str = ""
    allowed_regimes: str = ""
    side_mode: Literal["UP_ONLY", "DOWN_ONLY", "BOTH", "UP_PREFERRED"] = "BOTH"
    max_reasonable_edge: float = 0.35
    regime_gate_enabled: bool = False
    regime_gate_min_trades: int = 30
    regime_gate_min_pf: float = 1.15
    regime_gate_max_drawdown: float = 25.0
    regime_gate_max_stale_fill_rate: float = 0.10
    regime_gate_max_missed_fill_rate: float = 0.20
    rolling_pf_min: float = 0.8
    rolling_dd_max: float = 25.0
    rolling_loss_limit: int = 3
    no_trade_first_seconds: int = 10
    no_trade_last_seconds: int = 20
    strong_impulse_bps: float = 8.0

    max_trade_balance_pct: float = 0.02
    max_market_balance_pct: float = 0.05
    max_same_side_exposure_pct: float = 0.12
    max_correlated_exposure_pct: float = 0.18
    max_trades_per_market: int = 1
    market_trade_cooldown_seconds: int = 45
    max_quote_age_ms: float = 5_000
    max_stale_edge_share: float = 0.70
    daily_loss_limit_pct: float = 0.05
    max_consecutive_losses: int = 4
    cooldown_seconds: int = 300
    max_allowed_drawdown_pct: float = 0.12
    min_live_completed_trades: int = 300
    taker_fee_bps: float = 10.0
    maker_fee_bps: float = 0.0
    conservative_max_size_usd: float = 20.0
    conservative_loss_size_multiplier: float = 0.5
    strategy_version: str = "candidate_v1"
    feature_schema_version: int = 2
    data_collection_started_at: str = ""

    market_refresh_seconds: int = 20
    service_tick_seconds: float = Field(default=1.0, gt=0)
    settlement_check_seconds: int = 15
    settlement_delay_seconds: int = 10
    settlement_max_tick_lag_seconds: int = 30

    def admin_id_set(self) -> set[int]:
        ids: set[int] = set()
        for raw in self.telegram_admin_ids.split(","):
            raw = raw.strip()
            if raw:
                ids.add(int(raw))
        return ids

    def disabled_regime_set(self) -> set[str]:
        return {item.strip() for item in self.disabled_regimes.split(",") if item.strip()}

    def allowed_regime_set(self) -> set[str]:
        return {item.strip() for item in self.allowed_regimes.split(",") if item.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()
