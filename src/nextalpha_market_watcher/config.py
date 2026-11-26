from __future__ import annotations

import re
from typing import Literal, Self

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ServiceMode = Literal["alerts", "klines", "all"]


class BinanceConfig(BaseModel):
    ws_base: str = "wss://fstream.binance.com/market/stream"
    rest_base: str = "https://fapi.binance.com"
    symbols_refresh_seconds: int = 1800
    # Optional explicit proxy for WS socket connection.
    # e.g. socks5://host:port
    ws_proxy_url: str | None = None
    ws_watchdog_enabled: bool = True
    ws_watchdog_no_data_periods: int = 2
    ws_watchdog_check_seconds: int = 15
    use_test_symbols: bool = False
    test_symbols: list[str] = Field(default_factory=list)


class BackfillConfig(BaseModel):
    enabled: bool = True
    rest_timeout_seconds: float = 15.0
    rest_concurrency: int = 8
    rest_request_delay_seconds: float = 0.02
    bars_1m: int = 32
    bars_1h: int = 3
    klines_limit_cap: int = 1500


class RedisConfig(BaseModel):
    url: str = "redis://127.0.0.1:6379/0"
    key_prefix: str = "nw:"
    stream_1h_closed: str = "stream:kline:1h:closed"
    stream_downstream_ack: str = "stream:kline:1h:ack"
    stream_latency_events: str = "stream:latency:events"
    stream_maxlen: int = 200000
    published_1h_ttl_seconds: int = 9 * 3600
    one_m_ring_max_bars: int = 480
    one_m_ring_ttl_seconds: int = 36 * 3600
    pending_latency_max: int = 100_000


class AlertsConfig(BaseModel):
    threshold_1m_pct: float = 10.0
    threshold_5m_pct: float = 15.0
    threshold_1h_pct: float = 25.0
    threshold_4h_pct: float = 40.0
    cooldown_seconds: int = 900
    major_1m_enabled: bool = True
    publish_major_1m_klines: bool = True
    # Empty means reuse websocket.klines_major_symbols.
    major_symbols: list[str] = Field(default_factory=list)


class WebhookConfig(BaseModel):
    enabled: bool = False
    url: str = ""
    # Optional dedicated webhook for threshold alert notifications.
    # If empty, alerts reuse `url`.
    alerts_url: str = ""


class LatencyConfig(BaseModel):
    e2e_warn_ms: int = 1500


class MetricsConfig(BaseModel):
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 9090


class WebSocketPushConfig(BaseModel):
    host: str = "0.0.0.0"
    alerts_port: int = 8765
    klines_port: int = 8766
    klines_interval: str = "1h"
    klines_all_topic: str = ""
    klines_ws_batch_size: int = 200
    klines_major_symbols: list[str] = Field(
        default_factory=lambda: ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
    )
    klines_exclude_major_symbols: bool = True
    klines_major_1h_enabled: bool = True
    klines_major_1h_interval: str = "1h"
    klines_major_1h_topic: str = "klines.majors.1h"
    klines_major_1m_enabled: bool = False
    klines_major_1m_interval: str = "1m"
    klines_major_1m_topic: str = "klines.majors.1m"
    # Publish 1m kline topic only on this minute cadence. 1 means every minute.
    klines_major_1m_publish_every_minutes: int = 1
    klines_major_batch_max_wait_ms: int = 3_000
    # If True, publish each closed kline row immediately to downstream clients
    # without server-side close_time batch aggregation.
    klines_passthrough_mode: bool = False
    # Max wait (ms) to hold a shard-local buffer before flushing a partial batch.
    klines_passthrough_shard_max_wait_ms: int = 3_000
    klines_replay_count: int = 3
    klines_flush_grace_ms: int = 20_000
    klines_full_batch_only: bool = False
    klines_full_batch_max_wait_ms: int = 120_000
    klines_min_publish_ratio: float = 1.0
    # In full-batch mode, optionally allow publishing after max wait
    # when missing symbols are within this count.
    klines_max_missing_symbols: int = 0
    # 0 means auto by interval: 5m->2min, 1h->10min
    klines_rest_repair_delay_ms: int = 0
    # Emit full missing symbol list for each published batch
    klines_log_full_missing_symbols: bool = True
    # If True, when WS is silent for a while, run low-frequency REST fallback repair.
    # Keep this on for testing, and disable in low-latency production once WS is stable.
    klines_ws_silence_fallback_rest: bool = True

    @model_validator(mode="after")
    def validate_full_market_topic(self) -> Self:
        topic = self.klines_all_topic.strip()
        match = re.fullmatch(r"klines\.all\.(\d+[smhdwM])", topic)
        interval = self.klines_interval.strip() or "1h"
        if match and match[1] != interval:
            raise ValueError(f"klines_all_topic {topic!r} does not match klines_interval {interval!r}")
        return self


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    app_name: str = "nextalpha-market-watcher"
    service_region: str = "unknown"
    log_level: str = "INFO"
    service_mode: ServiceMode = "all"
    binance: BinanceConfig = BinanceConfig()
    backfill: BackfillConfig = BackfillConfig()
    redis: RedisConfig = RedisConfig()
    alerts: AlertsConfig = AlertsConfig()
    webhook: WebhookConfig = WebhookConfig()
    latency: LatencyConfig = LatencyConfig()
    metrics: MetricsConfig = MetricsConfig()
    websocket: WebSocketPushConfig = WebSocketPushConfig()
