from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from datetime import datetime

import httpx
import orjson
import redis.asyncio as redis
from fastapi import WebSocket
from websockets.server import WebSocketServerProtocol

from .alerts import AlertEngine, AlertSignal
from .binance_client import BinanceClient
from .config import AppConfig, WebhookConfig
from .latency import LatencyEvent
from .metrics import start_metrics_server
from .models import ClosedKline
from .notifier import Notifier
from .ws_push import WsBroadcaster

logger = logging.getLogger(__name__)


class App:
    def __init__(
        self,
        cfg: AppConfig,
        alerts_ws: object | None = None,
        klines_ws: object | None = None,
    ) -> None:
        self._cfg = cfg
        self._notifier = Notifier(cfg.webhook)
        alerts_webhook_url = cfg.webhook.alerts_url.strip() or cfg.webhook.url
        self._alerts_notifier = Notifier(
            WebhookConfig(
                enabled=cfg.webhook.enabled,
                url=alerts_webhook_url,
                alerts_url=cfg.webhook.alerts_url,
            ),
        )
        self._alerts: AlertEngine | None = None
        if cfg.service_mode in ("alerts", "all"):
            self._alerts = AlertEngine(cfg, self._alerts_notifier, self._get_alert_clients_count)
        self._binance = BinanceClient(cfg)
        self._alerts_ws: object | None = alerts_ws
        self._klines_ws: object | None = klines_ws
        replay_maxlen = max(0, cfg.websocket.klines_replay_count)
        self._klines_recent_maxlen = replay_maxlen
        self._klines_recent_by_topic: dict[str, deque[list[dict]]] = {}
        self._klines_recent_lock = asyncio.Lock()
        self._klines_interval = self._cfg.websocket.klines_interval.strip() or "1h"
        self._klines_all_topic = self._cfg.websocket.klines_all_topic.strip() or f"klines.all.{self._klines_interval}"
        self._major_symbols = [s.strip().upper() for s in self._cfg.websocket.klines_major_symbols if s.strip()]
        self._major_symbol_set = set(self._major_symbols)
        self._exclude_major_symbols = bool(self._cfg.websocket.klines_exclude_major_symbols)
        self._major_1h_enabled = bool(self._cfg.websocket.klines_major_1h_enabled)
        self._major_1h_interval = self._cfg.websocket.klines_major_1h_interval.strip() or "1h"
        self._major_1h_topic = self._cfg.websocket.klines_major_1h_topic.strip() or "klines.majors.1h"
        self._major_1m_interval = self._cfg.websocket.klines_major_1m_interval.strip() or "1m"
        self._major_1m_topic = self._cfg.websocket.klines_major_1m_topic.strip() or "klines.majors.1m"
        self._major_1m_enabled = bool(self._cfg.websocket.klines_major_1m_enabled)
        self._major_1m_publish_every_minutes = max(1, self._cfg.websocket.klines_major_1m_publish_every_minutes)
        self._klines_stream_key = self._klines_interval
        self._major_1h_stream_key = f"{self._major_1h_interval}:{self._major_1h_topic}"
        self._major_1m_stream_key = f"{self._major_1m_interval}:{self._major_1m_topic}"
        alert_major_symbols = self._cfg.alerts.major_symbols or self._cfg.websocket.klines_major_symbols
        self._alert_major_symbols = [s.strip().upper() for s in alert_major_symbols if s.strip()]
        self._alert_major_symbol_set = set(self._alert_major_symbols)
        self._alert_major_1m_enabled = bool(self._cfg.alerts.major_1m_enabled)
        self._alert_publish_major_1m_klines = bool(self._cfg.alerts.publish_major_1m_klines)
        self._alert_1m_stream_key = "alerts.1m"
        self._alert_major_1m_stream_key = "alerts.1m:majors"
        self._major_batch_max_wait_ms = max(50, self._cfg.websocket.klines_major_batch_max_wait_ms)
        if self._alerts_ws is None and cfg.service_mode in ("alerts", "all"):
            self._alerts_ws = WsBroadcaster(cfg.websocket.host, cfg.websocket.alerts_port, name="alerts")
        if self._klines_ws is None and cfg.service_mode in ("klines", "all"):
            self._klines_ws = WsBroadcaster(
                cfg.websocket.host,
                cfg.websocket.klines_port,
                name="klines",
                on_client_connected=self._on_klines_client_connected,
                default_topic=self._klines_all_topic,
            )
        self._ingest_1m = 0
        self._ingest_1h = 0
        self._push_alerts = 0
        self._push_1h = 0
        self._expected_symbols: set[str] = set()
        self._pending_by_close: dict[int, dict[str, dict]] = {}
        self._pending_has_ws: dict[int, bool] = {}
        self._pending_first_ingest_ms: dict[int, int] = {}
        self._published_close_times: set[int] = set()
        self._published_close_order: deque[int] = deque()
        self._published_close_max = 32
        self._flush_grace_ms = max(1_000, self._cfg.websocket.klines_flush_grace_ms)
        self._full_batch_only = self._cfg.websocket.klines_full_batch_only
        self._min_publish_ratio = min(1.0, max(0.0, self._cfg.websocket.klines_min_publish_ratio))
        self._full_batch_max_wait_ms = max(1_000, self._cfg.websocket.klines_full_batch_max_wait_ms)
        self._max_missing_symbols = max(0, self._cfg.websocket.klines_max_missing_symbols)
        self._pending_lock = asyncio.Lock()
        self._klines_ws_batch_size = max(1, self._cfg.websocket.klines_ws_batch_size)
        self._klines_passthrough_mode = bool(self._cfg.websocket.klines_passthrough_mode)
        self._rest_repair_delay_ms = self._resolve_rest_repair_delay_ms(self._klines_interval)
        self._rest_repairing_close_times: set[int] = set()
        self._log_full_missing_symbols = self._cfg.websocket.klines_log_full_missing_symbols
        self._pending_latency: dict[tuple[str, str], LatencyEvent] = {}
        self._pending_latency_lock = asyncio.Lock()
        self._latency_redis = redis.from_url(cfg.redis.url, decode_responses=True)
        self._event_close_time: dict[tuple[str, str], tuple[str, int]] = {}
        self._ack_service_to_downstream_by_close: dict[tuple[str, int], list[int]] = {}
        self._ack_ingest_to_downstream_by_close: dict[tuple[str, int], list[int]] = {}
        self._batch_done_to_ack_by_close: dict[tuple[str, int], list[int]] = {}
        self._batch_done_to_integrated_ack_by_close: dict[tuple[str, int], list[int]] = {}
        self._batch_done_to_compute_ack_by_close: dict[tuple[str, int], list[int]] = {}
        self._batch_done_to_order_ack_by_close: dict[tuple[str, int], list[int]] = {}
        self._ack_expected_by_close: dict[tuple[str, int], int] = {}
        self._ack_clients_by_close: dict[tuple[str, int], set[int]] = {}
        self._batch_ack_clients_by_close: dict[tuple[str, int], set[int]] = {}
        self._batch_integrated_ack_clients_by_close: dict[tuple[str, int], set[int]] = {}
        self._batch_compute_ack_clients_by_close: dict[tuple[str, int], set[int]] = {}
        self._batch_order_ack_clients_by_close: dict[tuple[str, int], set[int]] = {}
        self._ack_notify_scheduled: set[tuple[str, int]] = set()
        self._batch_summary_by_close: dict[tuple[str, int], dict[str, str | int]] = {}
        self._batch_metric_values_by_close: dict[tuple[str, int], dict[str, int | list[int] | dict | str]] = {}
        self._passthrough_shards_sent_by_close: dict[int, set[str]] = {}
        self._passthrough_total_shards_by_close: dict[int, int] = {}
        self._batch_done_publish_ms_by_close: dict[tuple[str, int], int] = {}
        self._publish_queue: asyncio.Queue[tuple[str, int, dict[str, dict], int | None, bool, bool]] = asyncio.Queue(
            maxsize=4096,
        )
        self._latency_write_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue(maxsize=200_000)
        self._passthrough_shard_lock = asyncio.Lock()
        self._passthrough_shard_buffers: dict[tuple[int, str], dict[str, dict]] = {}
        # [expected_symbol_count, first_wall_ms, min_ingest_ms]
        self._passthrough_shard_meta: dict[tuple[int, str], list[int]] = {}
        self._passthrough_shard_max_wait_ms = max(50, self._cfg.websocket.klines_passthrough_shard_max_wait_ms)
        self._major_pending_lock = asyncio.Lock()
        self._major_pending_by_close: dict[tuple[str, int], dict[str, dict]] = {}
        self._major_pending_first_ingest_ms: dict[tuple[str, int], int] = {}
        self._major_expected_symbols_by_topic: dict[str, set[str]] = {}
        self._major_published_close_times: set[tuple[str, int]] = set()
        self._major_published_close_order: deque[tuple[str, int]] = deque()
        self._major_published_close_max = 64

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop_event = stop if stop is not None else asyncio.Event()
        start_metrics_server(self._cfg)
        logger.info("service.start mode=%s", self._cfg.service_mode)
        syms = await self._binance.fetch_symbols()
        logger.info("symbols.loaded count=%s", len(syms))
        if not syms:
            raise RuntimeError("No symbols found for Binance U perpetual.")

        ingest_task: asyncio.Task[None] | None = None
        refresh_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        stale_flush_task: asyncio.Task[None] | None = None
        major_stale_flush_task: asyncio.Task[None] | None = None
        passthrough_flush_task: asyncio.Task[None] | None = None
        publish_worker_task: asyncio.Task[None] | None = None
        latency_writer_task: asyncio.Task[None] | None = None
        ws_tasks: list[asyncio.Task[None]] = []
        if self._alerts_ws is not None:
            ws_tasks.append(asyncio.create_task(self._alerts_ws.serve_until_stop(stop_event)))
        if self._klines_ws is not None:
            ws_tasks.append(asyncio.create_task(self._klines_ws.serve_until_stop(stop_event)))
        try:
            ingest_task = asyncio.create_task(self._run_ingestion(syms))
            refresh_task = asyncio.create_task(
                asyncio.sleep(self._cfg.binance.symbols_refresh_seconds),
            )
            heartbeat_task = asyncio.create_task(self._run_heartbeat(stop_event))
            publish_worker_task = asyncio.create_task(self._run_publish_worker(stop_event))
            latency_writer_task = asyncio.create_task(self._run_latency_writer(stop_event))
            if self._cfg.service_mode in ("klines", "all") and not self._klines_passthrough_mode:
                stale_flush_task = asyncio.create_task(self._run_klines_stale_flush(stop_event))
            if self._should_run_any_dedicated_major() or self._should_publish_alert_major_1m_klines():
                major_stale_flush_task = asyncio.create_task(self._run_major_stale_flush(stop_event))
            if self._cfg.service_mode in ("klines", "all") and self._klines_passthrough_mode:
                passthrough_flush_task = asyncio.create_task(self._run_passthrough_shard_flush(stop_event))
            stop_wait = asyncio.create_task(stop_event.wait())

            while True:
                wait_set = {ingest_task, refresh_task, stop_wait}
                done, _pending = await asyncio.wait(
                    wait_set,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if stop_wait in done:
                    if ingest_task and not ingest_task.done():
                        ingest_task.cancel()
                    if refresh_task and not refresh_task.done():
                        refresh_task.cancel()
                    if ingest_task:
                        await asyncio.gather(ingest_task, return_exceptions=True)
                    if refresh_task:
                        await asyncio.gather(refresh_task, return_exceptions=True)
                    if heartbeat_task:
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
                    if stale_flush_task:
                        await asyncio.gather(stale_flush_task, return_exceptions=True)
                    if major_stale_flush_task:
                        await asyncio.gather(major_stale_flush_task, return_exceptions=True)
                    if passthrough_flush_task:
                        await asyncio.gather(passthrough_flush_task, return_exceptions=True)
                    if publish_worker_task:
                        await asyncio.gather(publish_worker_task, return_exceptions=True)
                    if latency_writer_task:
                        await asyncio.gather(latency_writer_task, return_exceptions=True)
                    break

                if ingest_task in done:
                    if refresh_task and not refresh_task.done():
                        refresh_task.cancel()
                        await asyncio.gather(refresh_task, return_exceptions=True)
                    exc = ingest_task.exception()
                    if exc is not None and not isinstance(exc, asyncio.CancelledError):
                        raise exc
                    break

                if refresh_task in done:
                    new_syms = await self._binance.fetch_symbols()
                    if set(new_syms) != set(syms):
                        logger.info("symbols.refresh %s->%s", len(syms), len(new_syms))
                        if self._alerts is not None:
                            self._alerts.prune_to_symbols(set(new_syms))
                        syms = new_syms
                        if ingest_task and not ingest_task.done():
                            ingest_task.cancel()
                            await asyncio.gather(ingest_task, return_exceptions=True)
                        ingest_task = asyncio.create_task(self._run_ingestion(syms))
                    refresh_task = asyncio.create_task(
                        asyncio.sleep(self._cfg.binance.symbols_refresh_seconds),
                    )

        finally:
            stop_event.set()
            to_cancel = [
                ingest_task,
                refresh_task,
                heartbeat_task,
                stale_flush_task,
                major_stale_flush_task,
                passthrough_flush_task,
                publish_worker_task,
                latency_writer_task,
                *ws_tasks,
            ]
            for t in to_cancel:
                if t is not None and not t.done():
                    t.cancel()
            await asyncio.gather(
                *[x for x in to_cancel if x is not None],
                return_exceptions=True,
            )
            await self._latency_redis.aclose()

    async def _run_ingestion(self, symbols: list[str]) -> None:
        tasks: list[asyncio.Task[None]] = []
        if self._cfg.service_mode in ("klines", "all"):
            await self._reset_full_klines_batch_state(symbols)
            primary_symbols = self._primary_klines_symbols(symbols)
            if primary_symbols:
                tasks.append(asyncio.create_task(self._run_1h_publish(primary_symbols)))
            else:
                logger.warning("klines.primary.disabled reason=no_matching_symbols")
            major_symbols = self._available_major_symbols(symbols)
            if self._major_symbol_set and not major_symbols:
                logger.warning("klines.major.disabled reason=no_matching_symbols")
            if major_symbols and self._should_run_dedicated_major_1h():
                tasks.append(
                    asyncio.create_task(
                        self._run_major_interval_publish(
                            major_symbols,
                            interval=self._major_1h_interval,
                            topic=self._major_1h_topic,
                            stream_key=self._major_1h_stream_key,
                            mirror_to_all_topic=self._major_1h_interval == self._klines_interval,
                        ),
                    ),
                )
            if major_symbols and self._should_run_dedicated_major_1m():
                tasks.append(
                    asyncio.create_task(
                        self._run_major_interval_publish(
                            major_symbols,
                            interval=self._major_1m_interval,
                            topic=self._major_1m_topic,
                            stream_key=self._major_1m_stream_key,
                        ),
                    ),
                )
        if self._cfg.service_mode in ("alerts", "all"):
            if self._alerts is None:
                raise RuntimeError("alerts ingestion requires service_mode alerts or all")
            publish_major_1m_topic = self._major_1m_topic if self._should_publish_alert_major_1m_klines() else None
            if self._should_run_dedicated_alert_major_1m():
                alert_major_symbols = self._available_alert_major_symbols(symbols)
                if alert_major_symbols:
                    if publish_major_1m_topic:
                        self._major_expected_symbols_by_topic[publish_major_1m_topic] = set(alert_major_symbols)
                    alert_primary_symbols = self._primary_alert_symbols(symbols)
                    if alert_primary_symbols:
                        tasks.append(
                            asyncio.create_task(
                                self._run_1m_alerts(
                                    alert_primary_symbols,
                                    stream_key=self._alert_1m_stream_key,
                                ),
                            ),
                        )
                    else:
                        logger.warning("alerts.primary.disabled reason=no_matching_symbols")
                    tasks.append(
                        asyncio.create_task(
                            self._run_1m_alerts(
                                alert_major_symbols,
                                batch_size=max(1, len(alert_major_symbols)),
                                stream_key=self._alert_major_1m_stream_key,
                                publish_kline_topic=publish_major_1m_topic,
                                publish_kline_symbols=set(alert_major_symbols),
                            ),
                        ),
                    )
                else:
                    logger.warning("alerts.major.disabled reason=no_matching_symbols")
                    tasks.append(
                        asyncio.create_task(
                            self._run_1m_alerts(symbols, stream_key=self._alert_1m_stream_key),
                        ),
                    )
            else:
                alert_major_symbols = self._available_alert_major_symbols(symbols) if publish_major_1m_topic else []
                if publish_major_1m_topic and alert_major_symbols:
                    self._major_expected_symbols_by_topic[publish_major_1m_topic] = set(alert_major_symbols)
                tasks.append(
                    asyncio.create_task(
                        self._run_1m_alerts(
                            symbols,
                            stream_key=self._alert_1m_stream_key,
                            publish_kline_topic=publish_major_1m_topic,
                            publish_kline_symbols=set(alert_major_symbols),
                        ),
                    ),
                )
        if not tasks:
            raise RuntimeError(f"No ingestion tasks for service_mode={self._cfg.service_mode}")

        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for t in done:
                exc = t.exception()
                if exc is not None and not isinstance(exc, asyncio.CancelledError):
                    raise exc
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _primary_klines_symbols(self, symbols: list[str]) -> list[str]:
        if not self._exclude_major_symbols or not self._major_symbol_set:
            return symbols
        if not self._should_run_dedicated_major_1h() or self._major_1h_interval != self._klines_interval:
            return symbols
        filtered = [symbol for symbol in symbols if symbol not in self._major_symbol_set]
        logger.info(
            "klines.primary.symbols selected=%s dedicated_major_shard=%s",
            len(filtered),
            len(symbols) - len(filtered),
        )
        return filtered

    def _available_major_symbols(self, symbols: list[str]) -> list[str]:
        available_symbols = set(symbols)
        major_symbols = [s for s in self._major_symbols if s in available_symbols]
        missing_major = sorted(self._major_symbol_set - set(major_symbols))
        if missing_major:
            logger.warning("klines.major.symbols_missing symbols=%s", ",".join(missing_major))
        return major_symbols

    def _primary_alert_symbols(self, symbols: list[str]) -> list[str]:
        if not self._should_run_dedicated_alert_major_1m():
            return symbols
        filtered = [symbol for symbol in symbols if symbol not in self._alert_major_symbol_set]
        logger.info(
            "alerts.primary.symbols selected=%s dedicated_major_shard=%s",
            len(filtered),
            len(symbols) - len(filtered),
        )
        return filtered

    def _available_alert_major_symbols(self, symbols: list[str]) -> list[str]:
        available_symbols = set(symbols)
        major_symbols = [s for s in self._alert_major_symbols if s in available_symbols]
        missing_major = sorted(self._alert_major_symbol_set - set(major_symbols))
        if missing_major:
            logger.warning("alerts.major.symbols_missing symbols=%s", ",".join(missing_major))
        return major_symbols

    def _should_run_any_dedicated_major(self) -> bool:
        return self._should_run_dedicated_major_1h() or self._should_run_dedicated_major_1m()

    def _should_run_dedicated_major_1h(self) -> bool:
        return (
            self._cfg.service_mode in ("klines", "all")
            and self._major_1h_enabled
            and bool(self._major_symbol_set)
        )

    def _should_run_dedicated_major_1m(self) -> bool:
        return (
            self._cfg.service_mode in ("klines", "all")
            and self._major_1m_enabled
            and bool(self._major_symbol_set)
        )

    def _should_run_dedicated_alert_major_1m(self) -> bool:
        return (
            self._cfg.service_mode in ("alerts", "all")
            and self._alert_major_1m_enabled
            and bool(self._alert_major_symbol_set)
        )

    def _should_publish_alert_major_1m_klines(self) -> bool:
        return (
            self._cfg.service_mode in ("alerts", "all")
            and self._alert_publish_major_1m_klines
            and bool(self._alert_major_symbol_set)
            and bool(self._major_1m_topic)
            and not (self._cfg.service_mode == "all" and self._major_1m_enabled)
        )

    async def _reset_full_klines_batch_state(self, symbols: list[str]) -> None:
        async with self._pending_lock:
            self._expected_symbols = set(symbols)
            self._pending_by_close.clear()
            self._pending_has_ws.clear()
            self._pending_first_ingest_ms.clear()
            self._published_close_times.clear()
            self._published_close_order.clear()
            self._rest_repairing_close_times.clear()
        async with self._passthrough_shard_lock:
            self._passthrough_shard_buffers.clear()
            self._passthrough_shard_meta.clear()
            self._passthrough_shards_sent_by_close.clear()
            self._passthrough_total_shards_by_close.clear()

    def _full_batch_min_required(self) -> int:
        expected_symbols = len(self._expected_symbols)
        if self._full_batch_only:
            return max(1, expected_symbols)
        return max(1, math.ceil(expected_symbols * self._min_publish_ratio))

    def _kline_row_dict(self, kline: ClosedKline) -> dict:
        return {
            "event_id": f"{kline.symbol}:{kline.interval}:{kline.open_time_ms}",
            "symbol": kline.symbol,
            "interval": kline.interval,
            "open_time_ms": kline.open_time_ms,
            "close_time_ms": kline.close_time_ms,
            "open": str(kline.open_price),
            "high": str(kline.high_price),
            "low": str(kline.low_price),
            "close": str(kline.close_price),
            "volume": str(kline.volume),
            "quote_volume": str(kline.quote_volume),
            "exchange_event_ms": kline.event_time_ms,
            "service_ingest_ms": kline.ingest_time_ms,
            "service_region": self._cfg.service_region,
            "source": kline.source,
            "ws_shard_id": kline.ws_shard_id,
            "ws_shard_symbol_count": kline.ws_shard_symbol_count,
            "ws_total_shards": kline.ws_total_shards,
        }

    def _passthrough_row_dict(self, kline: ClosedKline) -> dict:
        return self._kline_row_dict(kline)

    async def _passthrough_buffer_add(self, kline: ClosedKline) -> None:
        row = self._passthrough_row_dict(kline)
        if kline.ws_shard_symbol_count <= 0 or not kline.ws_shard_id:
            await self._enqueue_publish(kline.close_time_ms, {kline.symbol: row}, kline.ingest_time_ms)
            return
        key = (kline.close_time_ms, kline.ws_shard_id)
        maybe_flush: tuple[int, dict[str, dict], int | None] | None = None
        async with self._passthrough_shard_lock:
            buf = self._passthrough_shard_buffers.setdefault(key, {})
            buf[kline.symbol] = row
            meta = self._passthrough_shard_meta.setdefault(
                key,
                [kline.ws_shard_symbol_count, int(time.time() * 1000), kline.ingest_time_ms],
            )
            meta[2] = min(meta[2], kline.ingest_time_ms)
            if len(buf) >= meta[0]:
                maybe_flush = (key[0], dict(buf), meta[2])
                del self._passthrough_shard_buffers[key]
                del self._passthrough_shard_meta[key]
        if maybe_flush is not None:
            close_ms, rows, first_ingest = maybe_flush
            await self._enqueue_publish(close_ms, rows, first_ingest)

    async def _run_passthrough_shard_flush(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(0.05)
            now_ms = int(time.time() * 1000)
            flush_items: list[tuple[int, dict[str, dict], int | None]] = []
            async with self._passthrough_shard_lock:
                for key in list(self._passthrough_shard_buffers.keys()):
                    buf = self._passthrough_shard_buffers.get(key)
                    if not buf:
                        continue
                    meta = self._passthrough_shard_meta.get(key)
                    if not meta:
                        continue
                    _exp, first_wall, min_ingest = meta
                    if now_ms - first_wall >= self._passthrough_shard_max_wait_ms:
                        flush_items.append((key[0], dict(buf), min_ingest))
                        del self._passthrough_shard_buffers[key]
                        del self._passthrough_shard_meta[key]
            for close_ms, rows, first_ingest in flush_items:
                await self._enqueue_publish(close_ms, rows, first_ingest)

    async def _run_1h_publish(self, symbols: list[str]) -> None:
        if self._klines_ws is None:
            async for _ in self._binance.iter_closed_klines(
                symbols,
                interval=self._klines_interval,
                stream_key=self._klines_stream_key,
            ):
                self._ingest_1h += 1
            return

        latest_close_by_symbol: dict[str, int] = {}
        async for kline in self._binance.iter_closed_klines(
            symbols,
            interval=self._klines_interval,
            batch_size=self._klines_ws_batch_size,
            stream_key=self._klines_stream_key,
        ):
            prev_close = latest_close_by_symbol.get(kline.symbol)
            if prev_close is not None and kline.close_time_ms <= prev_close:
                continue
            latest_close_by_symbol[kline.symbol] = kline.close_time_ms
            self._ingest_1h += 1
            await self._add_full_topic_kline(kline)

        async with self._pending_lock:
            pending_items = sorted(self._pending_by_close.items())
            self._pending_by_close = {}
            self._pending_has_ws = {}
            pending_first = self._pending_first_ingest_ms
            self._pending_first_ingest_ms = {}
            for close_time_ms, _ in pending_items:
                self._mark_close_time_published_locked(close_time_ms)

        for close_time_ms, rows_by_symbol in pending_items:
            missing = len(self._expected_symbols) - len(rows_by_symbol)
            if missing > 0:
                logger.warning(
                    "klines.batch.incomplete close_time_ms=%s got=%s expected=%s missing=%s",
                    close_time_ms,
                    len(rows_by_symbol),
                    len(self._expected_symbols),
                    missing,
                )
            await self._enqueue_publish(
                close_time_ms=close_time_ms,
                rows_by_symbol=rows_by_symbol,
                first_ingest_ms=pending_first.get(close_time_ms),
            )

    async def _add_full_topic_kline(self, kline: ClosedKline) -> None:
        if self._klines_ws is None:
            return
        if self._klines_passthrough_mode:
            await self._passthrough_buffer_add(kline)
            return

        complete_rows: dict[str, dict] | None = None
        first_ingest_ms: int | None = None
        min_required = self._full_batch_min_required()
        async with self._pending_lock:
            if kline.close_time_ms in self._published_close_times:
                return
            rows_by_symbol = self._pending_by_close.setdefault(kline.close_time_ms, {})
            self._pending_first_ingest_ms.setdefault(kline.close_time_ms, kline.ingest_time_ms)
            rows_by_symbol[kline.symbol] = self._kline_row_dict(kline)
            if kline.source == "ws":
                self._pending_has_ws[kline.close_time_ms] = True
            if len(rows_by_symbol) >= min_required:
                complete_rows = self._pending_by_close.pop(kline.close_time_ms, {})
                self._pending_has_ws.pop(kline.close_time_ms, None)
                first_ingest_ms = self._pending_first_ingest_ms.pop(kline.close_time_ms, None)
                self._mark_close_time_published_locked(kline.close_time_ms)

        if complete_rows:
            await self._enqueue_publish(
                close_time_ms=kline.close_time_ms,
                rows_by_symbol=complete_rows,
                first_ingest_ms=first_ingest_ms,
            )

    async def _flush_close_time(
        self,
        topic: str,
        close_time_ms: int,
        rows_by_symbol: dict[str, dict],
        first_ingest_ms: int | None = None,
        notify_summary: bool = True,
        track_latency: bool = True,
        publish_derived: bool = True,
    ) -> None:
        if not rows_by_symbol or self._klines_ws is None:
            return
        publish_ms = int(time.time() * 1000)
        payload = [dict(row, service_publish_ms=publish_ms, topic=topic) for row in rows_by_symbol.values()]
        payload.sort(key=lambda row: row["symbol"])
        if track_latency:
            await self._record_published_latency_state(payload, topic, send_ack_summary=notify_summary)
        await self._remember_recent_payload(topic, payload)
        batch_done = False
        batch_sent_shards = 0
        batch_total_shards = 0
        if topic == self._klines_all_topic and self._klines_passthrough_mode and payload:
            shard_id = str(payload[0].get("ws_shard_id") or "")
            if shard_id:
                total_from_payload = int(payload[0].get("ws_total_shards") or 0)
                sent = self._passthrough_shards_sent_by_close.setdefault(close_time_ms, set())
                sent.add(shard_id)
                total = self._passthrough_total_shards_by_close.get(close_time_ms, 0)
                total = max(total, total_from_payload)
                self._passthrough_total_shards_by_close[close_time_ms] = total
                batch_sent_shards = len(sent)
                batch_total_shards = total
                batch_done = total > 0 and batch_sent_shards >= total
            for row in payload:
                row["batch_sent_shards"] = batch_sent_shards
                row["batch_total_shards"] = batch_total_shards
                row["batch_done"] = batch_done
        elif payload:
            # 聚合模式：一次 flush 即整批对外推送，便于下游 batch_ack 对齐 batch_push_done_ms
            batch_done = True
            batch_sent_shards = 1
            batch_total_shards = 1
            for row in payload:
                row["batch_sent_shards"] = batch_sent_shards
                row["batch_total_shards"] = batch_total_shards
                row["batch_done"] = True
        batch_push_done_ms = int(time.time() * 1000)
        for row in payload:
            row["batch_push_done_ms"] = batch_push_done_ms
        pushed_clients = await self._klines_ws.broadcast_json(payload, topic=topic)
        if track_latency:
            self._enqueue_latency_events(self._build_published_latency_events(payload, topic))
        push_done_ms = int(time.time() * 1000)
        if batch_done:
            self._batch_done_publish_ms_by_close[(topic, close_time_ms)] = batch_push_done_ms
        service_to_client_no_ack_ms = max(0, push_done_ms - publish_ms)
        if pushed_clients > 0:
            self._push_1h += pushed_clients * len(payload)
        got_symbols = len(rows_by_symbol)
        expected_symbol_set: set[str] | None = None
        if topic == self._klines_all_topic:
            expected_symbol_set = self._expected_symbols
        elif topic in (self._major_1h_topic, self._major_1m_topic):
            expected_symbol_set = self._major_expected_symbols_by_topic.get(topic, self._major_symbol_set)
        if topic == self._klines_all_topic and self._klines_passthrough_mode:
            expected_symbols = self._ack_expected_by_close.get((topic, close_time_ms), got_symbols)
            missing_symbols = 0
            got_ratio = 1.0
            missing_list: list[str] = []
        else:
            expected_symbols = len(expected_symbol_set) if expected_symbol_set is not None else got_symbols
            missing_symbols = expected_symbols - got_symbols
            got_ratio = (got_symbols / expected_symbols) if expected_symbols > 0 else 0.0
            missing_list = sorted((expected_symbol_set or set()) - set(rows_by_symbol.keys()))
        missing_sample = ",".join(missing_list[:20]) if missing_list else "-"
        ws_count = sum(1 for row in payload if row.get("source") == "ws")
        rest_count = len(payload) - ws_count
        full_batch_wait_ms = None
        if first_ingest_ms is not None:
            full_batch_wait_ms = max(0, publish_ms - first_ingest_ms)
        min_required_for_log = 1 if self._klines_passthrough_mode else max(
            1,
            math.ceil(expected_symbols * self._min_publish_ratio),
        )
        logger.info(
            "klines.batch.published topic=%s close_time_ms=%s batch_size=%s got_symbols=%s expected_symbols=%s got_ratio=%.3f ws_count=%s rest_count=%s min_required=%s missing_symbols=%s missing_sample=%s full_batch_wait_ms=%s full_batch_only=%s min_publish_ratio=%.3f pushed_clients=%s pushed_rows_total=%s",
            topic,
            close_time_ms,
            len(payload),
            got_symbols,
            expected_symbols,
            got_ratio,
            ws_count,
            rest_count,
            min_required_for_log,
            max(0, missing_symbols),
            missing_sample,
            full_batch_wait_ms,
            self._full_batch_only,
            self._min_publish_ratio,
            pushed_clients,
            self._push_1h,
        )
        if notify_summary and self._log_full_missing_symbols and missing_list:
            logger.info(
                "klines.batch.missing_full topic=%s close_time_ms=%s missing_symbols=%s missing_list=%s",
                topic,
                close_time_ms,
                len(missing_list),
                ",".join(missing_list),
            )
        if notify_summary:
            await self._notify_kline_batch(
                payload,
                topic,
                expected_symbols,
                missing_symbols,
                ws_count,
                rest_count,
                service_to_client_no_ack_ms,
                pushed_clients,
                publish_ms,
                push_done_ms,
            )
        if publish_derived and topic == self._klines_all_topic:
            await self._publish_derived_major_topics(close_time_ms, rows_by_symbol, first_ingest_ms)

    async def _publish_derived_major_topics(
        self,
        close_time_ms: int,
        rows_by_symbol: dict[str, dict],
        first_ingest_ms: int | None,
    ) -> None:
        if not self._major_symbol_set:
            return
        major_rows = {symbol: rows_by_symbol[symbol] for symbol in self._major_symbols if symbol in rows_by_symbol}
        if not major_rows:
            return
        if self._klines_interval == self._major_1h_interval and not self._should_run_dedicated_major_1h():
            await self._flush_close_time(
                self._major_1h_topic,
                close_time_ms,
                major_rows,
                first_ingest_ms,
                notify_summary=True,
                track_latency=True,
                publish_derived=False,
            )
        if (
            self._major_1m_enabled
            and self._klines_interval == self._major_1m_interval
            and not self._should_run_dedicated_major_1m()
        ):
            await self._flush_close_time(
                self._major_1m_topic,
                close_time_ms,
                major_rows,
                first_ingest_ms,
                notify_summary=True,
                track_latency=True,
                publish_derived=False,
            )

    async def _run_major_interval_publish(
        self,
        symbols: list[str],
        interval: str,
        topic: str,
        stream_key: str | None = None,
        mirror_to_all_topic: bool = False,
    ) -> None:
        expected_symbols = set(symbols)
        self._major_expected_symbols_by_topic[topic] = expected_symbols
        if self._klines_ws is None:
            async for _ in self._binance.iter_closed_klines(
                symbols,
                interval=interval,
                batch_size=max(1, len(symbols)),
                stream_key=stream_key,
            ):
                self._ingest_1h += 1
            return

        latest_close_by_symbol: dict[str, int] = {}
        async for kline in self._binance.iter_closed_klines(
            symbols,
            interval=interval,
            batch_size=max(1, len(symbols)),
            stream_key=stream_key,
        ):
            prev_close = latest_close_by_symbol.get(kline.symbol)
            if prev_close is not None and kline.close_time_ms <= prev_close:
                continue
            latest_close_by_symbol[kline.symbol] = kline.close_time_ms
            self._ingest_1h += 1
            if not self._should_publish_major_kline_topic(topic, kline.interval, kline.close_time_ms):
                continue
            complete_rows: dict[str, dict] | None = None
            first_ingest_ms: int | None = None
            batch_key = (topic, kline.close_time_ms)
            async with self._major_pending_lock:
                if batch_key not in self._major_published_close_times:
                    rows_by_symbol = self._major_pending_by_close.setdefault(batch_key, {})
                    self._major_pending_first_ingest_ms.setdefault(batch_key, kline.ingest_time_ms)
                    rows_by_symbol[kline.symbol] = self._kline_row_dict(kline)
                    if len(rows_by_symbol) >= len(expected_symbols):
                        complete_rows = self._major_pending_by_close.pop(batch_key, {})
                        first_ingest_ms = self._major_pending_first_ingest_ms.pop(batch_key, None)
                        self._mark_major_close_time_published_locked(topic, kline.close_time_ms)

            if complete_rows:
                await self._enqueue_topic_publish(
                    topic,
                    kline.close_time_ms,
                    complete_rows,
                    first_ingest_ms,
                    notify_summary=True,
                    track_latency=True,
                )
            if mirror_to_all_topic:
                await self._add_full_topic_kline(kline)

    async def _add_major_topic_kline(
        self,
        kline: ClosedKline,
        topic: str,
        expected_symbols: set[str],
    ) -> None:
        if self._klines_ws is None or not topic or not expected_symbols:
            return
        if not self._should_publish_major_kline_topic(topic, kline.interval, kline.close_time_ms):
            return
        self._major_expected_symbols_by_topic[topic] = expected_symbols
        complete_rows: dict[str, dict] | None = None
        first_ingest_ms: int | None = None
        batch_key = (topic, kline.close_time_ms)
        async with self._major_pending_lock:
            if batch_key in self._major_published_close_times:
                return
            rows_by_symbol = self._major_pending_by_close.setdefault(batch_key, {})
            self._major_pending_first_ingest_ms.setdefault(batch_key, kline.ingest_time_ms)
            rows_by_symbol[kline.symbol] = self._kline_row_dict(kline)
            if len(rows_by_symbol) >= len(expected_symbols):
                complete_rows = self._major_pending_by_close.pop(batch_key, {})
                first_ingest_ms = self._major_pending_first_ingest_ms.pop(batch_key, None)
                self._mark_major_close_time_published_locked(topic, kline.close_time_ms)

        if complete_rows:
            await self._enqueue_topic_publish(
                topic,
                kline.close_time_ms,
                complete_rows,
                first_ingest_ms,
                notify_summary=True,
                track_latency=True,
            )

    def _should_publish_major_kline_topic(self, topic: str, interval: str, close_time_ms: int) -> bool:
        if topic != self._major_1m_topic or interval != self._major_1m_interval:
            return True
        cadence = self._major_1m_publish_every_minutes
        if cadence <= 1:
            return True
        # Binance close_time_ms is the last millisecond of the candle. Add 1ms
        # to align checks to the next minute boundary.
        boundary = datetime.fromtimestamp((close_time_ms + 1) / 1000)
        return boundary.minute % cadence == 0

    async def _run_major_stale_flush(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(0.5)
            now_ms = int(time.time() * 1000)
            flush_items: list[tuple[str, int, dict[str, dict], int | None]] = []
            async with self._major_pending_lock:
                candidate_keys: list[tuple[str, int]] = []
                for batch_key, rows_by_symbol in self._major_pending_by_close.items():
                    if not rows_by_symbol:
                        continue
                    first_ingest_ms = self._major_pending_first_ingest_ms.get(batch_key)
                    if first_ingest_ms is None:
                        continue
                    if now_ms - first_ingest_ms >= self._major_batch_max_wait_ms:
                        candidate_keys.append(batch_key)
                for batch_key in sorted(candidate_keys):
                    topic, close_time_ms = batch_key
                    rows_by_symbol = self._major_pending_by_close.pop(batch_key, {})
                    first_ingest_ms = self._major_pending_first_ingest_ms.pop(batch_key, None)
                    if rows_by_symbol:
                        self._mark_major_close_time_published_locked(topic, close_time_ms)
                        flush_items.append((topic, close_time_ms, rows_by_symbol, first_ingest_ms))

            for topic, close_time_ms, rows_by_symbol, first_ingest_ms in flush_items:
                expected_symbol_set = self._major_expected_symbols_by_topic.get(topic, self._major_symbol_set)
                missing = sorted(expected_symbol_set - set(rows_by_symbol.keys()))
                logger.warning(
                    "klines.major.batch.timeout_flush topic=%s close_time_ms=%s got=%s expected=%s missing=%s max_wait_ms=%s",
                    topic,
                    close_time_ms,
                    len(rows_by_symbol),
                    len(expected_symbol_set),
                    ",".join(missing) if missing else "-",
                    self._major_batch_max_wait_ms,
                )
                await self._enqueue_topic_publish(
                    topic,
                    close_time_ms,
                    rows_by_symbol,
                    first_ingest_ms,
                    notify_summary=True,
                    track_latency=True,
                )

    async def _flush_stale_batches(self, now_ms: int) -> None:
        if self._klines_ws is None:
            return
        async with self._pending_lock:
            stale_close_times = [
                close_time_ms
                for close_time_ms in self._pending_by_close.keys()
                if self._pending_has_ws.get(close_time_ms, False)
                and close_time_ms + self._flush_grace_ms <= now_ms
            ]
            stale_batches: list[tuple[int, dict[str, dict]]] = []
            for close_time_ms in sorted(stale_close_times):
                rows_by_symbol = self._pending_by_close.pop(close_time_ms, {})
                self._pending_has_ws.pop(close_time_ms, None)
                if rows_by_symbol:
                    first_ingest_ms = self._pending_first_ingest_ms.pop(close_time_ms, None)
                    self._mark_close_time_published_locked(close_time_ms)
                    stale_batches.append((close_time_ms, rows_by_symbol, first_ingest_ms))

        for close_time_ms, rows_by_symbol, first_ingest_ms in stale_batches:
            missing = len(self._expected_symbols) - len(rows_by_symbol)
            if missing > 0:
                logger.warning(
                    "klines.batch.timeout_flush close_time_ms=%s got=%s expected=%s missing=%s grace_ms=%s",
                    close_time_ms,
                    len(rows_by_symbol),
                    len(self._expected_symbols),
                    missing,
                    self._flush_grace_ms,
                )
            await self._enqueue_publish(close_time_ms=close_time_ms, rows_by_symbol=rows_by_symbol, first_ingest_ms=first_ingest_ms)

    async def _run_klines_stale_flush(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1)
            now_ms = int(time.time() * 1000)
            if self._full_batch_only:
                await self._flush_waited_tolerant_batches(now_ms)
                await self._try_rest_repair_for_waiting_batches(now_ms)
                await self._log_full_batch_waiting(now_ms)
            else:
                await self._flush_stale_batches(now_ms)

    async def _flush_waited_tolerant_batches(self, now_ms: int) -> None:
        if self._klines_ws is None:
            return
        if self._max_missing_symbols <= 0:
            return
        expected_symbols = len(self._expected_symbols)
        tolerant_required = max(1, expected_symbols - self._max_missing_symbols)
        flush_items: list[tuple[int, dict[str, dict], int | None]] = []
        async with self._pending_lock:
            candidate_close_times = []
            for close_time_ms, rows_by_symbol in self._pending_by_close.items():
                if not self._pending_has_ws.get(close_time_ms, False):
                    continue
                first_ingest_ms = self._pending_first_ingest_ms.get(close_time_ms)
                if first_ingest_ms is None:
                    continue
                wait_ms = max(0, now_ms - first_ingest_ms)
                got = len(rows_by_symbol)
                if wait_ms >= self._full_batch_max_wait_ms and got >= tolerant_required:
                    candidate_close_times.append(close_time_ms)
            for close_time_ms in sorted(candidate_close_times):
                rows_by_symbol = self._pending_by_close.pop(close_time_ms, {})
                self._pending_has_ws.pop(close_time_ms, None)
                first_ingest_ms = self._pending_first_ingest_ms.pop(close_time_ms, None)
                if rows_by_symbol:
                    self._mark_close_time_published_locked(close_time_ms)
                    flush_items.append((close_time_ms, rows_by_symbol, first_ingest_ms))

        for close_time_ms, rows_by_symbol, first_ingest_ms in flush_items:
            missing = max(0, expected_symbols - len(rows_by_symbol))
            logger.warning(
                "klines.batch.max_wait_tolerant_flush close_time_ms=%s got=%s expected=%s missing=%s max_missing_symbols=%s max_wait_ms=%s",
                close_time_ms,
                len(rows_by_symbol),
                expected_symbols,
                missing,
                self._max_missing_symbols,
                self._full_batch_max_wait_ms,
            )
            await self._enqueue_publish(close_time_ms=close_time_ms, rows_by_symbol=rows_by_symbol, first_ingest_ms=first_ingest_ms)

    async def _try_rest_repair_for_waiting_batches(self, now_ms: int) -> None:
        if self._klines_ws is None:
            return
        if self._rest_repair_delay_ms <= 0:
            return

        min_required = max(1, math.ceil(len(self._expected_symbols) * self._min_publish_ratio))
        repair_targets: list[tuple[int, int]] = []
        async with self._pending_lock:
            for close_time_ms, rows_by_symbol in self._pending_by_close.items():
                if close_time_ms in self._rest_repairing_close_times:
                    continue
                first_ingest_ms = self._pending_first_ingest_ms.get(close_time_ms)
                if first_ingest_ms is None:
                    continue
                if len(rows_by_symbol) >= min_required:
                    continue
                wait_ms = max(0, now_ms - first_ingest_ms)
                if wait_ms < self._rest_repair_delay_ms:
                    continue
                self._rest_repairing_close_times.add(close_time_ms)
                repair_targets.append((close_time_ms, wait_ms))

        for close_time_ms, wait_ms in repair_targets:
            try:
                await self._repair_close_time_with_rest(close_time_ms, wait_ms, min_required)
            finally:
                async with self._pending_lock:
                    self._rest_repairing_close_times.discard(close_time_ms)

    async def _repair_close_time_with_rest(self, close_time_ms: int, wait_ms: int, min_required: int) -> None:
        async with self._pending_lock:
            rows_by_symbol = self._pending_by_close.get(close_time_ms)
            if not rows_by_symbol:
                return
            missing_symbols = sorted(self._expected_symbols - set(rows_by_symbol.keys()))
            first_ingest_ms = self._pending_first_ingest_ms.get(close_time_ms)
        if not missing_symbols:
            return
        expected_symbols = len(self._expected_symbols)
        publish_required = min_required
        if self._max_missing_symbols > 0 and wait_ms >= self._full_batch_max_wait_ms:
            tolerant_required = max(1, expected_symbols - self._max_missing_symbols)
            publish_required = min(publish_required, tolerant_required)

        logger.warning(
            "klines.batch.rest_repair.start close_time_ms=%s missing_symbols=%s wait_ms=%s repair_delay_ms=%s publish_required=%s",
            close_time_ms,
            len(missing_symbols),
            wait_ms,
            self._rest_repair_delay_ms,
            publish_required,
        )

        timeout_seconds = max(5.0, self._cfg.backfill.rest_timeout_seconds)
        repaired = 0
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds)) as client:
            for symbol in missing_symbols:
                async with self._pending_lock:
                    rows_now = self._pending_by_close.get(close_time_ms)
                    if not rows_now:
                        return
                    if len(rows_now) >= publish_required:
                        break
                    if symbol in rows_now:
                        continue
                try:
                    kline = await self._binance.fetch_exact_closed_kline_rest(
                        client,
                        symbol,
                        self._klines_interval,
                        close_time_ms,
                    )
                except Exception as exc:
                    logger.warning(
                        "klines.batch.rest_repair.fail close_time_ms=%s symbol=%s error=%s",
                        close_time_ms,
                        symbol,
                        exc,
                    )
                    continue
                if kline is None:
                    continue
                self._ingest_1h += 1
                got_after_repair = 0
                async with self._pending_lock:
                    rows_now = self._pending_by_close.get(close_time_ms)
                    if rows_now is None or symbol in rows_now:
                        continue
                    rows_now[symbol] = self._kline_row_dict(kline)
                    got_after_repair = len(rows_now)
                repaired += 1
                if got_after_repair >= publish_required:
                    break

        maybe_flush_rows: dict[str, dict] | None = None
        async with self._pending_lock:
            rows_now = self._pending_by_close.get(close_time_ms, {})
            got = len(rows_now)
            if got >= publish_required:
                maybe_flush_rows = self._pending_by_close.pop(close_time_ms, {})
                self._pending_has_ws.pop(close_time_ms, None)
                first_ingest_ms = self._pending_first_ingest_ms.pop(close_time_ms, first_ingest_ms)
                self._mark_close_time_published_locked(close_time_ms)
        logger.warning(
            "klines.batch.rest_repair.done close_time_ms=%s repaired=%s got_symbols=%s min_required=%s publish_required=%s",
            close_time_ms,
            repaired,
            len(maybe_flush_rows) if maybe_flush_rows is not None else got,
            min_required,
            publish_required,
        )
        if maybe_flush_rows:
            await self._enqueue_publish(close_time_ms=close_time_ms, rows_by_symbol=maybe_flush_rows, first_ingest_ms=first_ingest_ms)

    async def _log_full_batch_waiting(self, now_ms: int) -> None:
        if not self._full_batch_only:
            return
        async with self._pending_lock:
            waiting_rows = [
                (
                    close_time_ms,
                    len(rows_by_symbol),
                    self._pending_first_ingest_ms.get(close_time_ms),
                )
                for close_time_ms, rows_by_symbol in self._pending_by_close.items()
                if self._pending_has_ws.get(close_time_ms, False)
            ]
        for close_time_ms, got, first_ingest_ms in waiting_rows:
            if first_ingest_ms is None:
                continue
            wait_ms = max(0, now_ms - first_ingest_ms)
            if wait_ms >= self._full_batch_max_wait_ms:
                expected_symbols = len(self._expected_symbols)
                got_ratio = (got / expected_symbols) if expected_symbols > 0 else 0.0
                logger.warning(
                    "klines.batch.waiting_full close_time_ms=%s got=%s expected=%s got_ratio=%.3f min_required=%s wait_ms=%s max_wait_ms=%s repair_delay_ms=%s min_publish_ratio=%.3f",
                    close_time_ms,
                    got,
                    expected_symbols,
                    got_ratio,
                    max(1, math.ceil(expected_symbols * self._min_publish_ratio)),
                    wait_ms,
                    self._full_batch_max_wait_ms,
                    self._rest_repair_delay_ms,
                    self._min_publish_ratio,
                )

    def _resolve_rest_repair_delay_ms(self, interval: str) -> int:
        configured = self._cfg.websocket.klines_rest_repair_delay_ms
        if configured > 0:
            return configured
        if interval == "5m":
            return 120_000
        if interval == "1h":
            return 600_000
        # default fallback for other intervals: 2 minutes
        return 120_000

    def _mark_close_time_published_locked(self, close_time_ms: int) -> None:
        if close_time_ms in self._published_close_times:
            return
        self._published_close_times.add(close_time_ms)
        self._published_close_order.append(close_time_ms)
        while len(self._published_close_order) > self._published_close_max:
            expired = self._published_close_order.popleft()
            self._published_close_times.discard(expired)

    def _mark_major_close_time_published_locked(self, topic: str, close_time_ms: int) -> None:
        key = (topic, close_time_ms)
        if key in self._major_published_close_times:
            return
        self._major_published_close_times.add(key)
        self._major_published_close_order.append(key)
        while len(self._major_published_close_order) > self._major_published_close_max:
            expired = self._major_published_close_order.popleft()
            self._major_published_close_times.discard(expired)

    async def _on_klines_client_connected(self, ws: WebSocketServerProtocol | WebSocket) -> None:
        topics = await self._topics_for_client(ws)
        await self._replay_klines_client(ws, topics)

    async def _topics_for_client(self, ws: WebSocketServerProtocol | WebSocket) -> set[str]:
        topics_for = getattr(self._klines_ws, "topics_for", None)
        if callable(topics_for):
            try:
                topics = set(await topics_for(ws))
            except Exception:
                topics = set()
        else:
            topics = set()
        if not topics:
            topics.add(self._klines_all_topic)
        return topics

    async def _send_ws_text(self, ws: WebSocketServerProtocol | WebSocket, text: str) -> None:
        send_text = getattr(ws, "send_text", None)
        if callable(send_text):
            await send_text(text)
            return
        await ws.send(text)

    async def _replay_klines_client(self, ws: WebSocketServerProtocol | WebSocket, topics: set[str]) -> None:
        if self._klines_recent_maxlen <= 0:
            return
        async with self._klines_recent_lock:
            if "*" in topics:
                replay_payloads = [
                    payload
                    for topic_payloads in self._klines_recent_by_topic.values()
                    for payload in list(topic_payloads)
                ]
            else:
                replay_payloads = [
                    payload
                    for topic in topics
                    for payload in list(self._klines_recent_by_topic.get(topic, ()))
                ]
        if not replay_payloads:
            return
        sent = 0
        for payload in replay_payloads:
            publish_ms = int(time.time() * 1000)
            replay_payload = [
                {
                    **item,
                    "replay": True,
                    "replay_reason": "cold_start",
                    "service_publish_ms": publish_ms,
                }
                for item in payload
            ]
            try:
                await self._send_ws_text(ws, orjson.dumps(replay_payload).decode("utf-8"))
                sent += 1
            except Exception:
                break
        logger.info("ws.client.replay name=klines topics=%s replayed=%s", sorted(topics), sent)

    async def _enqueue_publish(self, close_time_ms: int, rows_by_symbol: dict[str, dict], first_ingest_ms: int | None) -> None:
        await self._enqueue_topic_publish(
            self._klines_all_topic,
            close_time_ms,
            rows_by_symbol,
            first_ingest_ms,
            notify_summary=True,
            track_latency=True,
        )

    async def _enqueue_topic_publish(
        self,
        topic: str,
        close_time_ms: int,
        rows_by_symbol: dict[str, dict],
        first_ingest_ms: int | None,
        notify_summary: bool,
        track_latency: bool,
    ) -> None:
        if not rows_by_symbol:
            return
        await self._publish_queue.put(
            (topic, close_time_ms, rows_by_symbol, first_ingest_ms, notify_summary, track_latency),
        )

    async def _run_publish_worker(self, stop: asyncio.Event) -> None:
        while True:
            if stop.is_set() and self._publish_queue.empty():
                break
            try:
                topic, close_time_ms, rows_by_symbol, first_ingest_ms, notify_summary, track_latency = (
                    await asyncio.wait_for(self._publish_queue.get(), timeout=0.5)
                )
            except TimeoutError:
                continue
            try:
                await self._flush_close_time(
                    topic,
                    close_time_ms,
                    rows_by_symbol,
                    first_ingest_ms,
                    notify_summary=notify_summary,
                    track_latency=track_latency,
                )
            except Exception:
                logger.exception("klines.publish_worker.failed topic=%s close_time_ms=%s", topic, close_time_ms)
            finally:
                self._publish_queue.task_done()

    async def _remember_recent_payload(self, topic: str, payload: list[dict]) -> None:
        if self._klines_recent_maxlen <= 0 or not payload:
            return
        async with self._klines_recent_lock:
            topic_recent = self._klines_recent_by_topic.get(topic)
            if topic_recent is None:
                topic_recent = deque(maxlen=self._klines_recent_maxlen)
                self._klines_recent_by_topic[topic] = topic_recent
            topic_recent.append(payload)

    async def _record_published_latency_state(
        self,
        payload: list[dict],
        topic: str,
        send_ack_summary: bool,
    ) -> None:
        if not payload:
            return
        cap = self._cfg.redis.pending_latency_max
        for row in payload:
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            try:
                close_time_ms = int(row.get("close_time_ms") or 0)
            except (TypeError, ValueError):
                close_time_ms = 0
            try:
                ev = LatencyEvent(
                    event_id=event_id,
                    symbol=str(row.get("symbol") or ""),
                    interval=str(row.get("interval") or ""),
                    exchange_event_ms=int(row.get("exchange_event_ms") or 0),
                    service_ingest_ms=int(row.get("service_ingest_ms") or 0),
                    service_publish_ms=int(row.get("service_publish_ms") or 0),
                    service_region=self._cfg.service_region,
                )
            except (TypeError, ValueError):
                continue
            async with self._pending_latency_lock:
                while len(self._pending_latency) >= cap:
                    drop = next(iter(self._pending_latency))
                    self._pending_latency.pop(drop, None)
                event_key = (topic, event_id)
                self._pending_latency[event_key] = ev
                if close_time_ms > 0:
                    self._event_close_time[event_key] = (topic, close_time_ms)
        if payload:
            try:
                close_time_ms = int(payload[0].get("close_time_ms") or 0)
            except (TypeError, ValueError):
                close_time_ms = 0
            if close_time_ms > 0:
                batch_key = (topic, close_time_ms)
                self._ack_expected_by_close[batch_key] = self._ack_expected_by_close.get(batch_key, 0) + len(payload)
                self._ack_service_to_downstream_by_close.setdefault(batch_key, [])
                if batch_key not in self._ack_notify_scheduled:
                    self._ack_notify_scheduled.add(batch_key)
                    asyncio.create_task(
                        self._notify_ack_summary_later(
                            batch_key,
                            delay_seconds=20,
                            send_notification=send_ack_summary,
                        ),
                    )

    def _build_published_latency_events(self, payload: list[dict], topic: str) -> list[dict[str, str]]:
        events: list[dict[str, str]] = []
        for row in payload:
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            interval = str(row.get("interval") or "")
            symbol = str(row.get("symbol") or "")
            try:
                exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                service_ingest_ms = int(row.get("service_ingest_ms") or 0)
            except (TypeError, ValueError):
                continue
            exchange_to_service_ms = max(0, service_ingest_ms - exchange_event_ms)
            events.append(
                {
                    "event_id": event_id,
                    "symbol": symbol,
                    "interval": interval,
                    "topic": topic,
                    "exchange_to_service_ms": str(exchange_to_service_ms),
                    "service_region": self._cfg.service_region,
                    "phase": "published",
                },
            )
        return events

    def _enqueue_latency_events(self, events: list[dict[str, str]]) -> None:
        if not events:
            return
        dropped = 0
        for event in events:
            try:
                self._latency_write_queue.put_nowait(event)
            except asyncio.QueueFull:
                dropped += 1
        if dropped > 0:
            logger.warning("latency.queue.full dropped=%s", dropped)

    async def _run_latency_writer(self, stop: asyncio.Event) -> None:
        stream = self._cfg.redis.stream_latency_events
        maxlen = self._cfg.redis.stream_maxlen
        while True:
            if stop.is_set() and self._latency_write_queue.empty():
                break
            try:
                first = await asyncio.wait_for(self._latency_write_queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            batch = [first]
            while len(batch) < 512:
                try:
                    batch.append(self._latency_write_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                pipe = self._latency_redis.pipeline(transaction=False)
                for fields in batch:
                    pipe.xadd(stream, fields=fields, maxlen=maxlen, approximate=True)
                await pipe.execute()
            except Exception:
                logger.exception("latency.writer.persist_failed batch=%s", len(batch))
            finally:
                for _ in batch:
                    self._latency_write_queue.task_done()

    async def _on_klines_client_message(self, _ws: WebSocket, message: str) -> None:
        try:
            payload = orjson.loads(message)
        except Exception:
            return

        ack_items: list[dict] = []
        batch_ack_item: dict | None = None
        batch_integrated_ack_item: dict | None = None
        batch_compute_ack_item: dict | None = None
        batch_order_ack_item: dict | None = None
        if isinstance(payload, dict):
            msg_type = str(payload.get("type") or "")
            if msg_type in ("subscribe", "set_topics", "unsubscribe"):
                await self._apply_topic_subscription(_ws, payload, msg_type)
                return
            if msg_type == "batch_ack":
                batch_ack_item = payload
            elif msg_type == "batch_integrated_ack":
                batch_integrated_ack_item = payload
            elif msg_type == "batch_compute_ack":
                batch_compute_ack_item = payload
            elif msg_type == "batch_order_ack":
                batch_order_ack_item = payload
            elif "event_id" in payload:
                ack_items = [payload]
            elif isinstance(payload.get("acks"), list):
                ack_items = [x for x in payload["acks"] if isinstance(x, dict)]
        elif isinstance(payload, list):
            ack_items = [x for x in payload if isinstance(x, dict)]

        sender_client_id = id(_ws)
        if batch_ack_item is not None:
            await self._apply_batch_ack(batch_ack_item, sender_client_id)
        if batch_integrated_ack_item is not None:
            await self._apply_batch_integrated_ack(batch_integrated_ack_item, sender_client_id)
        if batch_compute_ack_item is not None:
            await self._apply_batch_compute_ack(batch_compute_ack_item, sender_client_id)
        if batch_order_ack_item is not None:
            await self._apply_batch_order_ack(batch_order_ack_item, sender_client_id)
        if not ack_items:
            return
        for item in ack_items:
            await self._apply_downstream_ack(item, sender_client_id)

    async def _apply_topic_subscription(self, ws: WebSocket, payload: dict, msg_type: str) -> None:
        requested = payload.get("topics", payload.get("topic"))
        if requested is None:
            return
        hub = self._klines_ws
        if hub is None:
            return
        if msg_type == "set_topics":
            set_topics = getattr(hub, "set_topics", None)
            if callable(set_topics):
                await set_topics(ws, requested)
        elif msg_type == "unsubscribe":
            remove_topics = getattr(hub, "remove_topics", None)
            if callable(remove_topics):
                await remove_topics(ws, requested)
        else:
            add_topics = getattr(hub, "add_topics", None)
            if callable(add_topics):
                await add_topics(ws, requested)
        topics = await self._topics_for_client(ws)
        await self._replay_klines_client(ws, topics)

    def _ack_topic(self, item: dict) -> str:
        return str(item.get("topic") or self._klines_all_topic)

    def _batch_key_for_ack(self, item: dict, close_time_ms: int) -> tuple[str, int]:
        batch_key = (self._ack_topic(item), close_time_ms)
        if item.get("topic") or batch_key in self._batch_done_publish_ms_by_close:
            return batch_key
        matches = [key for key in self._batch_done_publish_ms_by_close.keys() if key[1] == close_time_ms]
        if len(matches) == 1:
            return matches[0]
        return batch_key

    async def _apply_batch_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            downstream_receive_ms = int(item.get("downstream_receive_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        batch_key = self._batch_key_for_ack(item, close_time_ms)
        done_publish_ms = self._batch_done_publish_ms_by_close.get(batch_key)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif downstream_receive_ms > 0:
            done_to_ack_ms = max(0, downstream_receive_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_ack_by_close.setdefault(batch_key, []).append(done_to_ack_ms)
        if sender_client_id is not None:
            self._batch_ack_clients_by_close.setdefault(batch_key, set()).add(sender_client_id)

    async def _apply_batch_integrated_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            integrated_ack_ms = int(item.get("downstream_integrated_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        batch_key = self._batch_key_for_ack(item, close_time_ms)
        done_publish_ms = self._batch_done_publish_ms_by_close.get(batch_key)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_integrated_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif integrated_ack_ms > 0:
            done_to_integrated_ack_ms = max(0, integrated_ack_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_integrated_ack_by_close.setdefault(batch_key, []).append(done_to_integrated_ack_ms)
        if sender_client_id is not None:
            self._batch_integrated_ack_clients_by_close.setdefault(batch_key, set()).add(sender_client_id)

    async def _apply_batch_compute_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            compute_ack_ms = int(item.get("downstream_compute_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        batch_key = self._batch_key_for_ack(item, close_time_ms)
        done_publish_ms = self._batch_done_publish_ms_by_close.get(batch_key)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_compute_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif compute_ack_ms > 0:
            done_to_compute_ack_ms = max(0, compute_ack_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_compute_ack_by_close.setdefault(batch_key, []).append(done_to_compute_ack_ms)
        if sender_client_id is not None:
            self._batch_compute_ack_clients_by_close.setdefault(batch_key, set()).add(sender_client_id)

    async def _apply_batch_order_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            order_ack_ms = int(item.get("downstream_order_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        batch_key = (self._ack_topic(item), close_time_ms)
        done_publish_ms = self._batch_done_publish_ms_by_close.get(batch_key)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_order_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif order_ack_ms > 0:
            done_to_order_ack_ms = max(0, order_ack_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_order_ack_by_close.setdefault(batch_key, []).append(done_to_order_ack_ms)
        if sender_client_id is not None:
            self._batch_order_ack_clients_by_close.setdefault(batch_key, set()).add(sender_client_id)

    async def _apply_downstream_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        event_id = str(item.get("event_id") or "")
        if not event_id:
            return
        raw_receive_ms = item.get("downstream_receive_ms")
        if isinstance(raw_receive_ms, bool) or not isinstance(raw_receive_ms, (int, str)):
            return
        try:
            downstream_receive_ms = int(raw_receive_ms)
        except (TypeError, ValueError):
            return
        if downstream_receive_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        downstream_region = str(item.get("downstream_region") or "")
        topic = self._ack_topic(item)
        event_key = (topic, event_id)

        async with self._pending_latency_lock:
            ev = self._pending_latency.get(event_key)
            if ev is None and not item.get("topic"):
                matches = [key for key in self._pending_latency.keys() if key[1] == event_id]
                if len(matches) == 1:
                    event_key = matches[0]
                    topic = event_key[0]
                    ev = self._pending_latency.get(event_key)
            if ev is not None:
                # Client receipt times require synchronized clocks. Reject impossible
                # values without consuming the event, so a corrected ACK can retry.
                earliest_ms = max(ev.exchange_event_ms, ev.service_ingest_ms, ev.service_publish_ms)
                if not earliest_ms <= downstream_receive_ms <= server_ingress_ms:
                    logger.info("ack.invalid_receive_time topic=%s event_id=%s", topic, event_id)
                    return
                self._pending_latency.pop(event_key, None)
                batch_key = self._event_close_time.pop(event_key, None)
        if ev is None:
            logger.info("ack.unknown topic=%s event_id=%s", topic, event_id)
            return

        ev.downstream_receive_ms = downstream_receive_ms
        ev.downstream_region = downstream_region or None
        svc_to_downstream = ev.service_to_downstream_ms()
        e2e = ev.end_to_end_ms()
        region_ok = ev.region_aligned()
        logger.info(
            "latency.final event_id=%s symbol=%s e2e_ms=%s exchange_to_service_ms=%s "
            "service_to_downstream_ms=%s service_region=%s downstream_region=%s region_aligned=%s",
            ev.event_id,
            ev.symbol,
            e2e,
            ev.exchange_to_service_ms(),
            svc_to_downstream,
            ev.service_region,
            ev.downstream_region,
            region_ok,
        )
        if batch_key is not None:
            self._ack_service_to_downstream_by_close.setdefault(batch_key, []).append(int(svc_to_downstream))
            ingest_to_downstream = downstream_receive_ms - ev.service_ingest_ms
            self._ack_ingest_to_downstream_by_close.setdefault(batch_key, []).append(int(ingest_to_downstream))
            if sender_client_id is not None:
                self._ack_clients_by_close.setdefault(batch_key, set()).add(sender_client_id)
        self._enqueue_latency_events(
            [
                {
                    "event_id": ev.event_id,
                    "symbol": ev.symbol,
                    "interval": ev.interval,
                    "topic": topic,
                    "e2e_ms": str(e2e),
                    "exchange_to_service_ms": str(ev.exchange_to_service_ms()),
                    "service_to_downstream_ms": str(svc_to_downstream),
                    "service_region": ev.service_region,
                    "downstream_region": ev.downstream_region or "",
                    "region_aligned": str(region_ok),
                    "phase": "ack",
                },
            ],
        )

    async def _notify_kline_batch(
        self,
        payload: list[dict],
        topic: str,
        expected_symbols: int,
        missing_symbols: int,
        ws_count: int,
        rest_count: int,
        service_to_client_no_ack_ms: int,
        connected_clients: int,
        publish_ms: int,
        push_done_ms: int,
    ) -> None:
        if not payload:
            return
        b2s_values: list[int] = []
        s2p_values: list[int] = []
        for row in payload:
            try:
                exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                service_ingest_ms = int(row.get("service_ingest_ms") or 0)
                service_publish_ms = int(row.get("service_publish_ms") or 0)
            except (TypeError, ValueError):
                continue
            if exchange_event_ms and service_ingest_ms:
                b2s_values.append(service_ingest_ms - exchange_event_ms)
            if service_ingest_ms and service_publish_ms:
                s2p_values.append(service_publish_ms - service_ingest_ms)
        close_time_ms = payload[0].get("close_time_ms")
        try:
            close_time_key = int(close_time_ms or 0)
        except (TypeError, ValueError):
            close_time_key = 0
        if close_time_key <= 0:
            return
        batch_key = (topic, close_time_key)
        metric_values = self._batch_metric_values_by_close.setdefault(
            batch_key,
            {
                "batch_size": 0,
                "expected_symbols": 0,
                "missing_symbols": 0,
                "ws_count": 0,
                "rest_count": 0,
                "connected_clients": 0,
                "b2s_values": [],
                "s2p_values": [],
                "s2c_no_ack_values": [],
                "push_round_sizes": [],
                "push_round_no_ack_ms": [],
                "push_round_b2s_ms": [],
                "first_publish_ms": 0,
                "last_push_done_ms": 0,
                "exchange_event_min_ms": 0,
                "service_ingest_min_ms": 0,
                "service_ingest_max_ms": 0,
                "shard_exchange_event_min_ms": {},
                "shard_service_ingest_min_ms": {},
                "shard_service_ingest_max_ms": {},
                "shard_symbol_counts": {},
                "delayed_tokens": [],
                "major_symbol_latency": {},
            },
        )
        metric_values["batch_size"] = int(metric_values["batch_size"]) + len(payload)
        metric_values["expected_symbols"] = max(int(metric_values["expected_symbols"]), int(expected_symbols))
        metric_values["missing_symbols"] = max(int(metric_values["missing_symbols"]), max(0, missing_symbols))
        metric_values["ws_count"] = int(metric_values["ws_count"]) + ws_count
        metric_values["rest_count"] = int(metric_values["rest_count"]) + rest_count
        metric_values["connected_clients"] = max(int(metric_values["connected_clients"]), max(0, connected_clients))
        cast_b2s = metric_values["b2s_values"]
        cast_s2p = metric_values["s2p_values"]
        cast_s2c = metric_values["s2c_no_ack_values"]
        cast_round_sizes = metric_values["push_round_sizes"]
        cast_round_no_ack = metric_values["push_round_no_ack_ms"]
        cast_round_b2s = metric_values["push_round_b2s_ms"]
        cast_delayed_tokens = metric_values["delayed_tokens"]
        first_publish_ms = int(metric_values["first_publish_ms"])
        last_push_done_ms = int(metric_values["last_push_done_ms"])
        exchange_event_min_ms = int(metric_values["exchange_event_min_ms"])
        service_ingest_min_ms = int(metric_values["service_ingest_min_ms"])
        service_ingest_max_ms = int(metric_values["service_ingest_max_ms"])
        shard_exchange_min = metric_values["shard_exchange_event_min_ms"]
        shard_ingest_min = metric_values["shard_service_ingest_min_ms"]
        shard_ingest_max = metric_values["shard_service_ingest_max_ms"]
        shard_symbol_counts = metric_values["shard_symbol_counts"]
        major_symbol_latency = metric_values["major_symbol_latency"]
        if isinstance(cast_b2s, list):
            cast_b2s.extend(b2s_values)
        if isinstance(cast_s2p, list):
            cast_s2p.extend(s2p_values)
        if isinstance(cast_s2c, list):
            cast_s2c.extend([service_to_client_no_ack_ms] * len(payload))
        if isinstance(cast_round_sizes, list):
            cast_round_sizes.append(len(payload))
        if isinstance(cast_round_no_ack, list):
            cast_round_no_ack.append(service_to_client_no_ack_ms)
        if isinstance(cast_round_b2s, list):
            round_exchange_min = 0
            round_ingest_max = 0
            for row in payload:
                try:
                    row_exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                    row_service_ingest_ms = int(row.get("service_ingest_ms") or 0)
                except (TypeError, ValueError):
                    continue
                if row_exchange_event_ms > 0:
                    round_exchange_min = (
                        row_exchange_event_ms
                        if round_exchange_min <= 0
                        else min(round_exchange_min, row_exchange_event_ms)
                    )
                round_ingest_max = max(round_ingest_max, row_service_ingest_ms)
            round_b2s_ms = (
                max(0, round_ingest_max - round_exchange_min)
                if round_ingest_max > 0 and round_exchange_min > 0
                else 0
            )
            cast_round_b2s.append(round_b2s_ms)
        if isinstance(cast_delayed_tokens, list):
            for row in payload:
                try:
                    symbol = str(row.get("symbol") or "")
                    row_close_time_ms = int(row.get("close_time_ms") or 0)
                    row_service_ingest_ms = int(row.get("service_ingest_ms") or 0)
                    row_service_publish_ms = int(row.get("service_publish_ms") or 0)
                    row_exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                except (TypeError, ValueError):
                    continue
                if not symbol or row_service_ingest_ms <= 0 or row_service_publish_ms <= 0:
                    continue
                if isinstance(major_symbol_latency, dict) and symbol in self._major_symbol_set:
                    major_symbol_latency[symbol] = {
                        "interval": str(row.get("interval") or ""),
                        "exchange_to_service_ms": (
                            max(0, row_service_ingest_ms - row_exchange_event_ms)
                            if row_exchange_event_ms > 0
                            else 0
                        ),
                        "local_wait_ms": max(0, row_service_publish_ms - row_service_ingest_ms),
                        "close_to_push_done_ms": max(0, push_done_ms - row_close_time_ms),
                    }
                wait_ms = max(0, row_service_publish_ms - row_service_ingest_ms)
                cast_delayed_tokens.append(
                    {
                        "symbol": symbol,
                        "wait_ms": wait_ms,
                        "close_time_ms": row_close_time_ms,
                    },
                )
                if row_exchange_event_ms > 0:
                    if exchange_event_min_ms <= 0:
                        exchange_event_min_ms = row_exchange_event_ms
                    else:
                        exchange_event_min_ms = min(exchange_event_min_ms, row_exchange_event_ms)
                if service_ingest_min_ms <= 0:
                    service_ingest_min_ms = row_service_ingest_ms
                else:
                    service_ingest_min_ms = min(service_ingest_min_ms, row_service_ingest_ms)
                service_ingest_max_ms = max(service_ingest_max_ms, row_service_ingest_ms)
                shard_id = str(row.get("ws_shard_id") or "")
                if shard_id:
                    try:
                        row_shard_symbol_count = int(row.get("ws_shard_symbol_count") or 0)
                    except (TypeError, ValueError):
                        row_shard_symbol_count = 0
                    prior_ex_min = int(shard_exchange_min.get(shard_id, 0)) if isinstance(shard_exchange_min, dict) else 0
                    prior_in_min = int(shard_ingest_min.get(shard_id, 0)) if isinstance(shard_ingest_min, dict) else 0
                    prior_in_max = int(shard_ingest_max.get(shard_id, 0)) if isinstance(shard_ingest_max, dict) else 0
                    if isinstance(shard_exchange_min, dict) and row_exchange_event_ms > 0:
                        shard_exchange_min[shard_id] = (
                            row_exchange_event_ms
                            if prior_ex_min <= 0
                            else min(prior_ex_min, row_exchange_event_ms)
                        )
                    if isinstance(shard_ingest_min, dict):
                        shard_ingest_min[shard_id] = (
                            row_service_ingest_ms
                            if prior_in_min <= 0
                            else min(prior_in_min, row_service_ingest_ms)
                        )
                    if isinstance(shard_ingest_max, dict):
                        shard_ingest_max[shard_id] = max(prior_in_max, row_service_ingest_ms)
                    if isinstance(shard_symbol_counts, dict):
                        shard_symbol_counts[shard_id] = max(
                            int(shard_symbol_counts.get(shard_id, 0)),
                            row_shard_symbol_count,
                        )
        if first_publish_ms <= 0:
            metric_values["first_publish_ms"] = publish_ms
        else:
            metric_values["first_publish_ms"] = min(first_publish_ms, publish_ms)
        metric_values["last_push_done_ms"] = max(last_push_done_ms, push_done_ms)
        metric_values["exchange_event_min_ms"] = exchange_event_min_ms
        metric_values["service_ingest_min_ms"] = service_ingest_min_ms
        metric_values["service_ingest_max_ms"] = service_ingest_max_ms

        final_push_done_ms = int(metric_values["last_push_done_ms"])
        close_to_push_done_ms = final_push_done_ms - close_time_key
        first_publish_final_ms = int(metric_values["first_publish_ms"])
        close_to_publish_ms = first_publish_final_ms - close_time_key if first_publish_final_ms > 0 else 0

        shard_push_done_ms = max(
            0,
            int(metric_values["last_push_done_ms"]) - first_publish_final_ms,
        )
        full_arrival_ms = (
            max(0, service_ingest_max_ms - exchange_event_min_ms)
            if service_ingest_max_ms > 0 and exchange_event_min_ms > 0
            else 0
        )
        ingest_spread_ms = (
            max(0, service_ingest_max_ms - service_ingest_min_ms)
            if service_ingest_max_ms > 0 and service_ingest_min_ms > 0
            else 0
        )
        shard_arrival_lines: list[str] = []
        if isinstance(shard_ingest_max, dict) and isinstance(shard_exchange_min, dict):
            sorted_shards = sorted(shard_ingest_max.keys())
            for idx, shard_id in enumerate(sorted_shards):
                try:
                    shard_ingest_max_ms = int(shard_ingest_max.get(shard_id, 0))
                    shard_exchange_min_ms = int(shard_exchange_min.get(shard_id, 0))
                except (TypeError, ValueError):
                    continue
                if shard_ingest_max_ms <= 0 or shard_exchange_min_ms <= 0:
                    continue
                shard_symbols = 0
                if isinstance(shard_symbol_counts, dict):
                    try:
                        shard_symbols = int(shard_symbol_counts.get(shard_id, 0))
                    except (TypeError, ValueError):
                        shard_symbols = 0
                shard_arrival_lines.append(
                    f"第{idx + 1}路一次性推送：symbols={shard_symbols} 到齐耗时={max(0, shard_ingest_max_ms - shard_exchange_min_ms)}ms",
                )
        shard_arrival_text = "；".join(shard_arrival_lines) if shard_arrival_lines else "n=0"
        round_b2s_text = "n=0"
        if isinstance(cast_round_sizes, list) and isinstance(cast_round_b2s, list):
            total_rounds = min(len(cast_round_sizes), len(cast_round_b2s))
            round_b2s_lines: list[str] = []
            for idx in range(total_rounds):
                try:
                    round_sz = int(cast_round_sizes[idx])
                    round_ms = int(cast_round_b2s[idx])
                except (TypeError, ValueError):
                    continue
                round_b2s_lines.append(f"第{idx + 1}次：symbols={round_sz} 到达耗时={round_ms}ms")
            if round_b2s_lines:
                round_b2s_text = self._format_round_lines(round_b2s_lines)
        delayed_top_text = "n=0"
        if isinstance(cast_delayed_tokens, list) and cast_delayed_tokens:
            delayed_sorted = sorted(
                [x for x in cast_delayed_tokens if isinstance(x, dict)],
                key=lambda x: int(x.get("wait_ms", 0)),
                reverse=True,
            )[:10]
            delayed_parts: list[str] = []
            for item in delayed_sorted:
                symbol = str(item.get("symbol") or "")
                wait_ms = int(item.get("wait_ms") or 0)
                readable_close = self._format_close_time(item.get("close_time_ms"))
                delayed_parts.append(f"{symbol}:等待{wait_ms}ms(close={readable_close})")
            if delayed_parts:
                delayed_top_text = "；".join(delayed_parts)
        major_latency_text = (
            self._format_major_symbol_latency(major_symbol_latency)
            if isinstance(major_symbol_latency, dict)
            else ""
        )

        self._batch_summary_by_close[batch_key] = {
            "topic": topic,
            "close_time_ms": close_time_key,
            "batch_size": int(metric_values["batch_size"]),
            "expected_symbols": int(metric_values["expected_symbols"]),
            "missing_symbols": int(metric_values["missing_symbols"]),
            "ws_count": int(metric_values["ws_count"]),
            "rest_count": int(metric_values["rest_count"]),
            "binance_to_service": self._format_stats(cast_b2s if isinstance(cast_b2s, list) else []),
            "service_ingest_to_publish": self._format_stats(cast_s2p if isinstance(cast_s2p, list) else []),
            "service_to_client_no_ack": self._format_stats(cast_s2c if isinstance(cast_s2c, list) else []),
            "connected_clients": int(metric_values["connected_clients"]),
            "service_ws_shards_push_done_ms": shard_push_done_ms,
            "binance_to_service_full_arrival_ms": full_arrival_ms,
            "service_ingest_spread_ms": ingest_spread_ms,
            "binance_to_service_shard_arrival_ms": shard_arrival_text,
            "binance_to_service_round_arrival_ms": round_b2s_text,
            "delayed_tokens_top10": delayed_top_text,
            "major_symbol_latency": major_latency_text,
            "close_time_to_publish_ms": close_to_publish_ms,
            "last_batch_push_done_ms": final_push_done_ms,
            "close_time_to_push_done_ms": close_to_push_done_ms,
        }

    async def _notify_ack_summary_later(
        self,
        batch_key: tuple[str, int],
        delay_seconds: int,
        send_notification: bool = True,
    ) -> None:
        await asyncio.sleep(max(1, delay_seconds))
        topic, close_time_ms = batch_key
        expected = self._ack_expected_by_close.get(batch_key, 0)
        values = self._ack_service_to_downstream_by_close.get(batch_key, [])
        ingest_to_downstream_values = self._ack_ingest_to_downstream_by_close.get(batch_key, [])
        batch_done_to_ack_values = self._batch_done_to_ack_by_close.get(batch_key, [])
        batch_done_to_integrated_ack_values = self._batch_done_to_integrated_ack_by_close.get(batch_key, [])
        batch_done_to_compute_ack_values = self._batch_done_to_compute_ack_by_close.get(batch_key, [])
        batch_done_to_order_ack_values = self._batch_done_to_order_ack_by_close.get(batch_key, [])
        ack_clients = len(self._ack_clients_by_close.get(batch_key, set()))
        batch_ack_clients = len(self._batch_ack_clients_by_close.get(batch_key, set()))
        batch_integrated_ack_clients = len(self._batch_integrated_ack_clients_by_close.get(batch_key, set()))
        batch_compute_ack_clients = len(self._batch_compute_ack_clients_by_close.get(batch_key, set()))
        batch_order_ack_clients = len(self._batch_order_ack_clients_by_close.get(batch_key, set()))
        batch = self._batch_summary_by_close.get(batch_key, {})
        raw_metrics = self._batch_metric_values_by_close.get(batch_key, {})
        s2c_no_ack_values = raw_metrics.get("s2c_no_ack_values", [])
        ctp = batch.get("close_time_to_push_done_ms")
        if isinstance(ctp, int):
            ctp_line = f"{ctp:+d}ms"
        elif ctp is None:
            ctp_line = "n/a"
        else:
            ctp_line = str(ctp)
        try:
            if send_notification:
                try:
                    batch_size = int(batch.get("batch_size") or 0)
                    expected_total = int(batch.get("expected_symbols") or expected or 0)
                    raw_missing = batch.get("missing_symbols")
                    if raw_missing is None:
                        missing_total = max(0, expected - len(values))
                    else:
                        missing_total = int(raw_missing)
                    close_to_publish_ms = int(batch.get("close_time_to_publish_ms") or 0)
                    publish_to_push_done_ms = int(batch.get("service_ws_shards_push_done_ms") or 0)
                    ingest_spread_ms = int(batch.get("service_ingest_spread_ms") or 0)
                    connected_clients = int(batch.get("connected_clients") or 0)
                except (TypeError, ValueError):
                    batch_size = int(expected or 0)
                    expected_total = int(expected or 0)
                    missing_total = 0
                    close_to_publish_ms = 0
                    publish_to_push_done_ms = 0
                    ingest_spread_ms = 0
                    connected_clients = 0
                major_latency = str(batch.get("major_symbol_latency") or "")
                show_major_latency = topic in (self._major_1h_topic, self._major_1m_topic)
                s2c_values = s2c_no_ack_values if isinstance(s2c_no_ack_values, list) else []
                body = (
                    "【K线批次与ACK汇总】\n"
                    f"Topic：{topic}\n"
                    f"收盘时间：{self._format_close_time(batch.get('close_time_ms', close_time_ms))}\n"
                    f"数据源/模式：{self._topic_data_source_label(batch)} / {self._push_mode_label(topic)}\n"
                    f"推送：{batch_size}/{expected_total} 缺失={missing_total} 客户端={connected_clients}\n"
                    f"服务端链路：收盘边界→本地聚合完成={close_to_publish_ms}ms；本地聚合完成→WS写出完成={publish_to_push_done_ms}ms；收盘→WS写出完成={ctp_line}\n"
                    f"接收跨度：{ingest_spread_ms}ms；服务端→客户端(不含ACK)：{self._format_stats_short(s2c_values)}\n"
                    f"ACK客户端：接收={batch_ack_clients} 整合={batch_integrated_ack_clients} 计算={batch_compute_ack_clients} 下单={batch_order_ack_clients}\n"
                    f"客户端接收ACK耗时：{self._format_stats_short(batch_done_to_ack_values)}\n"
                    f"计算ACK耗时：{self._format_stats_short(batch_done_to_compute_ack_values)}\n"
                    f"下单ACK耗时：{self._format_stats_short(batch_done_to_order_ack_values)}\n"
                )
                if show_major_latency and major_latency:
                    body += major_latency + "\n"
                await self._notifier.send("K线批次与ACK汇总", body)
        except Exception:
            logger.exception("notify.ack_summary_failed topic=%s close_time_ms=%s", topic, close_time_ms)
        finally:
            self._ack_service_to_downstream_by_close.pop(batch_key, None)
            self._ack_ingest_to_downstream_by_close.pop(batch_key, None)
            self._batch_done_to_ack_by_close.pop(batch_key, None)
            self._batch_done_to_integrated_ack_by_close.pop(batch_key, None)
            self._batch_done_to_compute_ack_by_close.pop(batch_key, None)
            self._batch_done_to_order_ack_by_close.pop(batch_key, None)
            self._ack_expected_by_close.pop(batch_key, None)
            self._ack_clients_by_close.pop(batch_key, None)
            self._batch_ack_clients_by_close.pop(batch_key, None)
            self._batch_integrated_ack_clients_by_close.pop(batch_key, None)
            self._batch_compute_ack_clients_by_close.pop(batch_key, None)
            self._batch_order_ack_clients_by_close.pop(batch_key, None)
            self._batch_summary_by_close.pop(batch_key, None)
            self._batch_metric_values_by_close.pop(batch_key, None)
            self._passthrough_shards_sent_by_close.pop(close_time_ms, None)
            self._passthrough_total_shards_by_close.pop(close_time_ms, None)
            self._batch_done_publish_ms_by_close.pop(batch_key, None)
            self._ack_notify_scheduled.discard(batch_key)

    def _format_stats(self, values: list[int]) -> str:
        if not values:
            return "n=0"
        current = values[-1]
        values_sorted = sorted(values)
        avg = sum(values_sorted) / len(values_sorted)
        p95 = self._percentile(values_sorted, 95.0)
        return (
            f"n={len(values_sorted)} 本次={current}ms "
            f"avg={avg:.2f}ms p95={p95:.2f}ms min={values_sorted[0]}ms max={values_sorted[-1]}ms"
        )

    def _format_stats_short(self, values: list[int]) -> str:
        if not values:
            return "n=0"
        current = values[-1]
        values_sorted = sorted(values)
        p95 = self._percentile(values_sorted, 95.0)
        return f"n={len(values_sorted)} 本次={current}ms p95={p95:.0f}ms max={values_sorted[-1]}ms"

    def _format_major_symbol_latency(self, values_by_symbol: dict) -> str:
        if not values_by_symbol:
            return ""
        lines: list[str] = []
        interval = ""
        for symbol in self._major_symbols:
            item = values_by_symbol.get(symbol)
            if not isinstance(item, dict):
                continue
            interval = interval or str(item.get("interval") or "")
            label = symbol.removesuffix("USDT")
            try:
                exchange_to_service_ms = int(item.get("exchange_to_service_ms") or 0)
                local_wait_ms = int(item.get("local_wait_ms") or 0)
                close_to_push_done_ms = int(item.get("close_to_push_done_ms") or 0)
            except (TypeError, ValueError):
                continue
            lines.append(
                f"{label}: 交易所→服务={exchange_to_service_ms}ms 本地等待={local_wait_ms}ms 收盘→推送=+{close_to_push_done_ms}ms",
            )
        if not lines:
            return ""
        suffix = f"({interval})" if interval else ""
        return "重点币耗时" + suffix + "：\n" + "\n".join(lines)

    def _topic_data_source_label(self, batch: dict) -> str:
        try:
            rest_count = int(batch.get("rest_count") or 0)
        except (TypeError, ValueError):
            rest_count = 0
        return "kline(ws+rest)" if rest_count > 0 else "kline(ws)"

    def _push_mode_label(self, topic: str) -> str:
        if topic == self._klines_all_topic:
            if self._klines_passthrough_mode:
                return f"全量{self._klines_interval}分片多轮"
            return f"全量{self._klines_interval}聚合整批"
        if topic == self._major_1h_topic:
            return f"重点币{self._major_1h_interval}独立WSS直推"
        if topic == self._major_1m_topic:
            if self._should_publish_alert_major_1m_klines() and not self._major_1m_enabled:
                return f"重点币{self._major_1m_interval}复用告警WSS直推"
            return f"重点币{self._major_1m_interval}独立WSS直推"
        return "服务端整批"

    def _format_round_lines(self, lines: list[str], head: int = 12, tail: int = 5) -> str:
        if not lines:
            return "n=0"
        head = max(1, head)
        tail = max(0, tail)
        if len(lines) <= head + tail:
            return "；".join(lines)
        omitted = len(lines) - head - tail
        return "；".join([*lines[:head], f"... 省略{omitted}轮 ...", *lines[-tail:]])

    def _format_close_time(self, close_time_ms: object) -> str:
        try:
            ts_ms = int(close_time_ms)
        except (TypeError, ValueError):
            return str(close_time_ms)
        if ts_ms <= 0:
            return str(close_time_ms)
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

    def _percentile(self, values_sorted: list[int], pct: float) -> float:
        if not values_sorted:
            return 0.0
        rank = (len(values_sorted) - 1) * (pct / 100.0)
        lo = int(rank)
        hi = min(lo + 1, len(values_sorted) - 1)
        if lo == hi:
            return float(values_sorted[lo])
        frac = rank - lo
        return values_sorted[lo] + (values_sorted[hi] - values_sorted[lo]) * frac

    async def _run_1m_alerts(
        self,
        symbols: list[str],
        batch_size: int = 200,
        stream_key: str | None = None,
        publish_kline_topic: str | None = None,
        publish_kline_symbols: set[str] | None = None,
    ) -> None:
        if self._alerts is None:
            return
        latest_close_by_symbol: dict[str, int] = {}
        publish_symbols = publish_kline_symbols or set()
        async for kline in self._binance.iter_closed_klines(
            symbols,
            interval="1m",
            batch_size=batch_size,
            stream_key=stream_key,
        ):
            self._ingest_1m += 1
            prev_close = latest_close_by_symbol.get(kline.symbol)
            is_new_closed_kline = prev_close is None or kline.close_time_ms > prev_close
            if is_new_closed_kline:
                latest_close_by_symbol[kline.symbol] = kline.close_time_ms
            signals = await self._alerts.on_closed_1m(kline)
            if (
                publish_kline_topic
                and is_new_closed_kline
                and kline.symbol in publish_symbols
            ):
                await self._add_major_topic_kline(kline, publish_kline_topic, publish_symbols)
            if signals and self._alerts_ws is not None:
                for sig in signals:
                    pushed = await self._alerts_ws.broadcast_json(self._alert_payload(sig))
                    if pushed > 0:
                        self._push_alerts += pushed

    async def _run_heartbeat(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(15)
            alerts_clients = await self._alerts_ws.client_count() if self._alerts_ws is not None else 0
            klines_clients = await self._klines_ws.client_count() if self._klines_ws is not None else 0
            all_topic_clients = (
                await self._klines_ws.client_count(self._klines_all_topic) if self._klines_ws is not None else 0
            )
            major_1h_clients = (
                await self._klines_ws.client_count(self._major_1h_topic) if self._klines_ws is not None else 0
            )
            major_1m_clients = (
                await self._klines_ws.client_count(self._major_1m_topic) if self._klines_ws is not None else 0
            )
            ws_activity = self._binance.get_ws_activity_snapshot()
            ws_shard_health = self._binance.get_ws_shard_health_snapshot()
            ws_ago_ms = ws_activity.get(self._klines_stream_key)
            major_1h_ago_ms = ws_activity.get(self._major_1h_stream_key)
            alert_1m_ago_ms = ws_activity.get(self._alert_1m_stream_key)
            alert_major_1m_ago_ms = ws_activity.get(self._alert_major_1m_stream_key)
            major_1m_ago_ms = ws_activity.get(self._major_1m_interval)
            if major_1m_ago_ms is None:
                major_1m_ago_ms = ws_activity.get(self._major_1m_stream_key)
            interval_shards = ws_shard_health.get(self._klines_stream_key, {})
            major_1h_shards = ws_shard_health.get(self._major_1h_stream_key, {})
            alert_1m_shards = ws_shard_health.get(self._alert_1m_stream_key, {})
            alert_major_1m_shards = ws_shard_health.get(self._alert_major_1m_stream_key, {})
            major_1m_shards = ws_shard_health.get(self._major_1m_stream_key, {})
            logger.info(
                "service.heartbeat mode=%s ingest_1m=%s ingest_1h=%s pushed_alerts=%s pushed_1h=%s clients_alerts=%s clients_klines=%s clients_klines_all_topic=%s clients_majors_1h=%s clients_majors_1m=%s alert_1m_last_event_ago_ms=%s alert_1m_total_shards=%s alert_major_1m_last_event_ago_ms=%s alert_major_1m_total_shards=%s klines_interval=%s ws_last_event_ago_ms=%s ws_stale_shards=%s ws_total_shards=%s ws_worst_shard_ago_ms=%s major_1h_interval=%s major_1h_last_event_ago_ms=%s major_1h_total_shards=%s major_1m_interval=%s major_1m_last_event_ago_ms=%s major_1m_total_shards=%s",
                self._cfg.service_mode,
                self._ingest_1m,
                self._ingest_1h,
                self._push_alerts,
                self._push_1h,
                alerts_clients,
                klines_clients,
                all_topic_clients,
                major_1h_clients,
                major_1m_clients,
                alert_1m_ago_ms,
                alert_1m_shards.get("total_shards"),
                alert_major_1m_ago_ms,
                alert_major_1m_shards.get("total_shards"),
                self._klines_interval,
                ws_ago_ms,
                interval_shards.get("stale_shards"),
                interval_shards.get("total_shards"),
                interval_shards.get("worst_shard_ago_ms"),
                self._major_1h_interval,
                major_1h_ago_ms,
                major_1h_shards.get("total_shards"),
                self._major_1m_interval,
                major_1m_ago_ms,
                major_1m_shards.get("total_shards"),
            )

    def _alert_payload(self, signal: AlertSignal) -> dict:
        return {
            "type": "price_alert",
            "symbol": signal.symbol,
            "interval": signal.interval,
            "direction": signal.direction,
            "threshold_pct": signal.threshold_pct,
            "move_pct": signal.move_pct,
            "previous_close": signal.reference_close_price,
            "current_close": signal.current_close_price,
            "open_time_ms": signal.open_time_ms,
            "close_time_ms": signal.close_time_ms,
            "exchange_event_ms": signal.event_time_ms,
            "service_ingest_ms": signal.ingest_time_ms,
            "service_region": self._cfg.service_region,
        }

    async def _get_alert_clients_count(self) -> int:
        if self._alerts_ws is None:
            return 0
        client_count = getattr(self._alerts_ws, "client_count", None)
        if callable(client_count):
            try:
                return int(await client_count())
            except Exception:
                return 0
        return 0

