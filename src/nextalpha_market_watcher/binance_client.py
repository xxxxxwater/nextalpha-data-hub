from __future__ import annotations

import asyncio
import contextlib
import http.client
import logging
import base64
import os
import socket
import time
from collections.abc import AsyncIterator
from decimal import Decimal
from urllib.parse import urlparse, urlencode

import aiohttp
import httpx
import orjson
import websockets
import socks

from .config import AppConfig
from .metrics import (
    BACKFILL_ERRORS,
    BACKFILL_KLINES,
    EXCHANGE_TO_SERVICE_MS,
    KLINES_RECEIVED,
    WS_KLINE_FAST_SKIPPED,
    WS_CONNECTIONS,
    touch_ingest,
)
from .models import ClosedKline

logger = logging.getLogger(__name__)

_JSON_WHITESPACE = b" \t\r\n"
_CLOSED_KLINE_KEY = b'"x"'
_CONTROL_FRAME_MARKERS = (b'"code"', b'"msg"', b'"result"', b'"id"')


def _raw_has_closed_kline_marker(raw: bytes) -> bool:
    """Return True when a raw Binance frame contains the kline close flag x=true."""
    start = 0
    while True:
        idx = raw.find(_CLOSED_KLINE_KEY, start)
        if idx < 0:
            return False
        pos = idx + len(_CLOSED_KLINE_KEY)
        while pos < len(raw) and raw[pos] in _JSON_WHITESPACE:
            pos += 1
        if pos < len(raw) and raw[pos] == ord(":"):
            pos += 1
            while pos < len(raw) and raw[pos] in _JSON_WHITESPACE:
                pos += 1
            if raw.startswith(b"true", pos):
                return True
        start = idx + len(_CLOSED_KLINE_KEY)


def _raw_maybe_control_frame(raw: bytes) -> bool:
    return any(marker in raw for marker in _CONTROL_FRAME_MARKERS)


def _no_proxy_matches_host(host: str, no_proxy: str) -> bool:
    host = host.lower().strip(".")
    for raw in no_proxy.split(","):
        pat = raw.strip().lower()
        if not pat:
            continue
        if pat == "*":
            return True
        if pat.startswith("."):
            pat = pat[1:]
        if host == pat or host.endswith("." + pat):
            return True
    return False


def _ws_proxy_from_env(dest_host: str) -> str | None:
    no_proxy = (os.getenv("NO_PROXY") or os.getenv("no_proxy") or "").strip()
    if no_proxy and _no_proxy_matches_host(dest_host, no_proxy):
        return None
    return (
        (os.getenv("BINANCE_HTTP_PROXY") or "").strip()
        or (os.getenv("HTTPS_PROXY") or "").strip()
        or (os.getenv("HTTP_PROXY") or "").strip()
        or None
    )


def _kline_from_rest_row(symbol: str, interval: str, row: list) -> ClosedKline:
    ingest_ms = int(time.time() * 1000)
    open_t = int(row[0])
    close_t = int(row[6])
    return ClosedKline(
        symbol=symbol,
        interval=interval,
        open_time_ms=open_t,
        close_time_ms=close_t,
        open_price=Decimal(row[1]),
        high_price=Decimal(row[2]),
        low_price=Decimal(row[3]),
        close_price=Decimal(row[4]),
        volume=Decimal(row[5]),
        quote_volume=Decimal(row[7]),
        event_time_ms=close_t,
        ingest_time_ms=ingest_ms,
        source="rest",
        ws_shard_id="",
        ws_shard_symbol_count=0,
        ws_total_shards=0,
    )


class BinanceClient:
    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg
        self._ws_rebuild_generation: dict[str, int] = {}
        self._ws_last_event_ms: dict[str, int] = {}
        self._ws_watchdog_alerted: dict[str, bool] = {}
        self._ws_shard_last_event_ms: dict[str, dict[str, int]] = {}
        self._ws_shard_watchdog_alerted: dict[str, dict[str, bool]] = {}
        self._ws_activity_interval: dict[str, str] = {}
        # Debug: print first kline payload structure per interval/shard.
        self._ws_parse_debug_logged: set[str] = set()

    def _build_ws_proxy_sock_socks5(
        self,
        proxy_url: str,
        dest_host: str,
        dest_port: int,
    ) -> socket.socket:
        """
        Build a pre-connected socket using SOCKS5 proxy.

        websockets.connect() doesn't implement HTTP/SOCKS proxy itself, but it
        forwards extra kwargs to asyncio.loop.create_connection(), which
        accepts `sock=...`. So we create a TCP socket to the destination via
        the proxy and pass it in.
        """
        parsed = urlparse(proxy_url)
        proxy_host = parsed.hostname
        proxy_port = parsed.port
        if not proxy_host or not proxy_port:
            raise ValueError(f"Invalid ws_proxy_url={proxy_url!r}")

        scheme = (parsed.scheme or "").lower()
        if not scheme.startswith("socks5"):
            raise ValueError(f"Unsupported ws_proxy_url scheme={parsed.scheme!r}, url={proxy_url!r}")

        rdns = scheme == "socks5h"
        username = parsed.username
        password = parsed.password

        sock: socket.socket = socks.socksocket()
        # PySocks uses rdns to decide whether hostname resolution is done remotely.
        sock.set_proxy(
            socks.SOCKS5,
            proxy_host,
            proxy_port,
            username=username,
            password=password,
            rdns=rdns,
        )
        sock.settimeout(20.0)
        sock.connect((dest_host, dest_port))
        sock.settimeout(None)
        sock.setblocking(False)
        return sock

    def _build_ws_proxy_sock_http_connect(
        self,
        proxy_url: str,
        dest_host: str,
        dest_port: int,
    ) -> socket.socket:
        """
        Build a pre-connected socket via an HTTP proxy using CONNECT.

        websockets.connect() will pass unknown kwargs to asyncio's
        loop.create_connection(). If we provide `sock=...`, it will use the
        already-connected socket and perform the TLS handshake (for wss://)
        on top of it.
        """
        parsed = urlparse(proxy_url)
        proxy_host = parsed.hostname
        proxy_port = parsed.port
        if not proxy_host:
            raise ValueError(f"Invalid ws_proxy_url={proxy_url!r}")

        scheme = (parsed.scheme or "").lower()
        if not scheme.startswith("http"):
            raise ValueError(f"Unsupported ws_proxy_url scheme={parsed.scheme!r}, url={proxy_url!r}")

        proxy_port = proxy_port or 80
        username = parsed.username
        password = parsed.password or ""

        headers: dict[str, str] = {}
        if username is not None:
            creds = f"{username}:{password}".encode("utf-8")
            b64 = base64.b64encode(creds).decode("ascii")
            headers["Proxy-Authorization"] = f"Basic {b64}"

        # NOTE: http.client's tunnel support will read until the end of the
        # proxy response headers and leave the tunnel socket open.
        conn = http.client.HTTPConnection(proxy_host, proxy_port, timeout=20.0)
        conn.set_tunnel(dest_host, dest_port, headers=headers or None)
        conn.connect()
        sock = conn.sock
        # Detach so conn.__del__ doesn't close the socket.
        conn.sock = None
        if sock is None:
            raise RuntimeError(f"HTTP proxy tunnel failed for proxy={proxy_url!r}")

        sock.setblocking(False)
        return sock

    async def fetch_symbols(self) -> list[str]:
        if self._cfg.binance.use_test_symbols and self._cfg.binance.test_symbols:
            return sorted(set(self._cfg.binance.test_symbols))

        url = f"{self._cfg.binance.rest_base}/fapi/v1/exchangeInfo"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        symbols: list[str] = []
        for item in data.get("symbols", []):
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            if item.get("status") != "TRADING":
                continue
            sym = item.get("symbol")
            if sym:
                symbols.append(sym)
        return sorted(set(symbols))

    async def fetch_closed_klines_rest(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        interval: str,
        limit: int,
    ) -> list[ClosedKline]:
        cap = min(limit, self._cfg.backfill.klines_limit_cap)
        url = f"{self._cfg.binance.rest_base}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": cap}
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw_rows = resp.json()
        if not isinstance(raw_rows, list):
            return []
        out: list[ClosedKline] = []
        now_ms = int(time.time() * 1000)
        for row in raw_rows:
            if not isinstance(row, list) or len(row) < 8:
                continue
            # REST may include the currently forming bar; only keep closed bars.
            if int(row[6]) > now_ms:
                continue
            out.append(_kline_from_rest_row(symbol, interval, row))
        return out

    async def fetch_exact_closed_kline_rest(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        interval: str,
        close_time_ms: int,
    ) -> ClosedKline | None:
        # Query with endTime to target the expected closed candle window.
        url = f"{self._cfg.binance.rest_base}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": 3,
            "endTime": close_time_ms,
        }
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw_rows = resp.json()
        if not isinstance(raw_rows, list):
            return None
        for row in reversed(raw_rows):
            if not isinstance(row, list) or len(row) < 8:
                continue
            if int(row[6]) != close_time_ms:
                continue
            return _kline_from_rest_row(symbol, interval, row)
        return None

    async def _backfill_batch(
        self,
        symbols: list[str],
        interval: str,
        out_queue: asyncio.Queue[ClosedKline],
        limit_override: int | None = None,
    ) -> None:
        if not self._cfg.backfill.enabled:
            return

        limit = self._cfg.backfill.bars_1m if interval == "1m" else self._cfg.backfill.bars_1h
        if interval not in ("1m", "1h"):
            limit = self._cfg.backfill.bars_1m
        if limit_override is not None:
            limit = max(1, limit_override)

        sem = asyncio.Semaphore(max(1, self._cfg.backfill.rest_concurrency))
        timeout = httpx.Timeout(self._cfg.backfill.rest_timeout_seconds)

        async def one(client: httpx.AsyncClient, sym: str) -> list[ClosedKline]:
            async with sem:
                if self._cfg.backfill.rest_request_delay_seconds > 0:
                    await asyncio.sleep(self._cfg.backfill.rest_request_delay_seconds)
                try:
                    rows = await self.fetch_closed_klines_rest(client, sym, interval, limit)
                    BACKFILL_KLINES.labels(interval).inc(len(rows))
                    return rows
                except Exception as exc:
                    BACKFILL_ERRORS.labels(interval).inc()
                    logger.warning(
                        "backfill.fail symbol=%s interval=%s error=%s",
                        sym,
                        interval,
                        exc,
                    )
                    return []

        async with httpx.AsyncClient(timeout=timeout) as http:
            results = await asyncio.gather(*[one(http, s) for s in symbols])
        merged: list[ClosedKline] = []
        for rows in results:
            merged.extend(rows)
        # Sort by bar time first so downstream batching by close_time can aggregate
        # all symbols for the same candle before considering the next candle.
        merged.sort(key=lambda k: (k.open_time_ms, k.symbol))
        for kline in merged:
            lag = max(0, kline.ingest_time_ms - kline.event_time_ms)
            EXCHANGE_TO_SERVICE_MS.labels(interval, kline.source).observe(lag)
            KLINES_RECEIVED.labels(interval, kline.source).inc()
            touch_ingest(interval)
            await out_queue.put(kline)
        logger.info("backfill.done interval=%s symbols=%s klines=%s", interval, len(symbols), len(merged))

    async def _run_ws_silence_rest_repair(
        self,
        interval: str,
        symbols: list[str],
        out_queue: asyncio.Queue[ClosedKline],
        activity_key: str | None = None,
    ) -> None:
        if (
            interval == "1m"
            or not self._cfg.backfill.enabled
            or not self._cfg.websocket.klines_ws_silence_fallback_rest
        ):
            return
        silence_threshold_ms = 30_000
        activity_key = activity_key or interval
        while True:
            await asyncio.sleep(10)
            last_ms = self._ws_last_event_ms.get(activity_key, 0)
            if last_ms <= 0:
                continue
            now_ms = int(time.time() * 1000)
            no_event_ms = now_ms - last_ms
            if no_event_ms < silence_threshold_ms:
                continue
            logger.warning(
                "ws.silence.rest_repair interval=%s stream=%s no_event_ms=%s symbols=%s",
                interval,
                activity_key,
                no_event_ms,
                len(symbols),
            )
            # limit=1 often returns the currently forming candle; use 2 so we can
            # still pick the previous fully closed candle.
            await self._backfill_batch(symbols, interval, out_queue, limit_override=2)

    async def iter_closed_klines(
        self,
        symbols: list[str],
        interval: str,
        batch_size: int = 200,
        stream_key: str | None = None,
    ) -> AsyncIterator[ClosedKline]:
        # Keep the target batch size, but split further when the combined stream
        # query becomes too long. This avoids silent WS subscriptions with no data.
        symbol_batches = self._build_symbol_batches(
            symbols,
            interval,
            batch_size,
            # SUBSCRIBE mode does not need long URL query splitting.
            max_streams_query_chars=0,
        )
        activity_key = stream_key or interval
        self._ws_activity_interval[activity_key] = interval
        shard_ids = [f"{activity_key}#{idx}" for idx in range(len(symbol_batches))]
        queue: asyncio.Queue[ClosedKline] = asyncio.Queue(maxsize=10000)
        tasks = [
            asyncio.create_task(
                self._consume_batch(batch, interval, shard_ids[idx], queue, len(shard_ids), activity_key),
            )
            for idx, batch in enumerate(symbol_batches)
            if batch and idx < len(shard_ids)
        ]
        rest_repair_task = asyncio.create_task(
            self._run_ws_silence_rest_repair(interval, symbols, queue, activity_key),
        )
        watchdog_task: asyncio.Task[None] | None = None
        if self._cfg.binance.ws_watchdog_enabled:
            self._ws_last_event_ms[activity_key] = int(time.time() * 1000)
            self._ws_watchdog_alerted[activity_key] = False
            self._ws_shard_last_event_ms[activity_key] = {
                shard_id: int(time.time() * 1000) for shard_id in shard_ids
            }
            self._ws_shard_watchdog_alerted[activity_key] = {shard_id: False for shard_id in shard_ids}
            watchdog_task = asyncio.create_task(
                self._run_ws_watchdog(interval, len(symbols), shard_ids, activity_key),
            )
        try:
            while True:
                yield await queue.get()
        finally:
            for t in tasks:
                t.cancel()
            rest_repair_task.cancel()
            if watchdog_task is not None:
                watchdog_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watchdog_task
            with contextlib.suppress(asyncio.CancelledError):
                await rest_repair_task
            await asyncio.gather(*tasks, return_exceptions=True)

    def _build_symbol_batches(
        self,
        symbols: list[str],
        interval: str,
        batch_size: int,
        max_streams_query_chars: int = 0,
    ) -> list[list[str]]:
        batches: list[list[str]] = []
        current: list[str] = []

        for symbol in symbols:
            stream_name = f"{symbol.lower()}@kline_{interval}"
            # Use encoded query length as the true budget (important for non-ascii symbols).
            if current:
                projected_chars = len(
                    urlencode(
                        {
                            "streams": "/".join(
                                [*(f"{s.lower()}@kline_{interval}" for s in current), stream_name]
                            )
                        }
                    )
                )
            else:
                projected_chars = len(urlencode({"streams": stream_name}))
            hit_size_limit = len(current) >= max(1, batch_size)
            hit_query_limit = max_streams_query_chars > 0 and projected_chars > max_streams_query_chars
            if current and (hit_size_limit or hit_query_limit):
                batches.append(current)
                current = [symbol]
            else:
                current.append(symbol)

        if current:
            batches.append(current)

        if batches:
            logger.info(
                "ws.batches.built interval=%s requested_batch_size=%s actual_batches=%s largest_batch=%s max_query_chars=%s",
                interval,
                batch_size,
                len(batches),
                max(len(b) for b in batches),
                max_streams_query_chars,
            )
        return batches

    async def _consume_batch(
        self,
        symbols: list[str],
        interval: str,
        shard_id: str,
        out_queue: asyncio.Queue[ClosedKline],
        total_shards: int,
        activity_key: str,
    ) -> None:
        stream_names = [f"{s.lower()}@kline_{interval}" for s in symbols]
        fast_skipped_metric = WS_KLINE_FAST_SKIPPED.labels(interval)
        ws_base = self._cfg.binance.ws_base.split("?", 1)[0].rstrip("/")
        if ws_base.endswith("/stream"):
            stream_base = ws_base
        elif ws_base.endswith("/ws"):
            # Support new market endpoint style:
            #   wss://fstream.binance.com/market/ws
            # Combined streams should use sibling /stream path.
            stream_base = f"{ws_base[:-3]}/stream"
        else:
            stream_base = f"{ws_base}/stream"
        url = f"{stream_base}?streams={'/'.join(stream_names)}"
        dest_host = urlparse(url).hostname or "fstream.binance.com"
        while True:
            try:
                proxy_url = self._cfg.binance.ws_proxy_url or _ws_proxy_from_env(dest_host)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=120.0, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(
                        url,
                        proxy=proxy_url,
                        autoping=True,
                        heartbeat=None,
                    ) as ws:
                        local_generation = self._ws_rebuild_generation.get(shard_id, 0)
                        WS_CONNECTIONS.labels(interval).inc()
                        logger.info("ws.connected interval=%s shard=%s symbols=%s", interval, shard_id, len(symbols))
                        # Keep 1m warm-start for alert calculations; higher intervals use delayed REST repair.
                        if interval == "1m":
                            await self._backfill_batch(symbols, interval, out_queue)
                        while True:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                            except TimeoutError:
                                # Periodically wake up to honor watchdog-triggered rebuilds.
                                if self._ws_rebuild_generation.get(shard_id, 0) != local_generation:
                                    logger.warning(
                                        "ws.rebuild.triggered interval=%s shard=%s symbols=%s",
                                        interval,
                                        shard_id,
                                        len(symbols),
                                    )
                                    break
                                continue
                            if self._ws_rebuild_generation.get(shard_id, 0) != local_generation:
                                logger.warning(
                                    "ws.rebuild.triggered interval=%s shard=%s symbols=%s",
                                    interval,
                                    shard_id,
                                    len(symbols),
                                )
                                break
                            if msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                raise ConnectionError("WebSocket closed by peer")
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                raw = msg.data.encode("utf-8")
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                raw = msg.data
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise msg.data
                            else:
                                continue
                            # Treat any successfully received WS frame as shard activity.
                            # This prevents watchdog false positives when only non-closed kline
                            # updates (x=false) are flowing between close boundaries.
                            now_ms = int(time.time() * 1000)
                            self._ws_last_event_ms[activity_key] = now_ms
                            interval_shards = self._ws_shard_last_event_ms.setdefault(activity_key, {})
                            interval_shards[shard_id] = now_ms
                            self._ws_watchdog_alerted[activity_key] = False
                            shard_alerted = self._ws_shard_watchdog_alerted.setdefault(activity_key, {})
                            shard_alerted[shard_id] = False
                            if not _raw_has_closed_kline_marker(raw) and not _raw_maybe_control_frame(raw):
                                fast_skipped_metric.inc()
                                debug_key = f"{interval}:{shard_id}:fast_skip"
                                if debug_key not in self._ws_parse_debug_logged:
                                    self._ws_parse_debug_logged.add(debug_key)
                                    try:
                                        raw_sample = raw[:200].decode("utf-8", errors="ignore")
                                    except Exception:
                                        raw_sample = "<unprintable>"
                                    logger.info(
                                        "ws.kline_fast_skip_debug interval=%s shard=%s raw_sample=%s",
                                        interval,
                                        shard_id,
                                        raw_sample,
                                    )
                                continue
                            payload = orjson.loads(raw)
                            if "code" in payload or "msg" in payload:
                                logger.error(
                                    "ws.subscribe.error interval=%s shard=%s code=%s msg=%s",
                                    interval,
                                    shard_id,
                                    payload.get("code"),
                                    payload.get("msg"),
                                )
                                continue
                            data = payload.get("data", payload)
                            k = data.get("k", {})
                            if not k.get("x"):
                                debug_key = f"{interval}:{shard_id}"
                                if debug_key not in self._ws_parse_debug_logged:
                                    self._ws_parse_debug_logged.add(debug_key)
                                    try:
                                        raw_sample = raw[:200].decode("utf-8", errors="ignore")
                                    except Exception:
                                        raw_sample = "<unprintable>"
                                    logger.info(
                                        "ws.kline_parse_debug interval=%s shard=%s payload_keys=%s data_keys=%s k_keys=%s k_x=%s raw_sample=%s",
                                        interval,
                                        shard_id,
                                        list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
                                        list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                                        list(k.keys()) if isinstance(k, dict) else type(k).__name__,
                                        k.get("x"),
                                        raw_sample,
                                    )
                            if not k.get("x"):
                                continue

                            ingest_ms = int(time.time() * 1000)
                            kline = ClosedKline(
                                symbol=k["s"],
                                interval=k["i"],
                                open_time_ms=int(k["t"]),
                                close_time_ms=int(k["T"]),
                                open_price=Decimal(k["o"]),
                                high_price=Decimal(k["h"]),
                                low_price=Decimal(k["l"]),
                                close_price=Decimal(k["c"]),
                                volume=Decimal(k["v"]),
                                quote_volume=Decimal(k["q"]),
                                event_time_ms=int(data["E"]),
                                ingest_time_ms=ingest_ms,
                                source="ws",
                                ws_shard_id=shard_id,
                                ws_shard_symbol_count=len(symbols),
                                ws_total_shards=total_shards,
                            )
                            lag = max(0, kline.ingest_time_ms - kline.event_time_ms)
                            EXCHANGE_TO_SERVICE_MS.labels(interval, kline.source).observe(lag)
                            KLINES_RECEIVED.labels(interval, kline.source).inc()
                            touch_ingest(interval)
                            await out_queue.put(kline)
            except Exception as exc:
                logger.warning("ws.reconnect interval=%s shard=%s error=%s", interval, shard_id, exc)
                await asyncio.sleep(2.0)

    async def _run_ws_watchdog(
        self,
        interval: str,
        expected_symbols: int,
        shard_ids: list[str],
        activity_key: str | None = None,
    ) -> None:
        activity_key = activity_key or interval
        interval_ms = self._interval_to_ms(interval)
        if interval_ms <= 0:
            logger.warning("ws.watchdog.disabled interval=%s reason=unknown_interval", interval)
            return
        no_data_periods = max(1, self._cfg.binance.ws_watchdog_no_data_periods)
        stale_threshold_ms = interval_ms * no_data_periods
        check_seconds = max(1, self._cfg.binance.ws_watchdog_check_seconds)
        while True:
            await asyncio.sleep(check_seconds)
            now_ms = int(time.time() * 1000)
            shard_last = self._ws_shard_last_event_ms.get(activity_key, {})
            shard_alerted = self._ws_shard_watchdog_alerted.setdefault(activity_key, {})
            stale_shards = 0
            for shard_id in shard_ids:
                last_ms = shard_last.get(shard_id, 0)
                stale_ms = max(0, now_ms - last_ms) if last_ms > 0 else stale_threshold_ms
                if stale_ms < stale_threshold_ms:
                    continue
                stale_shards += 1
                if shard_alerted.get(shard_id, False):
                    continue
                new_generation = self._ws_rebuild_generation.get(shard_id, 0) + 1
                self._ws_rebuild_generation[shard_id] = new_generation
                shard_alerted[shard_id] = True
                logger.error(
                    "ws.watchdog.rebuild_shard interval=%s stream=%s shard=%s no_data_ms=%s threshold_ms=%s periods=%s expected_symbols=%s total_shards=%s generation=%s",
                    interval,
                    activity_key,
                    shard_id,
                    stale_ms,
                    stale_threshold_ms,
                    no_data_periods,
                    expected_symbols,
                    len(shard_ids),
                    new_generation,
                )
            self._ws_watchdog_alerted[activity_key] = stale_shards > 0

    def _interval_to_ms(self, interval: str) -> int:
        try:
            if interval.endswith("m"):
                return int(interval[:-1]) * 60_000
            if interval.endswith("h"):
                return int(interval[:-1]) * 3_600_000
            if interval.endswith("d"):
                return int(interval[:-1]) * 86_400_000
        except ValueError:
            return 0
        return 0

    def get_ws_activity_snapshot(self) -> dict[str, int | None]:
        now_ms = int(time.time() * 1000)
        out: dict[str, int | None] = {}
        for interval, last_ms in self._ws_last_event_ms.items():
            if last_ms <= 0:
                out[interval] = None
            else:
                out[interval] = max(0, now_ms - last_ms)
        return out

    def get_ws_shard_health_snapshot(self) -> dict[str, dict[str, int]]:
        now_ms = int(time.time() * 1000)
        out: dict[str, dict[str, int]] = {}
        for activity_key, shard_map in self._ws_shard_last_event_ms.items():
            interval = self._ws_activity_interval.get(activity_key, activity_key.split(":", 1)[0])
            interval_ms = self._interval_to_ms(interval)
            no_data_periods = max(1, self._cfg.binance.ws_watchdog_no_data_periods)
            stale_threshold_ms = interval_ms * no_data_periods if interval_ms > 0 else 0
            total = len(shard_map)
            stale = 0
            worst_ago_ms = 0
            for last_ms in shard_map.values():
                ago_ms = max(0, now_ms - last_ms) if last_ms > 0 else 0
                worst_ago_ms = max(worst_ago_ms, ago_ms)
                if stale_threshold_ms > 0 and ago_ms >= stale_threshold_ms:
                    stale += 1
            out[activity_key] = {
                "total_shards": total,
                "stale_shards": stale,
                "worst_shard_ago_ms": worst_ago_ms,
            }
        return out

