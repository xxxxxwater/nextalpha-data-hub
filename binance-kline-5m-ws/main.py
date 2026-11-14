#!/usr/bin/env python3
"""
Binance USDT-M kline subscriber — optimized for minimum end-to-end latency.

Hot path: recv -> queue (timestamp) -> parse/filter -> bitmap tracker -> log queue
"""

from __future__ import annotations

import asyncio
import inspect
import heapq
import os
import sys
import time
from array import array
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import orjson
import websockets
from websockets.exceptions import ConnectionClosed

ROOT = Path(__file__).resolve().parent
SYMBOLS_FILE = ROOT / "symbols.txt"
BINANCE_WS_BASE = "wss://fstream.binance.com/market/stream?streams="

# Closed kline marker in raw payload (skip full JSON parse when absent)
_CLOSED_MARKERS = (b'"x":true', b'"x": true', b'"x":True', b'"x": True')


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    return int(v) if v.isdigit() else default


def _env_optional_float(name: str, default: Optional[float]) -> Optional[float]:
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    if v in ("0", "none", "false", "off", "disable", "disabled"):
        return None
    try:
        return float(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


KLINE_INTERVAL = os.environ.get("KLINE_INTERVAL", "3m").strip() or "3m"
KLINE_STREAM_SUFFIX = f"@kline_{KLINE_INTERVAL}"
STREAMS_FILE = ROOT / f"klines_ws_{KLINE_INTERVAL}_528.txt"

# Smaller shards => less burst per socket at candle close (default 88 ≈ 6 connections for 528)
SHARD_SIZE = _env_int("SHARD_SIZE", 88)
LOG_EACH_KLINE = _env_bool("LOG_EACH_KLINE", False)
LOG_PROGRESS_EVERY = _env_int("LOG_PROGRESS_EVERY", 0)  # 0 = off on hot path
QUEUE_MAXSIZE = _env_int("QUEUE_MAXSIZE", 0)  # 0 = unbounded
PROCESSOR_WORKERS = _env_int("PROCESSOR_WORKERS", 6)
LOG_QUEUE_MAXSIZE = _env_int("LOG_QUEUE_MAXSIZE", 100_000)
TRACKER_MAX_BATCHES = max(1, _env_int("TRACKER_MAX_BATCHES", 128))
BINANCE_WS_PROXY_URL = (
    os.environ.get("BINANCE_WS_PROXY_URL", "").strip()
    or os.environ.get("WS_PROXY_URL", "").strip()
)
PING_INTERVAL = _env_optional_float("PING_INTERVAL", 20.0)
PING_TIMEOUT = _env_optional_float("PING_TIMEOUT", 60.0)
OPEN_TIMEOUT = _env_optional_float("OPEN_TIMEOUT", 30.0)
_CONNECT_SUPPORTS_PROXY = "proxy" in inspect.signature(websockets.connect).parameters


def load_symbols(path: Path = SYMBOLS_FILE) -> list[str]:
    symbols = [s.strip().lower() for s in path.read_text().splitlines() if s.strip()]
    if not symbols:
        raise RuntimeError(f"No symbols in {path}")
    return symbols


def build_ws_urls(symbols: list[str], shard_size: int = SHARD_SIZE) -> list[str]:
    urls: list[str] = []
    for i in range(0, len(symbols), shard_size):
        chunk = symbols[i : i + shard_size]
        streams = "/".join(f"{s}{KLINE_STREAM_SUFFIX}" for s in chunk)
        urls.append(BINANCE_WS_BASE + streams)
    return urls


def load_ws_urls(path: Path = STREAMS_FILE) -> list[str]:
    urls = [ln.strip() for ln in path.read_text().splitlines() if ln.strip().startswith("wss://")]
    return urls if urls else []


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def ns_to_ms(ns: int) -> int:
    return ns // 1_000_000


def ns_span_ms(start_ns: int, end_ns: int) -> float:
    if start_ns <= 0 or end_ns <= 0:
        return 0.0
    return max(0.0, (end_ns - start_ns) / 1_000_000)


def fmt_stats(values: list[float]) -> str:
    if not values:
        return "n=0"
    ordered = sorted(values)
    current = values[-1]
    avg = sum(ordered) / len(ordered)
    idx = min(len(ordered) - 1, int((len(ordered) - 1) * 0.95))
    p95 = ordered[idx]
    return (
        f"n={len(ordered)} 本次={current:.3f}ms avg={avg:.3f}ms "
        f"p95={p95:.3f}ms min={ordered[0]:.3f}ms max={ordered[-1]:.3f}ms"
    )


def is_likely_closed(raw: bytes) -> bool:
    return any(m in raw for m in _CLOSED_MARKERS)


@dataclass
class BatchStats:
    close_time_ms: int = 0
    first_recv_ns: int = 0
    last_recv_ns: int = 0
    first_proc_ns: int = 0
    last_proc_ns: int = 0
    first_symbol: str = ""
    last_symbol: str = ""
    first_shard_id: int = 0
    last_shard_id: int = 0
    event_min_ms: int = 0
    event_max_ms: int = 0
    event_to_recv_ms: list[float] = field(default_factory=list)
    recv_to_parse_ms: list[float] = field(default_factory=list)
    shard_counts: dict[int, int] = field(default_factory=dict)
    shard_recv_min_ns: dict[int, int] = field(default_factory=dict)
    shard_recv_max_ns: dict[int, int] = field(default_factory=dict)
    shard_event_min_ms: dict[int, int] = field(default_factory=dict)
    shard_event_max_ms: dict[int, int] = field(default_factory=dict)

    def update(
        self,
        *,
        shard_id: int,
        symbol: str,
        close_time_ms: int,
        event_time_ms: int,
        recv_ns: int,
        proc_ns: int,
    ) -> None:
        self.close_time_ms = close_time_ms
        if self.first_recv_ns <= 0 or recv_ns < self.first_recv_ns:
            self.first_recv_ns = recv_ns
            self.first_proc_ns = proc_ns
            self.first_symbol = symbol
            self.first_shard_id = shard_id
        if recv_ns >= self.last_recv_ns:
            self.last_recv_ns = recv_ns
            self.last_proc_ns = proc_ns
            self.last_symbol = symbol
            self.last_shard_id = shard_id

        if event_time_ms > 0:
            self.event_min_ms = event_time_ms if self.event_min_ms <= 0 else min(self.event_min_ms, event_time_ms)
            self.event_max_ms = max(self.event_max_ms, event_time_ms)
            self.event_to_recv_ms.append(float(ns_to_ms(recv_ns) - event_time_ms))
        self.recv_to_parse_ms.append(ns_span_ms(recv_ns, proc_ns))

        self.shard_counts[shard_id] = self.shard_counts.get(shard_id, 0) + 1
        shard_min_recv = self.shard_recv_min_ns.get(shard_id, 0)
        self.shard_recv_min_ns[shard_id] = recv_ns if shard_min_recv <= 0 else min(shard_min_recv, recv_ns)
        self.shard_recv_max_ns[shard_id] = max(self.shard_recv_max_ns.get(shard_id, 0), recv_ns)
        if event_time_ms > 0:
            shard_min_event = self.shard_event_min_ms.get(shard_id, 0)
            self.shard_event_min_ms[shard_id] = (
                event_time_ms if shard_min_event <= 0 else min(shard_min_event, event_time_ms)
            )
            self.shard_event_max_ms[shard_id] = max(self.shard_event_max_ms.get(shard_id, 0), event_time_ms)

    def shard_lines(self) -> str:
        parts: list[str] = []
        for shard_id in sorted(self.shard_counts):
            recv_min = self.shard_recv_min_ns.get(shard_id, 0)
            recv_max = self.shard_recv_max_ns.get(shard_id, 0)
            event_min = self.shard_event_min_ms.get(shard_id, 0)
            event_max = self.shard_event_max_ms.get(shard_id, 0)
            parts.append(
                f"第{shard_id}路：symbols={self.shard_counts[shard_id]} "
                f"E跨度={max(0, event_max - event_min)}ms "
                f"入站跨度={ns_span_ms(recv_min, recv_max):.3f}ms"
            )
        return "；".join(parts) if parts else "n=0"


class BitmapBatchTracker:
    """Bounded batch history; O(1) per symbol, O(log capacity) per new batch."""

    __slots__ = ("expected", "symbol_index", "_bits", "_tally", "_reported", "_stats",
                 "_max_batches", "_retained", "_expired_through", "_on_expire")

    def __init__(self, symbols: list[str], *, max_batches: int = TRACKER_MAX_BATCHES,
                 on_expire: Optional[Callable[[str], None]] = None) -> None:
        if isinstance(max_batches, bool) or not isinstance(max_batches, int) or max_batches < 1:
            raise ValueError("max_batches must be a positive integer")
        self._max_batches = max_batches
        self._retained: list[int] = []  # Min-heap covering pending AND completed batches.
        self._expired_through: Optional[int] = None
        self._on_expire = on_expire
        self.expected = len(symbols)
        self.symbol_index = {s: i for i, s in enumerate(symbols)}
        self._bits: dict[int, array] = {}
        self._tally: dict[int, int] = {}
        self._reported: set[int] = set()
        self._stats: dict[int, BatchStats] = {}

    def on_closed(
        self,
        *,
        shard_id: int,
        symbol: str,
        open_time_ms: int,
        close_time_ms: int,
        event_time_ms: int,
        recv_ns: int,
        proc_ns: int,
    ) -> tuple[bool, int, Optional[BatchStats]]:
        if open_time_ms in self._reported:
            return False, self.expected, None

        idx = self.symbol_index.get(symbol)
        if idx is None:
            return False, 0, None

        if self._expired_through is not None and open_time_ms <= self._expired_through:
            return False, 0, None

        bits = self._bits.get(open_time_ms)
        if bits is None:
            if len(self._retained) >= self._max_batches:
                if open_time_ms <= self._retained[0]:
                    return False, 0, None  # Never reopen history outside the retained window.
                expired = heapq.heappop(self._retained)
                self._expired_through = expired
                count = self._tally.pop(expired, None)
                self._bits.pop(expired, None)
                self._stats.pop(expired, None)
                self._reported.discard(expired)
                if count is not None and self._on_expire is not None:
                    self._on_expire(
                        f"[batch-expired] open_time_ms={expired} received={count}/{self.expected} "
                        f"missing={self.expected - count} reason=retention-capacity"
                    )
            heapq.heappush(self._retained, open_time_ms)
            bits = array("B", [0]) * self.expected
            self._bits[open_time_ms] = bits
            self._tally[open_time_ms] = 0
            self._stats[open_time_ms] = BatchStats(close_time_ms=close_time_ms)

        if bits[idx]:
            return False, self._tally[open_time_ms], None

        bits[idx] = 1
        n = self._tally[open_time_ms] + 1
        self._tally[open_time_ms] = n
        self._stats[open_time_ms].update(
            shard_id=shard_id,
            symbol=symbol,
            close_time_ms=close_time_ms,
            event_time_ms=event_time_ms,
            recv_ns=recv_ns,
            proc_ns=proc_ns,
        )
        if n >= self.expected:
            self._reported.add(open_time_ms)
            self._bits.pop(open_time_ms, None)
            self._tally.pop(open_time_ms, None)
            return True, n, self._stats.pop(open_time_ms, None)
        return False, n, None


class KlineSubscriber:
    def __init__(self, urls: list[str], symbols: list[str]) -> None:
        self.urls = urls
        self.symbols = symbols
        self.tracker = BitmapBatchTracker(symbols, on_expire=self._log)
        self.raw_queue: asyncio.Queue[tuple[int, bytes, int]]
        self.tracker_queue: asyncio.Queue
        self.log_queue: asyncio.Queue[str]
        self._stats_closed = 0
        self._stats_skipped = 0
        self._stats_raw_dropped = 0

    def _init_queues(self) -> None:
        # Must run inside asyncio.run() — queues bind to the active event loop.
        self.raw_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE or 0)
        self.tracker_queue = asyncio.Queue()
        self.log_queue = asyncio.Queue(maxsize=LOG_QUEUE_MAXSIZE)

    async def run(self) -> None:
        self._init_queues()
        n = len(self.symbols)
        print(
            f"symbols={n} interval={KLINE_INTERVAL} shards={len(self.urls)} "
            f"shard_size~{SHARD_SIZE} processors={PROCESSOR_WORKERS} log_each={LOG_EACH_KLINE} "
            f"proxy={BINANCE_WS_PROXY_URL or 'auto'} ping_interval={PING_INTERVAL} ping_timeout={PING_TIMEOUT}",
            flush=True,
        )
        tasks = [
            asyncio.create_task(self._log_writer(), name="log-writer"),
            asyncio.create_task(self._tracker_loop(), name="tracker"),
            *[asyncio.create_task(self._processor(i), name=f"proc-{i}") for i in range(PROCESSOR_WORKERS)],
            *[asyncio.create_task(self._run_shard(i, url), name=f"shard-{i}") for i, url in enumerate(self.urls, 1)],
            asyncio.create_task(self._stats_reporter(), name="stats"),
        ]
        await asyncio.gather(*tasks)

    async def _log_writer(self) -> None:
        while True:
            line = await self.log_queue.get()
            sys.stdout.write(line)
            if not line.endswith("\n"):
                sys.stdout.write("\n")
            sys.stdout.flush()

    def _log(self, msg: str) -> None:
        try:
            self.log_queue.put_nowait(msg)
        except asyncio.QueueFull:
            pass  # drop log rather than block hot path

    async def _stats_reporter(self) -> None:
        while True:
            await asyncio.sleep(60)
            self._log(
                f"[stats] closed={self._stats_closed} fast_skipped={self._stats_skipped} "
                f"raw_dropped={self._stats_raw_dropped} raw_q={self.raw_queue.qsize()} "
                f"tracker_q={self.tracker_queue.qsize()} log_q={self.log_queue.qsize()}"
            )

    async def _run_shard(self, shard_id: int, url: str) -> None:
        backoff = 1.0
        while True:
            try:
                self._log(f"[shard {shard_id}] connecting...")
                connect_kwargs = {
                    "compression": None,  # no permessage-deflate CPU/latency
                    "ping_interval": PING_INTERVAL,
                    "ping_timeout": PING_TIMEOUT,
                    "max_queue": None,
                    "open_timeout": OPEN_TIMEOUT,
                }
                if BINANCE_WS_PROXY_URL:
                    if _CONNECT_SUPPORTS_PROXY:
                        connect_kwargs["proxy"] = BINANCE_WS_PROXY_URL
                    else:
                        self._log(
                            f"[shard {shard_id}] proxy ignored: websockets version does not support proxy=..."
                        )
                async with websockets.connect(url, **connect_kwargs) as ws:
                    n_streams = url.count(KLINE_STREAM_SUFFIX)
                    self._log(f"[shard {shard_id}] connected streams={n_streams}")
                    backoff = 1.0
                    async for raw in ws:
                        if isinstance(raw, str):
                            raw = raw.encode()
                        recv_ns = time.time_ns()
                        # Never await heavy work here — only enqueue
                        try:
                            self.raw_queue.put_nowait((shard_id, raw, recv_ns))
                        except asyncio.QueueFull:
                            self._stats_raw_dropped += 1
            except ConnectionClosed as exc:
                self._log(f"[shard {shard_id}] closed: {exc}")
            except Exception as exc:
                self._log(f"[shard {shard_id}] error: {exc}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _processor(self, worker_id: int) -> None:
        while True:
            shard_id, raw, recv_ns = await self.raw_queue.get()
            try:
                if not is_likely_closed(raw):
                    self._stats_skipped += 1
                    continue
                msg = orjson.loads(raw)
            except orjson.JSONDecodeError:
                continue

            if not isinstance(msg, dict):
                continue
            data = msg.get("data") or msg
            if not isinstance(data, dict):
                continue
            k = data.get("k")
            if not isinstance(k, dict) or not k.get("x"):
                continue

            symbol = (k.get("s") or data.get("s") or "").lower()
            open_time = int(k["t"])
            close_time = int(k["T"])
            event_time = int(data.get("E") or close_time)
            proc_ns = time.time_ns()
            latency_ms = (proc_ns - recv_ns) / 1_000_000

            self._stats_closed += 1

            if LOG_EACH_KLINE:
                payload = orjson.dumps(msg).decode()
                self._log(
                    f"\n=== CLOSED {KLINE_INTERVAL} shard={shard_id} worker={worker_id} symbol={symbol} "
                    f"open={fmt_ts(open_time)} recv_latency_ms={latency_ms:.3f} ===\n{payload}"
                )

            self.tracker_queue.put_nowait(
                (shard_id, symbol, open_time, close_time, event_time, recv_ns, proc_ns)
            )

    async def _tracker_loop(self) -> None:
        while True:
            shard_id, symbol, open_time, close_time, event_time, recv_ns, proc_ns = await self.tracker_queue.get()

            done, progress, batch_stats = self.tracker.on_closed(
                shard_id=shard_id,
                symbol=symbol,
                open_time_ms=open_time,
                close_time_ms=close_time,
                event_time_ms=event_time,
                recv_ns=recv_ns,
                proc_ns=proc_ns,
            )

            if LOG_PROGRESS_EVERY and progress > 0 and progress % LOG_PROGRESS_EVERY == 0:
                self._log(f"[progress] open={fmt_ts(open_time)} {progress}/{self.tracker.expected}")

            if done:
                done_ns = time.time_ns()
                last_ms = done_ns // 1_000_000
                open_ms = open_time
                stats = batch_stats or BatchStats(close_time_ms=close_time)
                first_recv_ms = ns_to_ms(stats.first_recv_ns)
                last_recv_ms = ns_to_ms(stats.last_recv_ns)
                first_proc_ms = ns_to_ms(stats.first_proc_ns)
                last_proc_ms = ns_to_ms(stats.last_proc_ns)
                close_to_first_recv = first_recv_ms - close_time
                close_to_last_recv = last_recv_ms - close_time
                close_to_done = last_ms - close_time
                recv_spread = ns_span_ms(stats.first_recv_ns, stats.last_recv_ns)
                proc_spread = ns_span_ms(stats.first_proc_ns, stats.last_proc_ns)
                event_spread = max(0, stats.event_max_ms - stats.event_min_ms)
                event_to_last_recv = last_recv_ms - stats.event_min_ms if stats.event_min_ms > 0 else 0
                self._log(
                    f"\n*** 全部 {self.tracker.expected} 个交易对 {KLINE_INTERVAL} K线已收盘 ***\n"
                    f"  开盘时间: {fmt_ts(open_ms)}\n"
                    f"  K线结束: {fmt_ts(close_time)}\n"
                    f"  首条收包: {fmt_ts(first_recv_ms)} symbol={stats.first_symbol} shard={stats.first_shard_id}\n"
                    f"  末条收包: {fmt_ts(last_recv_ms)} symbol={stats.last_symbol} shard={stats.last_shard_id}\n"
                    f"  首条处理完成: {fmt_ts(first_proc_ms)}\n"
                    f"  最后一条处理完成: {fmt_ts(proc_ns // 1_000_000)}\n"
                    f"  全量汇总完成: {fmt_ts(last_ms)}\n"
                    f"  距开盘耗时: {last_ms - open_ms} ms\n"
                    f"  收盘至首条收包: {close_to_first_recv:+d} ms\n"
                    f"  收盘至末条收包: {close_to_last_recv:+d} ms\n"
                    f"  收盘至全量汇总完成: {close_to_done:+d} ms\n"
                    f"  全市场 Binance WS事件时间分布跨度(max E - min E): {event_spread} ms\n"
                    f"  全市场 WS入站分布跨度(first recv -> last recv): {recv_spread:.3f} ms\n"
                    f"  全市场 处理完成分布跨度(first parse -> last parse): {proc_spread:.3f} ms\n"
                    f"  首个 Binance事件(E) -> 末条收包: {event_to_last_recv} ms\n"
                    f"  单条 Binance WS事件(E)->本机收包: {fmt_stats(stats.event_to_recv_ms)}\n"
                    f"  本机收包->解析完成: {fmt_stats(stats.recv_to_parse_ms)}\n"
                    f"  按 Binance WS 分片统计: {stats.shard_lines()}\n"
                    f"  末条收包至汇总耗时: {(done_ns - recv_ns) / 1_000_000:.3f} ms\n"
                )


def _install_uvloop() -> None:
    if not _env_bool("USE_UVLOOP", True):
        return
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass


async def _async_main() -> None:
    symbols = load_symbols()
    urls = build_ws_urls(symbols) if _env_bool("DYNAMIC_SHARDS", True) else load_ws_urls()
    if not urls:
        urls = build_ws_urls(symbols)
    if len(symbols) != 528:
        print(f"warning: expected 528 symbols, got {len(symbols)}", file=sys.stderr)
    await KlineSubscriber(urls, symbols).run()


def main() -> None:
    _install_uvloop()
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nstopped", flush=True)


if __name__ == "__main__":
    main()

