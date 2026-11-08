"""Offline tests of the actual receive loop, without optional service dependencies."""
import ast
import asyncio
from decimal import Decimal
from enum import IntEnum
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlparse


class MsgType(IntEnum):
    TEXT = 1
    BINARY = 2
    ERROR = 3
    CLOSE = 4
    CLOSING = 5
    CLOSED = 6
    PING = 7


class FakeWS:
    def __init__(self, messages, on_receive=None, block_when_empty=False):
        self.messages = list(messages)
        self.reads = 0
        self.exited = False
        self.on_receive = on_receive
        self.block_when_empty = block_when_empty

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.exited = True

    async def receive(self):
        self.reads += 1
        if self.reads > 10:
            raise asyncio.CancelledError
        if self.on_receive:
            self.on_receive()
        if self.messages:
            return self.messages.pop(0)
        if self.block_when_empty:
            await asyncio.Event().wait()
        # Match aiohttp's immediate repeated CLOSED result after normal closure.
        return SimpleNamespace(type=MsgType.CLOSED, data=None)


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[1] / 'src/nextalpha_market_watcher/binance_client.py'
        tree = ast.parse(source.read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'BinanceClient')
        method = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_consume_batch')
        metric = SimpleNamespace(labels=lambda *a: metric, inc=lambda: None, observe=lambda *a: None)
        self.namespace = dict(asyncio=asyncio, aiohttp=None, logger=logging.getLogger(__name__),
                              urlparse=urlparse, _ws_proxy_from_env=lambda _: None,
                              WS_KLINE_FAST_SKIPPED=metric, WS_CONNECTIONS=metric,
                              EXCHANGE_TO_SERVICE_MS=metric, KLINES_RECEIVED=metric,
                              touch_ingest=lambda _: None, _raw_has_closed_kline_marker=lambda _: True,
                              _raw_maybe_control_frame=lambda _: False, orjson=SimpleNamespace(loads=json.loads),
                              Decimal=Decimal, time=SimpleNamespace(time=lambda: 1000),
                              ClosedKline=lambda **kw: SimpleNamespace(**kw))
        module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), method], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(source), 'exec'), self.namespace)
        self.client = SimpleNamespace(
            _cfg=SimpleNamespace(binance=SimpleNamespace(ws_base='wss://example.invalid/stream', ws_proxy_url=None)),
            _ws_rebuild_generation={}, _ws_last_event_ms={}, _ws_shard_last_event_ms={},
            _ws_watchdog_alerted={}, _ws_shard_watchdog_alerted={}, _ws_parse_debug_logged=set())

    async def run_case(self, first_type, binary=False, generation=False, interval="1h"):
        first = FakeWS([SimpleNamespace(type=first_type, data=None)])
        if generation:
            first.on_receive = lambda: self.client._ws_rebuild_generation.update({'shard': 1})
        payload = json.dumps({'E': 59999, 'k': dict(x=True, s='BTCUSDT', i='1h', t=0, T=59999,
                                                  o='1', h='2', l='1', c='2', v='1', q='2')})
        second = FakeWS([SimpleNamespace(type=MsgType.BINARY if binary else MsgType.TEXT,
                                        data=payload.encode() if binary else payload)], block_when_empty=True)
        connections = []
        class Session:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def ws_connect(self, *args, **kwargs):
                ws = first if not connections else second
                connections.append(ws)
                return ws
        self.namespace['aiohttp'] = SimpleNamespace(WSMsgType=MsgType, ClientTimeout=lambda **k: None,
                                                    ClientSession=lambda **k: Session())
        async def no_backfill(*args): pass
        self.client._backfill_batch = no_backfill
        queue = asyncio.Queue()
        real_sleep = asyncio.sleep
        async def fast_sleep(_): await real_sleep(0)
        with patch.object(asyncio, 'sleep', fast_sleep):
            task = asyncio.create_task(self.namespace['_consume_batch'](self.client, ['BTCUSDT'], interval, 'shard', queue, 1, interval))
            try:
                row = await asyncio.wait_for(queue.get(), timeout=0.2)
                self.assertEqual(row.symbol, 'BTCUSDT')
                self.assertEqual(row.close_price, Decimal('2'))
                self.assertGreaterEqual(len(connections), 2)
                self.assertEqual(first.reads, 1)
                self.assertTrue(first.exited)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task
            self.assertTrue(second.exited)

    async def test_normal_close_reconnects_and_recovers(self):
        for kind in (MsgType.CLOSE, MsgType.CLOSING, MsgType.CLOSED):
            for binary in (False, True):
                with self.subTest(kind=kind, binary=binary):
                    for interval in ("1m", "1h"):
                        await self.run_case(kind, binary=binary, interval=interval)

    async def test_watchdog_generation_is_checked_on_control_frames(self):
        await self.run_case(MsgType.PING, generation=True)


if __name__ == '__main__': unittest.main()
