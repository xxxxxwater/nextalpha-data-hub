import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from nextalpha_market_watcher.app import App
from nextalpha_market_watcher.config import AppConfig, WebSocketPushConfig
from nextalpha_market_watcher.models import ClosedKline


HOUR_MS = 3_600_000


def candle(symbol: str, interval: str, close_ms: int, source: str) -> ClosedKline:
    price = Decimal("1")
    return ClosedKline(
        symbol=symbol,
        interval=interval,
        open_time_ms=close_ms - HOUR_MS + 1,
        close_time_ms=close_ms,
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        volume=price,
        quote_volume=price,
        event_time_ms=close_ms + 1,
        ingest_time_ms=close_ms + 2,
        source=source,
    )


class SymbolRefreshBatchDedupTest(unittest.IsolatedAsyncioTestCase):
    async def test_restart_ignores_backfill_for_already_published_close(self):
        initial_close_ms = HOUR_MS - 1
        next_close_ms = initial_close_ms + HOUR_MS
        cfg = AppConfig(service_mode="klines", websocket=WebSocketPushConfig(klines_major_symbols=[]))
        app = App(cfg, klines_ws=AsyncMock())
        self.addAsyncCleanup(app._latency_redis.aclose)

        async def stream(symbols, *, interval, **_kwargs):
            after_refresh = "SUIUSDT" in symbols
            for symbol in symbols:
                yield candle(symbol, interval, initial_close_ms, "rest" if after_refresh else "ws")
            if after_refresh:
                for symbol in symbols:
                    yield candle(symbol, interval, next_close_ms, "ws")

        with patch.object(app._binance, "iter_closed_klines", stream):
            await app._run_ingestion(["ADAUSDT", "DOTUSDT"])
            await app._run_ingestion(["ADAUSDT", "SUIUSDT"])

        _, close_ms, rows, *_ = app._publish_queue.get_nowait()
        self.assertEqual(close_ms, initial_close_ms)
        self.assertEqual(set(rows), {"ADAUSDT", "DOTUSDT"})
        _, close_ms, rows, *_ = app._publish_queue.get_nowait()
        self.assertEqual(close_ms, next_close_ms)
        self.assertEqual(set(rows), {"ADAUSDT", "SUIUSDT"})
        self.assertTrue(app._publish_queue.empty())
