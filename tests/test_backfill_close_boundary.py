import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

from nextalpha_market_watcher.alerts import AlertEngine
from nextalpha_market_watcher.binance_client import BinanceClient, _kline_from_rest_row
from nextalpha_market_watcher.config import AppConfig


def bar(open_ms, close_price):
    return [open_ms, "100", "120", "100", close_price, "1", open_ms + 59_999, "1"]


class BackfillCloseBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def fetch_at(self, start_ms, response_ms, rows):
        clock = Mock()
        clock.time.return_value = start_ms / 1000

        def deliver_stored_response(request):
            clock.time.return_value = response_ms / 1000
            return httpx.Response(200, json=rows)

        async with httpx.AsyncClient(transport=httpx.MockTransport(deliver_stored_response)) as client:
            with patch("nextalpha_market_watcher.binance_client.time", clock):
                return await BinanceClient(AppConfig()).fetch_closed_klines_rest(client, "BTCUSDT", "1m", 2)

    async def test_inflight_rest_snapshot_cannot_preempt_final_websocket_price(self):
        # The server sampled the second bar before its last trade. The response
        # reached the watcher after the minute rolled over.
        backfill = await self.fetch_at(119_950, 120_050, [bar(0, "100"), bar(60_000, "100")])
        engine = AlertEngine(AppConfig(), AsyncMock())
        for candle in backfill:
            await engine.on_closed_1m(candle)
        final = _kline_from_rest_row("BTCUSDT", "1m", bar(60_000, "120"))
        final.source = "ws"
        signals = await engine.on_closed_1m(final)
        self.assertEqual([(s.interval, s.move_pct) for s in signals], [("1m", 20.0)])
        self.assertEqual([c.open_time_ms for c in backfill], [0])

    async def test_inclusive_final_millisecond_belongs_to_the_open_bar(self):
        for start, expected in ((119_999, []), (120_000, [60_000])):
            with self.subTest(start=start):
                result = await self.fetch_at(start, 120_100, [bar(60_000, "100")])
                self.assertEqual([c.open_time_ms for c in result], expected)
