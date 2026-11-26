import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from nextalpha_market_watcher.app import App
from nextalpha_market_watcher.config import AppConfig


class SymbolRefreshTest(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_failures_keep_ingestion_until_a_valid_change(self):
        initial = ["BTCUSDT"]
        expanded = ["BTCUSDT", "ETHUSDT"]
        for failure in (httpx.ReadTimeout("exchangeInfo timed out"), []):
            with self.subTest(failure=failure):
                config = AppConfig(service_mode="klines")
                config.metrics.enabled = False
                config.binance.symbols_refresh_seconds = 0
                stop = asyncio.Event()
                app = App(config, klines_ws=AsyncMock())
                started = []

                async def ingest(symbols):
                    started.append(symbols)
                    if symbols == expanded:
                        stop.set()
                    await asyncio.Event().wait()

                with (
                    patch.object(app._binance, "fetch_symbols", side_effect=[initial, failure, initial, expanded]),
                    patch.object(app, "_run_ingestion", ingest),
                    patch.object(app, "_run_heartbeat", AsyncMock()),
                    self.assertLogs("nextalpha_market_watcher.app", level="WARNING"),
                ):
                    await asyncio.wait_for(app.run(stop), timeout=4)
                self.assertEqual(started, [initial, expanded])
