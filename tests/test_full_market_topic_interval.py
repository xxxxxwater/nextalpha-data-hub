import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from pydantic import ValidationError

from nextalpha_market_watcher.app import App
from nextalpha_market_watcher.binance_client import _kline_from_rest_row
from nextalpha_market_watcher.cli import _load_config
from nextalpha_market_watcher.config import WebSocketPushConfig


DOCKER_CONFIG = Path(__file__).resolve().parents[1] / "config.docker.yaml"


class FullMarketTopicIntervalTest(unittest.IsolatedAsyncioTestCase):
    async def test_docker_subscription_and_broadcast_match_the_hourly_contract(self):
        for overrides, expected_interval, expected_topic in (
            ({}, "1h", "klines.all.1h"),
            ({"klines_interval": "5m", "klines_all_topic": "klines.all.5m"}, "5m", "klines.all.5m"),
        ):
            with self.subTest(topic=expected_topic):
                config = _load_config(str(DOCKER_CONFIG), service_mode="klines")
                config.websocket = WebSocketPushConfig(**(config.websocket.model_dump() | overrides))
                config.webhook.enabled = False
                hub = AsyncMock()
                hub.broadcast_json.return_value = 1
                app = App(config, klines_ws=hub)
                symbol = "TESTUSDT"
                await app._reset_full_klines_batch_state([symbol])
                intervals = []

                async def market(symbols, interval, **kwargs):
                    intervals.append(interval)
                    duration = {"1h": 3600000, "5m": 300000}[interval]
                    row = [0, "1", "2", "1", "2", "3", duration - 1, "6"]
                    yield _kline_from_rest_row(symbol, interval, row)

                with patch.object(app._binance, "iter_closed_klines", market):
                    await app._run_1h_publish([symbol])
                topic, close, rows, first_ingest, _, _ = app._publish_queue.get_nowait()
                await app._flush_close_time(topic, close, rows, first_ingest, notify_summary=False, track_latency=False)
                hub.broadcast_json.assert_awaited_once()
                args, kwargs = hub.broadcast_json.await_args
                self.assertEqual(intervals, [expected_interval])
                self.assertEqual(kwargs["topic"], expected_topic)
                self.assertEqual(
                    [(row["topic"], row["interval"]) for row in args[0]],
                    [(expected_topic, expected_interval)],
                )

    def test_invalid_canonical_topic_is_rejected(self):
        with self.assertRaisesRegex(ValidationError, "klines.all.1h.*5m"):
            WebSocketPushConfig(klines_interval="5m", klines_all_topic="klines.all.1h")

    def test_custom_and_automatic_topic_names_remain_available(self):
        for topic in ("", "custom.rolling", "klines.all.consolidated"):
            with self.subTest(topic=topic):
                config = WebSocketPushConfig(klines_interval="5m", klines_all_topic=topic)
                self.assertEqual(config.klines_all_topic, topic)
