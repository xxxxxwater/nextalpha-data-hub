import unittest
from unittest.mock import AsyncMock

import orjson

from nextalpha_market_watcher.app import App
from nextalpha_market_watcher.config import AppConfig


class BatchAckDuringPublishTest(unittest.IsolatedAsyncioTestCase):
    async def test_batch_ack_is_recorded_before_broadcast_finishes(self):
        hub = AsyncMock()
        app = App(AppConfig(service_mode="klines"), klines_ws=hub)
        self.addAsyncCleanup(app._latency_redis.aclose)
        client = object()
        close_time_ms = 3_599_999
        topic = "klines.all.1h"

        async def broadcast(payload, *, topic):
            row = payload[0]
            self.assertTrue(row["batch_done"])
            await app._on_klines_client_message(
                client,
                orjson.dumps(
                    {
                        "type": "batch_ack",
                        "topic": topic,
                        "close_time_ms": row["close_time_ms"],
                        "batch_push_done_ms": row["batch_push_done_ms"],
                    },
                ).decode(),
            )
            return 1

        hub.broadcast_json.side_effect = broadcast
        await app._flush_close_time(
            topic,
            close_time_ms,
            {"BTCUSDT": {"symbol": "BTCUSDT", "close_time_ms": close_time_ms}},
            notify_summary=False,
            track_latency=False,
            publish_derived=False,
        )

        batch_key = (topic, close_time_ms)
        self.assertEqual(app._batch_ack_clients_by_close.get(batch_key), {id(client)})
        self.assertEqual(len(app._batch_done_to_ack_by_close.get(batch_key, [])), 1)
