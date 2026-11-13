"""Offline tests of production row ACK logic, no WebSocket/Redis/services."""
import ast
import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load_definitions(path, names, namespace):
    tree = ast.parse(path.read_text())
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if node.name in names:
                nodes.append(node)
            else:
                nodes.extend(n for n in node.body if getattr(n, "name", None) in names)
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)
    return namespace


class RowAckTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from dataclasses import dataclass
        ns = load_definitions(ROOT / "src/nextalpha_market_watcher/latency.py", {"LatencyEvent"}, {"dataclass": dataclass})
        self.Event = ns["LatencyEvent"]
        self.now = 1200
        self.log = logging.getLogger("row-ack-test")
        ns = load_definitions(ROOT / "src/nextalpha_market_watcher/app.py", {"_apply_downstream_ack"},
                              {"time": SimpleNamespace(time=lambda: self.now / 1000), "logger": self.log})
        self.apply = ns["_apply_downstream_ack"]
        self.queued = []
        self.ev = self.Event("event", "BTCUSDT", "1h", 900, 950, 1000, "region")
        self.key = ("topic", "event")
        self.client = SimpleNamespace(
            _ack_topic=lambda item: item.get("topic", "default"),
            _pending_latency_lock=asyncio.Lock(), _pending_latency={self.key: self.ev},
            _event_close_time={self.key: ("topic", 999)},
            _ack_service_to_downstream_by_close={}, _ack_ingest_to_downstream_by_close={},
            _ack_clients_by_close={}, _enqueue_latency_events=self.queued.extend)
        self.item = {"topic": "topic", "event_id": "event", "downstream_receive_ms": 1020, "downstream_region": "region"}

    async def test_receipt_metrics_do_not_include_ack_return(self):
        for ingress in (1200, 5000):
            with self.subTest(ingress=ingress):
                self.now = ingress
                self.client._pending_latency[self.key] = self.ev
                self.client._event_close_time[self.key] = ("topic", 999)
                with self.assertLogs(self.log, level="INFO") as logs:
                    await self.apply(self.client, self.item, 7)
                row = self.queued[-1]
                self.assertEqual(row["service_to_downstream_ms"], "20")
                self.assertEqual(row["e2e_ms"], "120")
                self.assertIn("e2e_ms=120", logs.output[-1])
                self.assertIn("service_to_downstream_ms=20", logs.output[-1])
                self.assertEqual(self.client._ack_ingest_to_downstream_by_close[("topic", 999)][-1], 70)
                self.assertEqual(self.client._ack_service_to_downstream_by_close[("topic", 999)][-1], 20)

    async def test_invalid_timestamps_preserve_pending_event(self):
        for invalid in (None, "", "bad", 0, -1, 999, 1201, True, 1020.5):
            with self.subTest(invalid=invalid):
                await self.apply(self.client, dict(self.item, downstream_receive_ms=invalid))
                self.assertIn(self.key, self.client._pending_latency)
                self.assertIn(self.key, self.client._event_close_time)
                self.assertEqual(self.queued, [])
        await self.apply(self.client, self.item)
        self.assertEqual(self.queued[-1]["e2e_ms"], "120")

    async def test_legacy_unique_topic_and_string_timestamp(self):
        item = dict(self.item, downstream_receive_ms="1020")
        del item["topic"]
        await self.apply(self.client, item)
        self.assertEqual(self.queued[-1]["topic"], "topic")
        self.assertNotIn(self.key, self.client._pending_latency)

    async def test_missing_timestamp_and_duplicate_do_not_emit(self):
        item = dict(self.item)
        del item["downstream_receive_ms"]
        await self.apply(self.client, item)
        self.assertFalse(self.queued)
        await self.apply(self.client, self.item)
        await self.apply(self.client, self.item)
        self.assertEqual(len(self.queued), 1)

    async def test_exact_publish_and_ingress_boundaries(self):
        for stamp in (1000, 1200):
            self.client._pending_latency[self.key] = self.ev
            await self.apply(self.client, dict(self.item, downstream_receive_ms=stamp))
            self.assertEqual(self.queued[-1]["service_to_downstream_ms"], str(stamp - 1000))


if __name__ == "__main__":
    unittest.main()
