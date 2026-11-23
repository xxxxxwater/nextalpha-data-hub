import unittest
from argparse import Namespace
from unittest.mock import patch

from scripts import benchmark_rest_proxy_shards as benchmark


class RestProxyShardLatencyTest(unittest.IsolatedAsyncioTestCase):
    async def test_no_details_keeps_round_request_percentiles(self):
        args = Namespace(
            symbols_per_proxy=2,
            seed=1,
            assignment="balanced",
            per_proxy_concurrency=0,
            per_proxy_min_interval_ms=0,
            rest_base="https://example.invalid",
            interval="1h",
            kline_limit=3,
            require_exact_close=True,
            rest_timeout_seconds=1.0,
            no_details=False,
        )
        endpoints = [
            benchmark.ProxyEndpoint("127.0.0.1", "http://127.0.0.1:3128"),
            benchmark.ProxyEndpoint("127.0.0.2", "http://127.0.0.2:3128"),
        ]
        request_costs = {"BTCUSDT": 10, "ETHUSDT": 20, "SOLUSDT": 90}

        async def fetch_kline(_client, _pacer, _rest_base, symbol, *_args):
            return {"symbol": symbol, "ok": True, "request_cost_ms": request_costs[symbol]}

        with patch.object(benchmark, "fetch_rest_kline", side_effect=fetch_kline):
            detailed = await benchmark.run_round(args, endpoints, list(request_costs), 1, None)
            args.no_details = True
            compact = await benchmark.run_round(args, endpoints, list(request_costs), 1, None)

        self.assertEqual(compact["per_request_ms"], detailed["per_request_ms"])
        self.assertEqual(compact["per_request_ms"]["count"], 3)
        self.assertEqual(benchmark.compact_round_for_stdout(compact)["request_p95_ms"], 90)
        self.assertTrue(all(shard["results"] for shard in detailed["shards"]))
        self.assertTrue(all(shard["results"] == [] for shard in compact["shards"]))
