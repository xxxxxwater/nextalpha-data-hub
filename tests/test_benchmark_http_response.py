import unittest
from unittest.mock import patch

import httpx

from scripts import benchmark_rest_proxy_shards, benchmark_rest_shard_latency


BENCHMARKS = (benchmark_rest_proxy_shards, benchmark_rest_shard_latency)


class BenchmarkResponseTests(unittest.IsolatedAsyncioTestCase):
    async def fetch(self, benchmark, handler):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await benchmark.fetch_rest_kline(
                client, benchmark.RequestPacer(0), "https://exchange.test",
                "BTCUSDT", "1m", 1, 59999, True,
            )

    async def test_response_errors_keep_http_measurements(self):
        for benchmark in BENCHMARKS:
            for status, body, error in (
                (503, b"maintenance", "HTTPStatusError"),
                (200, b"bad json", "JSONDecodeError"),
                (429, b"retry later", "rate_limited"),
            ):
                with self.subTest(benchmark=benchmark.__name__, status=status):
                    response = httpx.Response(status, content=body, headers={"x-mbx-used-weight-1m": "17"})
                    with patch.object(benchmark, "now_ms", side_effect=(100, 125, 140)):
                        result = await self.fetch(benchmark, lambda request: response)
                    self.assertEqual(result["request_cost_ms"], 25)
                    self.assertEqual(result["status"], status)
                    self.assertEqual(result["http_ok"], status == 200)
                    self.assertEqual(result["rate_limited"], status == 429)
                    self.assertEqual(result["response_bytes"], len(body))
                    self.assertEqual(result["used_weight_1m"], 17)
                    self.assertEqual(result["error"], error)
                    self.assertFalse(result["ok"])

    async def test_transport_failures_still_have_no_status(self):
        def offline(request):
            raise httpx.ConnectError("offline", request=request)

        for benchmark in BENCHMARKS:
            with self.subTest(benchmark=benchmark.__name__):
                result = await self.fetch(benchmark, offline)
                self.assertEqual(result["status"], 0)
                self.assertFalse(result["http_ok"])
                self.assertEqual(result["error"], "ConnectError")
                self.assertFalse(result["ok"])
