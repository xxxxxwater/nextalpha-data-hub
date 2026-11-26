import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx

from scripts import benchmark_kline_target, benchmark_rest_proxy_shards, benchmark_rest_shard_latency


# BTCUSDT and WIFUSDT open their three-day bars on different UTC dates.
AS_OF_MS = 1795564800000  # 2026-11-25 00:00 UTC
OPEN_MS = {"BTCUSDT": 1795305600000, "WIFUSDT": 1795219200000}
THREE_DAYS_MS = 259200000


def bar(open_ms):
    return [open_ms, "1", "1", "1", "1", "1", open_ms + THREE_DAYS_MS - 1, "1"]


class ThreeDayBenchmarkTargetTest(unittest.IsolatedAsyncioTestCase):
    async def test_benchmarks_select_each_symbols_latest_complete_bar(self):
        client_type = httpx.AsyncClient
        for module, options in (
            (benchmark_rest_shard_latency, ["--min-shard-size", "2", "--max-shard-size", "2"]),
            (benchmark_rest_proxy_shards, [
                "--no-probe-proxies", "--proxy-ips", "127.0.0.1", "--required-proxy-count", "1",
                "--proxy-password", "test",
            ]),
        ):
            requests = []

            def respond(request):
                if request.url.path == "/fapi/v1/time":
                    return httpx.Response(200, json={"serverTime": AS_OF_MS})
                params = request.url.params
                requests.append((params["symbol"], params["limit"], int(params["endTime"])))
                return httpx.Response(200, json=[bar(OPEN_MS[params["symbol"]])])

            def client(**kwargs):
                return client_type(transport=httpx.MockTransport(respond))

            with self.subTest(module=module.__name__), TemporaryDirectory() as output:
                argv = ["benchmark", "--symbols", "BTCUSDT,WIFUSDT", "--interval", "3d", "--kline-limit", "1",
                        "--rounds", "1", "--output-dir", output, *options]
                with patch("sys.argv", argv), patch.object(httpx, "AsyncClient", client), redirect_stdout(StringIO()):
                    await module.main_async()
                result = json.loads(next(Path(output).glob("*.json")).read_text())
                self.assertEqual(result["target"]["latest_closed_as_of_ms"], AS_OF_MS)
                self.assertIsNone(result["target"]["end_time_ms"])
                self.assertEqual(result["rounds"][0]["successes"], 2)
                self.assertCountEqual(requests, [(symbol, "1", AS_OF_MS - THREE_DAYS_MS) for symbol in OPEN_MS])

    def test_stale_open_and_truncated_bars_fail_recency_check(self):
        cutoff = AS_OF_MS - THREE_DAYS_MS
        for row in (bar(cutoff - THREE_DAYS_MS), bar(AS_OF_MS), [cutoff + 1, 1, 1, 1, 1, 1, AS_OF_MS - 1, 1]):
            with self.subTest(row=row):
                self.assertIsNone(benchmark_kline_target.match_close_time([row], cutoff, True))

    def test_explicit_close_still_requires_an_exact_match(self):
        expected = AS_OF_MS - 1
        self.assertEqual(benchmark_kline_target.match_close_time([bar(OPEN_MS["BTCUSDT"])], expected, False), expected)
        self.assertIsNone(benchmark_kline_target.match_close_time([bar(OPEN_MS["WIFUSDT"])], expected, False))
