import unittest
from datetime import UTC, datetime

from scripts import benchmark_rest_proxy_shards, benchmark_rest_shard_latency


BENCHMARKS = (benchmark_rest_proxy_shards, benchmark_rest_shard_latency)


class KlineCloseTest(unittest.TestCase):
    def test_weekly_rollover_is_monday_midnight_utc(self):
        monday = int(datetime(2025, 9, 1, tzinfo=UTC).timestamp()) * 1000
        day = 86_400_000
        for benchmark in BENCHMARKS:
            for now, expected in (
                (monday - 1, monday - 7 * day - 1),
                (monday, monday - 1),
                (monday + 3 * day, monday - 1),
            ):
                with self.subTest(benchmark=benchmark.__name__, now=now):
                    self.assertEqual(benchmark.latest_closed_kline_close_ms(now, "1w"), expected)
