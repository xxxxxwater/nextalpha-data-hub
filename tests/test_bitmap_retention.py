"""Execute production tracker classes offline, without importing network clients."""
import ast
from array import array
from dataclasses import dataclass, field
import heapq
from pathlib import Path
import unittest

SOURCE = Path(__file__).resolve().parents[1] / "binance-kline-5m-ws/main.py"
tree = ast.parse(SOURCE.read_text())
names = {"BatchStats", "BitmapBatchTracker", "ns_to_ms", "ns_span_ms"}
nodes = [n for n in tree.body if getattr(n, "name", None) in names]
namespace = dict(array=array, dataclass=dataclass, field=field, heapq=heapq, TRACKER_MAX_BATCHES=128)
exec(compile(ast.fix_missing_locations(ast.Module(body=[
    ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
    *nodes], type_ignores=[])), str(SOURCE), "exec"), namespace)
Tracker = namespace["BitmapBatchTracker"]


def send(tracker, timestamp, symbol="a"):
    return tracker.on_closed(shard_id=1, symbol=symbol, open_time_ms=timestamp,
                             close_time_ms=timestamp + 100, event_time_ms=timestamp + 100,
                             recv_ns=(timestamp + 101) * 1000000, proc_ns=(timestamp + 102) * 1000000)


class RetentionTests(unittest.TestCase):
    def test_incomplete_history_bounded(self):
        tracker = Tracker(["a", "b"])
        for t in range(1000):
            send(tracker, t)
        self.assertLessEqual(len(tracker._bits), 128)
        self.assertLessEqual(len(tracker._stats), 128)
        self.assertLessEqual(len(tracker._tally), 128)

    def test_complete_history_bounded(self):
        tracker = Tracker(["a"])
        for t in range(1000):
            self.assertTrue(send(tracker, t)[0])
        self.assertLessEqual(len(tracker._reported), 128)

    def test_late_expired_never_recreates(self):
        tracker = Tracker(["a", "b"])
        for t in range(1000):
            send(tracker, t)
        send(tracker, 0, "b")
        self.assertNotIn(0, tracker._bits)
        self.assertNotIn(0, tracker._reported)

    def test_window_completion_and_duplicates(self):
        tracker = Tracker(["a", "b"])
        send(tracker, 1000)
        send(tracker, 2000)
        self.assertEqual(send(tracker, 1000)[1], 1)
        done, count, stats = send(tracker, 1000, "b")
        self.assertTrue(done)
        self.assertEqual(count, 2)
        self.assertEqual(len(stats.recv_to_parse_ms), 2)
        self.assertFalse(send(tracker, 1000, "b")[0])
        self.assertNotIn(1000, tracker._bits)

    def test_mixed_bound_and_expiration_summary(self):
        logs = []
        tracker = Tracker(["a", "b"], max_batches=3, on_expire=logs.append)
        for t in (10, 20, 30):
            send(tracker, t)
        send(tracker, 20, "b")
        send(tracker, 5)  # Too old to enter a full window.
        self.assertNotIn(5, tracker._bits)
        send(tracker, 40)
        self.assertEqual(len(logs), 1)
        self.assertIn("open_time_ms=10", logs[0])
        self.assertIn("missing=1", logs[0])
        for t in range(41, 500):
            send(tracker, t)
            if t % 2:
                send(tracker, t, "b")
            self.assertLessEqual(len(tracker._bits) + len(tracker._reported), 3)
            self.assertEqual(set(tracker._bits), set(tracker._stats))
            self.assertEqual(set(tracker._bits), set(tracker._tally))
        send(tracker, 10, "b")
        self.assertNotIn(10, tracker._bits)

    def test_invalid_capacity_and_unknown_symbol(self):
        for cap in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                Tracker(["a"], max_batches=cap)
        tracker = Tracker(["a"], max_batches=1)
        send(tracker, 10)
        send(tracker, 999, "unknown")
        self.assertIn(10, tracker._reported)
        self.assertFalse(send(tracker, 10)[0])


if __name__ == "__main__":
    unittest.main()
