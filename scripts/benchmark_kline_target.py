from argparse import Namespace
from datetime import datetime
from typing import Any


THREE_DAYS_MS = 3 * 86_400_000


def uses_symbol_close(args: Namespace) -> bool:
    return args.interval == "3d" and args.use_end_time and args.end_time_ms <= 0


def match_close_time(rows: list, end_time_ms: int | None, symbol_close: bool) -> int | None:
    if end_time_ms is None:
        if rows and isinstance(rows[-1], list) and len(rows[-1]) >= 8:
            return int(rows[-1][6])
        return None

    for row in reversed(rows):
        if not isinstance(row, list) or len(row) < 8:
            continue
        close_ms = int(row[6])
        if symbol_close:
            # endTime filters by open time. The newest completed 3d bar must
            # close within one interval of the frozen exchange-time snapshot.
            full_interval = close_ms - int(row[0]) + 1 == THREE_DAYS_MS
            if full_interval and end_time_ms <= close_ms < end_time_ms + THREE_DAYS_MS:
                return close_ms
        elif close_ms == end_time_ms:
            return close_ms
    return None


def describe_target(end_time_ms: int | None, symbol_close: bool) -> dict[str, Any]:
    if symbol_close and end_time_ms is not None:
        return {
            "end_time_ms": None,
            "end_time": "",
            "latest_closed_as_of_ms": end_time_ms + THREE_DAYS_MS,
            "request_end_time_ms": end_time_ms,
        }
    formatted = ""
    if end_time_ms is not None:
        formatted = datetime.fromtimestamp(end_time_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return {"end_time_ms": end_time_ms, "end_time": formatted}


def markdown_target(target: dict[str, Any]) -> list[str]:
    if "latest_closed_as_of_ms" in target:
        return [f"- latest_closed_as_of_ms (per symbol): {target['latest_closed_as_of_ms']}"]
    return [
        f"- target_close_time_ms: {target['end_time_ms']}",
        f"- target_close_time: {target['end_time']}",
    ]
