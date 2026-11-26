#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import random
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import urlparse

import httpx

if __package__:
    from . import benchmark_kline_target as kline_target
else:
    import benchmark_kline_target as kline_target


DEFAULT_MAJOR_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT"
DEFAULT_OUTPUT_DIR = "logs/rest_shard_latency"
INTERVAL_MS = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "8h": 8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "3d": 3 * 24 * 60 * 60_000,
    "1w": 7 * 24 * 60 * 60_000,
}


class RequestPacer:
    def __init__(self, min_interval_ms: int) -> None:
        self._min_interval_ms = max(0, min_interval_ms)
        self._lock = asyncio.Lock()
        self._next_ms = 0

    async def wait(self) -> None:
        if self._min_interval_ms <= 0:
            return
        async with self._lock:
            now = now_ms()
            wait_ms = self._next_ms - now
            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000)
                now = now_ms()
            self._next_ms = max(now, self._next_ms) + self._min_interval_ms


def now_ms() -> int:
    return int(time.time() * 1000)


def md_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def parse_csv(raw: str) -> list[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def parse_optional_int(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if p <= 0:
        return ordered[0]
    if p >= 100:
        return ordered[-1]
    rank = math.ceil((p / 100.0) * len(ordered)) - 1
    idx = min(max(rank, 0), len(ordered) - 1)
    return ordered[idx]


def summarize(values: list[float]) -> dict[str, float]:
    if not values:
        return {"count": 0.0, "min": 0.0, "mean": 0.0, "p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    ordered = sorted(values)
    return {
        "count": float(len(ordered)),
        "min": ordered[0],
        "mean": mean(ordered),
        "p50": percentile(ordered, 50),
        "p90": percentile(ordered, 90),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
        "max": ordered[-1],
    }


def no_proxy_matches_host(host: str, no_proxy: str) -> bool:
    host = host.lower().strip(".")
    for raw in no_proxy.split(","):
        pat = raw.strip().lower()
        if not pat:
            continue
        if pat == "*":
            return True
        if pat.startswith("."):
            pat = pat[1:]
        if host == pat or host.endswith("." + pat):
            return True
    return False


def proxy_from_env(rest_base: str) -> str | None:
    host = urlparse(rest_base).hostname or ""
    no_proxy = (os.getenv("NO_PROXY") or os.getenv("no_proxy") or "").strip()
    if host and no_proxy and no_proxy_matches_host(host, no_proxy):
        return None
    return (
        (os.getenv("BINANCE_HTTP_PROXY") or "").strip()
        or (os.getenv("HTTPS_PROXY") or "").strip()
        or (os.getenv("HTTP_PROXY") or "").strip()
        or None
    )


async def fetch_symbols(rest_base: str, proxy: str | None, timeout_seconds: float) -> list[str]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds), proxy=proxy) as client:
        resp = await client.get(f"{rest_base.rstrip('/')}/fapi/v1/exchangeInfo")
        resp.raise_for_status()
        data = resp.json()
    symbols: list[str] = []
    for item in data.get("symbols", []):
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        if item.get("status") != "TRADING":
            continue
        symbol = item.get("symbol")
        if symbol:
            symbols.append(str(symbol).upper())
    return sorted(set(symbols))


async def fetch_server_time(rest_base: str, proxy: str | None, timeout_seconds: float) -> dict[str, Any]:
    url = f"{rest_base.rstrip('/')}/fapi/v1/time"
    local_before_ms = now_ms()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds), proxy=proxy) as client:
            resp = await client.get(url)
            local_after_ms = now_ms()
            resp.raise_for_status()
            server_time_ms = int(resp.json()["serverTime"])
    except Exception as exc:
        return {
            "ok": False,
            "error": type(exc).__name__,
            "message": str(exc),
            "fallback_local_ms": now_ms(),
        }
    midpoint_ms = (local_before_ms + local_after_ms) / 2
    return {
        "ok": True,
        "server_time_ms": server_time_ms,
        "local_before_ms": local_before_ms,
        "local_after_ms": local_after_ms,
        "rtt_ms": local_after_ms - local_before_ms,
        "offset_ms": server_time_ms - midpoint_ms,
    }


def latest_closed_kline_close_ms(server_time_ms: int, interval: str) -> int:
    interval_ms = INTERVAL_MS.get(interval)
    if interval_ms is None:
        raise ValueError(f"Unsupported interval for automatic endTime: {interval!r}")
    current_open_ms = (server_time_ms // interval_ms) * interval_ms
    return current_open_ms - 1


def circular_slice(symbols: list[str], start: int, size: int) -> list[str]:
    if not symbols:
        return []
    return [symbols[(start + idx) % len(symbols)] for idx in range(size)]


def select_round_symbols(args: argparse.Namespace, all_symbols: list[str], shard_size: int, round_index: int) -> list[str]:
    if args.sample_mode == "prefix":
        return all_symbols[:shard_size]
    if args.sample_mode == "shuffle":
        shuffled = list(all_symbols)
        random.Random(args.seed + shard_size * 1009 + round_index).shuffle(shuffled)
        return shuffled[:shard_size]
    start = args.symbol_offset + (round_index - 1) * shard_size
    return circular_slice(all_symbols, start, shard_size)


async def fetch_rest_kline(
    client: httpx.AsyncClient,
    pacer: RequestPacer,
    rest_base: str,
    symbol: str,
    interval: str,
    limit: int,
    end_time_ms: int | None,
    require_exact_close: bool,
    symbol_close: bool = False,
) -> dict[str, Any]:
    await pacer.wait()
    request_start_ms = now_ms()
    url = f"{rest_base.rstrip('/')}/fapi/v1/klines"
    params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time_ms is not None:
        params["endTime"] = end_time_ms
    try:
        resp = await client.get(url, params=params)
        response_ms = now_ms()
        used_weight_1m = parse_optional_int(resp.headers.get("x-mbx-used-weight-1m"))
        used_weight = parse_optional_int(resp.headers.get("x-mbx-used-weight"))
        status = resp.status_code
        result: dict[str, Any] = {
            "symbol": symbol,
            "status": status,
            "http_ok": 200 <= status < 300,
            "data_ok": False,
            "ok": False,
            "rate_limited": status in (418, 429),
            "used_weight_1m": used_weight_1m,
            "used_weight": used_weight,
            "request_start_ms": request_start_ms,
            "response_ms": response_ms,
            "request_cost_ms": response_ms - request_start_ms,
            "response_bytes": len(resp.content),
        }
        if status in (418, 429):
            result["error"] = "rate_limited"
            return result
        resp.raise_for_status()
        rows = resp.json()
        result["rows"] = len(rows) if isinstance(rows, list) else 0
        if not isinstance(rows, list):
            result["error"] = "unexpected_response"
            return result
        matched_close_ms = kline_target.match_close_time(rows, end_time_ms, symbol_close)
        result["matched_close_time_ms"] = matched_close_ms
        result["data_ok"] = matched_close_ms is not None
        if require_exact_close and end_time_ms is not None and matched_close_ms is None:
            result["error"] = "missing_close_time"
        result["ok"] = bool(result["http_ok"] and (result["data_ok"] or not require_exact_close))
        return result
    except Exception as exc:
        response_ms = now_ms()
        return {
            "symbol": symbol,
            "status": 0,
            "http_ok": False,
            "data_ok": False,
            "ok": False,
            "rate_limited": False,
            "error": type(exc).__name__,
            "message": str(exc),
            "request_start_ms": request_start_ms,
            "response_ms": response_ms,
            "request_cost_ms": response_ms - request_start_ms,
        }


async def run_shard_round(
    client: httpx.AsyncClient,
    pacer: RequestPacer,
    args: argparse.Namespace,
    shard_size: int,
    round_index: int,
    measured: bool,
    symbols: list[str],
    end_time_ms: int | None,
) -> dict[str, Any]:
    concurrency = args.rest_concurrency if args.rest_concurrency > 0 else len(symbols)
    sem = asyncio.Semaphore(max(1, min(concurrency, len(symbols))))
    round_start_ms = now_ms()

    async def one(symbol: str) -> dict[str, Any]:
        async with sem:
            return await fetch_rest_kline(
                client,
                pacer,
                args.rest_base,
                symbol,
                args.interval,
                args.kline_limit,
                end_time_ms,
                args.require_exact_close,
                kline_target.uses_symbol_close(args),
            )

    results = await asyncio.gather(*(one(symbol) for symbol in symbols))
    round_end_ms = now_ms()
    request_starts = [int(item["request_start_ms"]) for item in results if int(item.get("request_start_ms") or 0) > 0]
    response_times = [int(item["response_ms"]) for item in results if int(item.get("response_ms") or 0) > 0]
    request_costs = [float(item["request_cost_ms"]) for item in results if int(item.get("request_cost_ms") or 0) >= 0]
    successes = sum(1 for item in results if item.get("ok"))
    failures = len(results) - successes
    rate_limited = sum(1 for item in results if item.get("rate_limited"))
    used_weight_1m_values = [
        int(item["used_weight_1m"]) for item in results if item.get("used_weight_1m") is not None
    ]
    return {
        "event": "rest_shard_round",
        "shard_size": shard_size,
        "round_index": round_index,
        "measured": measured,
        "symbols": symbols,
        "request_start_ms": round_start_ms,
        "request_end_ms": round_end_ms,
        "wall_ms": round_end_ms - round_start_ms,
        "first_request_ms": min(request_starts) if request_starts else round_start_ms,
        "last_response_ms": max(response_times) if response_times else round_end_ms,
        "request_span_ms": (max(response_times) - min(request_starts)) if request_starts and response_times else 0,
        "successes": successes,
        "failures": failures,
        "rate_limited": rate_limited,
        "used_weight_1m_max": max(used_weight_1m_values) if used_weight_1m_values else None,
        "request_cost_ms_values": request_costs,
        "per_request_ms": summarize(request_costs),
        "results": [] if args.no_details else results,
    }


async def run_shard_round_with_client(
    pacer: RequestPacer,
    args: argparse.Namespace,
    proxy: str | None,
    shard_size: int,
    round_index: int,
    measured: bool,
    symbols: list[str],
    end_time_ms: int | None,
) -> dict[str, Any]:
    timeout = httpx.Timeout(args.rest_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout, proxy=proxy) as client:
        return await run_shard_round(client, pacer, args, shard_size, round_index, measured, symbols, end_time_ms)


def summarize_rounds(
    rounds: list[dict[str, Any]],
    selected_symbol_count: int,
    parallel_ips: int,
) -> list[dict[str, Any]]:
    by_size: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for item in rounds:
        if item.get("measured"):
            by_size[int(item["shard_size"])].append(item)

    summary: list[dict[str, Any]] = []
    for shard_size in sorted(by_size):
        items = by_size[shard_size]
        wall_values = [float(item["wall_ms"]) for item in items]
        span_values = [float(item["request_span_ms"]) for item in items]
        request_values: list[float] = []
        successes = 0
        failures = 0
        rate_limited = 0
        for item in items:
            successes += int(item["successes"])
            failures += int(item["failures"])
            rate_limited += int(item["rate_limited"])
            request_values.extend(float(value) for value in item.get("request_cost_ms_values") or [])
        if not request_values:
            for item in items:
                count = int(item["per_request_ms"]["count"])
                request_values.extend([float(item["per_request_ms"]["mean"])] * count)

        total_attempts = successes + failures
        total_wall_seconds = sum(wall_values) / 1000.0
        success_ratio = (successes / total_attempts) if total_attempts else 0.0
        throughput = (successes / total_wall_seconds) if total_wall_seconds > 0 else 0.0
        shard_count = math.ceil(selected_symbol_count / shard_size) if shard_size > 0 else 0
        ip_waves = math.ceil(shard_count / max(1, parallel_ips)) if shard_count else 0
        wall = summarize(wall_values)
        span = summarize(span_values)
        per_request = summarize(request_values)
        summary.append(
            {
                "shard_size": shard_size,
                "rounds": len(items),
                "success_ratio": success_ratio,
                "successes": successes,
                "failures": failures,
                "rate_limited": rate_limited,
                "wall_ms": wall,
                "request_span_ms": span,
                "per_request_ms": per_request,
                "throughput_symbols_per_second": throughput,
                "p50_wall_ms_per_symbol": (wall["p50"] / shard_size) if shard_size else 0.0,
                "estimated_shards_for_selected_symbols": shard_count,
                "estimated_full_serial_p50_ms": wall["p50"] * shard_count,
                "estimated_full_serial_p95_ms": wall["p95"] * shard_count,
                "estimated_full_parallel_ips": max(1, parallel_ips),
                "estimated_full_parallel_p50_ms": wall["p50"] * ip_waves,
                "estimated_full_parallel_p95_ms": wall["p95"] * ip_waves,
            },
        )
    return summary


def choose_recommendations(args: argparse.Namespace, summary: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [
        item
        for item in summary
        if item["success_ratio"] >= args.min_success_ratio
        and item["rate_limited"] == 0
        and (args.target_latency_ms <= 0 or item["wall_ms"]["p95"] <= args.target_latency_ms)
    ]
    if not eligible:
        return {
            "eligible_count": 0,
            "reason": "no shard size met min_success_ratio/rate_limit/target_latency filters",
            "best_by_throughput": None,
            "best_by_wall_p95": None,
        }
    best_by_throughput = max(
        eligible,
        key=lambda item: (item["throughput_symbols_per_second"], -item["wall_ms"]["p95"], item["shard_size"]),
    )
    best_by_wall_p95 = min(
        eligible,
        key=lambda item: (item["wall_ms"]["p95"], -item["throughput_symbols_per_second"], item["shard_size"]),
    )
    return {
        "eligible_count": len(eligible),
        "filters": {
            "min_success_ratio": args.min_success_ratio,
            "target_latency_ms": args.target_latency_ms,
            "rate_limited_required": 0,
        },
        "best_by_throughput": {
            "shard_size": best_by_throughput["shard_size"],
            "wall_p50_ms": best_by_throughput["wall_ms"]["p50"],
            "wall_p95_ms": best_by_throughput["wall_ms"]["p95"],
            "throughput_symbols_per_second": best_by_throughput["throughput_symbols_per_second"],
            "estimated_full_parallel_p95_ms": best_by_throughput["estimated_full_parallel_p95_ms"],
        },
        "best_by_wall_p95": {
            "shard_size": best_by_wall_p95["shard_size"],
            "wall_p50_ms": best_by_wall_p95["wall_ms"]["p50"],
            "wall_p95_ms": best_by_wall_p95["wall_ms"]["p95"],
            "throughput_symbols_per_second": best_by_wall_p95["throughput_symbols_per_second"],
            "estimated_full_parallel_p95_ms": best_by_wall_p95["estimated_full_parallel_p95_ms"],
        },
    }


def render_markdown(payload: dict[str, Any], json_name: str, csv_name: str) -> str:
    config = payload["config"]
    target = payload["target"]
    recommendations = payload["recommendations"]
    best = recommendations.get("best_by_throughput")
    latency_best = recommendations.get("best_by_wall_p95")
    lines = [
        "# REST shard latency benchmark",
        "",
        f"- created_at: {payload['created_at']}",
        f"- json: `{json_name}`",
        f"- csv: `{csv_name}`",
        f"- rest_base: `{config['rest_base']}`",
        f"- interval: `{config['interval']}`",
        f"- selected_symbols: {payload['selected_symbol_count']}",
        f"- shard_size_range: {config['min_shard_size']}..{config['max_shard_size']} step {config['step']}",
        f"- rounds_per_size: {config['rounds']}",
        f"- warmup_rounds_per_size: {config['warmup_rounds']}",
        f"- rest_concurrency: {config['rest_concurrency'] or 'shard_size'}",
        f"- rest_min_interval_ms: {config['rest_min_interval_ms']}",
        f"- parallel_ips_for_estimate: {config['parallel_ips']}",
        *kline_target.markdown_target(target),
        "",
        "## Recommendation",
        "",
    ]
    if best:
        lines.extend(
            [
                (
                    "- best_by_throughput: "
                    f"shard_size={best['shard_size']}, "
                    f"wall_p50_ms={best['wall_p50_ms']:.2f}, "
                    f"wall_p95_ms={best['wall_p95_ms']:.2f}, "
                    f"throughput={best['throughput_symbols_per_second']:.2f} symbols/s, "
                    f"estimated_full_parallel_p95_ms={best['estimated_full_parallel_p95_ms']:.2f}"
                ),
            ],
        )
    else:
        lines.append(f"- no eligible shard size: {recommendations.get('reason', '')}")
    if latency_best:
        lines.append(
            "- best_by_wall_p95: "
            f"shard_size={latency_best['shard_size']}, "
            f"wall_p50_ms={latency_best['wall_p50_ms']:.2f}, "
            f"wall_p95_ms={latency_best['wall_p95_ms']:.2f}, "
            f"throughput={latency_best['throughput_symbols_per_second']:.2f} symbols/s"
        )
    lines.extend(
        [
            "",
            "## Summary",
            "",
            "| shard_size | rounds | success_ratio | rate_limited | wall_p50_ms | wall_p95_ms | wall_max_ms | req_p50_ms | req_p95_ms | throughput_sym_s | p50_ms_per_symbol | est_shards | est_full_parallel_p95_ms |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ],
    )
    for item in payload["summary"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    md_cell(item["shard_size"]),
                    md_cell(item["rounds"]),
                    md_cell(f"{item['success_ratio']:.4f}"),
                    md_cell(item["rate_limited"]),
                    md_cell(f"{item['wall_ms']['p50']:.2f}"),
                    md_cell(f"{item['wall_ms']['p95']:.2f}"),
                    md_cell(f"{item['wall_ms']['max']:.2f}"),
                    md_cell(f"{item['per_request_ms']['p50']:.2f}"),
                    md_cell(f"{item['per_request_ms']['p95']:.2f}"),
                    md_cell(f"{item['throughput_symbols_per_second']:.2f}"),
                    md_cell(f"{item['p50_wall_ms_per_symbol']:.2f}"),
                    md_cell(item["estimated_shards_for_selected_symbols"]),
                    md_cell(f"{item['estimated_full_parallel_p95_ms']:.2f}"),
                ],
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any], output_dir: str) -> tuple[Path, Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = out_dir / f"rest_shard_latency_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    csv_path = base.with_suffix(".csv")

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(payload, json_path.name, csv_path.name), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "shard_size",
                "rounds",
                "success_ratio",
                "successes",
                "failures",
                "rate_limited",
                "wall_min_ms",
                "wall_mean_ms",
                "wall_p50_ms",
                "wall_p90_ms",
                "wall_p95_ms",
                "wall_p99_ms",
                "wall_max_ms",
                "request_p50_ms",
                "request_p95_ms",
                "throughput_symbols_per_second",
                "p50_wall_ms_per_symbol",
                "estimated_shards_for_selected_symbols",
                "estimated_full_serial_p95_ms",
                "estimated_full_parallel_ips",
                "estimated_full_parallel_p95_ms",
            ],
        )
        writer.writeheader()
        for item in payload["summary"]:
            writer.writerow(
                {
                    "shard_size": item["shard_size"],
                    "rounds": item["rounds"],
                    "success_ratio": item["success_ratio"],
                    "successes": item["successes"],
                    "failures": item["failures"],
                    "rate_limited": item["rate_limited"],
                    "wall_min_ms": item["wall_ms"]["min"],
                    "wall_mean_ms": item["wall_ms"]["mean"],
                    "wall_p50_ms": item["wall_ms"]["p50"],
                    "wall_p90_ms": item["wall_ms"]["p90"],
                    "wall_p95_ms": item["wall_ms"]["p95"],
                    "wall_p99_ms": item["wall_ms"]["p99"],
                    "wall_max_ms": item["wall_ms"]["max"],
                    "request_p50_ms": item["per_request_ms"]["p50"],
                    "request_p95_ms": item["per_request_ms"]["p95"],
                    "throughput_symbols_per_second": item["throughput_symbols_per_second"],
                    "p50_wall_ms_per_symbol": item["p50_wall_ms_per_symbol"],
                    "estimated_shards_for_selected_symbols": item["estimated_shards_for_selected_symbols"],
                    "estimated_full_serial_p95_ms": item["estimated_full_serial_p95_ms"],
                    "estimated_full_parallel_ips": item["estimated_full_parallel_ips"],
                    "estimated_full_parallel_p95_ms": item["estimated_full_parallel_p95_ms"],
                },
            )
    return json_path, md_path, csv_path


def read_symbols_file(path: str) -> list[str]:
    raw = Path(path).read_text(encoding="utf-8")
    symbols: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.extend(parse_csv(line.replace(" ", ",")))
    return sorted(set(symbols))


async def resolve_symbols(args: argparse.Namespace, proxy: str | None) -> list[str]:
    if args.symbols_file:
        symbols = read_symbols_file(args.symbols_file)
    elif args.symbols.strip().lower() == "all":
        symbols = await fetch_symbols(args.rest_base, proxy, args.rest_timeout_seconds)
    elif args.symbols.strip().lower() == "majors":
        symbols = parse_csv(args.major_symbols)
    else:
        symbols = parse_csv(args.symbols)

    symbols = sorted(set(symbols))
    if args.symbol_order == "shuffle":
        random.Random(args.seed).shuffle(symbols)
    if args.symbol_limit > 0:
        symbols = symbols[: args.symbol_limit]
    return symbols


async def main_async() -> None:
    args = parse_args()
    proxy = args.rest_proxy or proxy_from_env(args.rest_base)
    symbols = await resolve_symbols(args, proxy)
    if not symbols:
        raise SystemExit("No symbols selected")
    max_shard_size = min(args.max_shard_size, len(symbols))
    if args.min_shard_size > max_shard_size:
        raise SystemExit(f"min_shard_size={args.min_shard_size} is larger than selected symbols={len(symbols)}")

    server_time = None
    end_time_ms = None
    if args.use_end_time:
        if args.end_time_ms > 0:
            end_time_ms = args.end_time_ms
        else:
            server_time = await fetch_server_time(args.rest_base, proxy, args.rest_timeout_seconds)
            server_time_ms = int(server_time.get("server_time_ms") or server_time.get("fallback_local_ms") or now_ms())
            if kline_target.uses_symbol_close(args):
                end_time_ms = server_time_ms - kline_target.THREE_DAYS_MS
            else:
                end_time_ms = latest_closed_kline_close_ms(server_time_ms, args.interval)

    pacer = RequestPacer(args.rest_min_interval_ms)
    rounds: list[dict[str, Any]] = []
    sizes = list(range(args.min_shard_size, max_shard_size + 1, args.step))
    timeout = httpx.Timeout(args.rest_timeout_seconds)

    target = kline_target.describe_target(end_time_ms, kline_target.uses_symbol_close(args))
    print(
        json.dumps(
            {
                "event": "rest_shard_benchmark_start",
                "symbols": len(symbols),
                "sizes": sizes,
                "rounds": args.rounds,
                "warmup_rounds": args.warmup_rounds,
                "interval": args.interval,
                **target,
                "rest_concurrency": args.rest_concurrency or "shard_size",
                "rest_min_interval_ms": args.rest_min_interval_ms,
                "proxy": bool(proxy),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    shared_client: httpx.AsyncClient | None = None
    try:
        if not args.new_client_per_round:
            shared_client = httpx.AsyncClient(timeout=timeout, proxy=proxy)

        for shard_size in sizes:
            for warmup_index in range(1, args.warmup_rounds + 1):
                symbols_for_round = select_round_symbols(args, symbols, shard_size, warmup_index)
                if args.new_client_per_round:
                    round_item = await run_shard_round_with_client(
                        pacer,
                        args,
                        proxy,
                        shard_size,
                        warmup_index,
                        False,
                        symbols_for_round,
                        end_time_ms,
                    )
                else:
                    assert shared_client is not None
                    round_item = await run_shard_round(
                        shared_client,
                        pacer,
                        args,
                        shard_size,
                        warmup_index,
                        False,
                        symbols_for_round,
                        end_time_ms,
                    )
                rounds.append(round_item)
                print(json.dumps(compact_round_for_stdout(round_item), ensure_ascii=False), flush=True)

            for round_index in range(1, args.rounds + 1):
                symbols_for_round = select_round_symbols(args, symbols, shard_size, round_index)
                if args.new_client_per_round:
                    round_item = await run_shard_round_with_client(
                        pacer,
                        args,
                        proxy,
                        shard_size,
                        round_index,
                        True,
                        symbols_for_round,
                        end_time_ms,
                    )
                else:
                    assert shared_client is not None
                    round_item = await run_shard_round(
                        shared_client,
                        pacer,
                        args,
                        shard_size,
                        round_index,
                        True,
                        symbols_for_round,
                        end_time_ms,
                    )
                rounds.append(round_item)
                print(json.dumps(compact_round_for_stdout(round_item), ensure_ascii=False), flush=True)
    finally:
        if shared_client is not None:
            await shared_client.aclose()

    summary = summarize_rounds(rounds, len(symbols), args.parallel_ips)
    recommendations = choose_recommendations(args, summary)
    payload = {
        "event": "rest_shard_latency_benchmark",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "rest_base": args.rest_base,
            "interval": args.interval,
            "symbols": args.symbols,
            "symbols_file": args.symbols_file,
            "symbol_limit": args.symbol_limit,
            "symbol_order": args.symbol_order,
            "sample_mode": args.sample_mode,
            "seed": args.seed,
            "min_shard_size": args.min_shard_size,
            "max_shard_size": max_shard_size,
            "step": args.step,
            "rounds": args.rounds,
            "warmup_rounds": args.warmup_rounds,
            "kline_limit": args.kline_limit,
            "use_end_time": args.use_end_time,
            "require_exact_close": args.require_exact_close,
            "rest_timeout_seconds": args.rest_timeout_seconds,
            "rest_concurrency": args.rest_concurrency,
            "rest_min_interval_ms": args.rest_min_interval_ms,
            "new_client_per_round": args.new_client_per_round,
            "parallel_ips": args.parallel_ips,
            "target_latency_ms": args.target_latency_ms,
            "min_success_ratio": args.min_success_ratio,
            "no_details": args.no_details,
        },
        "server_time": server_time,
        "target": target,
        "selected_symbol_count": len(symbols),
        "selected_symbols": symbols,
        "recommendations": recommendations,
        "summary": summary,
        "rounds": rounds,
    }
    json_path, md_path, csv_path = write_outputs(payload, args.output_dir)
    print(
        json.dumps(
            {
                "event": "rest_shard_benchmark_written",
                "json_path": str(json_path),
                "markdown_path": str(md_path),
                "csv_path": str(csv_path),
                "recommendations": recommendations,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def compact_round_for_stdout(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "event": item["event"],
        "shard_size": item["shard_size"],
        "round_index": item["round_index"],
        "measured": item["measured"],
        "symbols": len(item["symbols"]),
        "wall_ms": item["wall_ms"],
        "request_span_ms": item["request_span_ms"],
        "request_p50_ms": item["per_request_ms"]["p50"],
        "request_p95_ms": item["per_request_ms"]["p95"],
        "successes": item["successes"],
        "failures": item["failures"],
        "rate_limited": item["rate_limited"],
        "used_weight_1m_max": item["used_weight_1m_max"],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark one Binance REST kline shard by increasing symbol count. "
            "Each shard size N sends N single-symbol REST requests and records wall latency."
        ),
    )
    parser.add_argument("--rest-base", default="https://fapi.binance.com")
    parser.add_argument("--rest-proxy", default="", help="Override REST proxy. Defaults to BINANCE_HTTP_PROXY/HTTPS_PROXY/HTTP_PROXY.")
    parser.add_argument("--interval", default="1h", help="Kline interval used for /fapi/v1/klines.")
    parser.add_argument("--symbols", default="all", help="'all', 'majors', or comma separated symbols.")
    parser.add_argument("--symbols-file", default="", help="Optional file with symbols, one or many comma-separated symbols per line.")
    parser.add_argument("--major-symbols", default=DEFAULT_MAJOR_SYMBOLS)
    parser.add_argument("--symbol-limit", type=int, default=0, help="Limit selected symbol universe after sorting/shuffling. 0 means no limit.")
    parser.add_argument("--symbol-order", choices=["sorted", "shuffle"], default="sorted")
    parser.add_argument("--sample-mode", choices=["rolling", "prefix", "shuffle"], default="rolling")
    parser.add_argument("--symbol-offset", type=int, default=0)
    parser.add_argument("--seed", type=int, default=20260520)
    parser.add_argument("--min-shard-size", type=int, default=1)
    parser.add_argument("--max-shard-size", type=int, default=50)
    parser.add_argument("--step", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=1, help="Measured rounds per shard size.")
    parser.add_argument("--warmup-rounds", type=int, default=0, help="Unmeasured warmup rounds per shard size.")
    parser.add_argument("--kline-limit", type=int, default=3)
    parser.add_argument("--use-end-time", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--end-time-ms", type=int, default=0, help="Explicit kline close_time_ms for endTime. 0 auto-selects latest closed kline.")
    parser.add_argument("--require-exact-close", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--rest-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--rest-concurrency", type=int, default=0, help="Requests in flight per shard. 0 means shard_size.")
    parser.add_argument("--rest-min-interval-ms", type=int, default=50, help="Minimum gap between REST request starts on this process/IP.")
    parser.add_argument("--new-client-per-round", action="store_true", help="Disable connection reuse between shard rounds.")
    parser.add_argument("--parallel-ips", type=int, default=1, help="Only used for full-market latency estimates.")
    parser.add_argument("--target-latency-ms", type=float, default=0.0, help="Optional p95 wall latency cap for recommendations.")
    parser.add_argument("--min-success-ratio", type=float, default=1.0)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--no-details", action="store_true", help="Do not write per-symbol request details into JSON.")
    args = parser.parse_args()

    if args.min_shard_size < 1:
        parser.error("--min-shard-size must be >= 1")
    if args.max_shard_size < args.min_shard_size:
        parser.error("--max-shard-size must be >= --min-shard-size")
    if args.step < 1:
        parser.error("--step must be >= 1")
    if args.rounds < 1:
        parser.error("--rounds must be >= 1")
    if args.warmup_rounds < 0:
        parser.error("--warmup-rounds must be >= 0")
    if args.kline_limit < 1:
        parser.error("--kline-limit must be >= 1")
    if args.rest_timeout_seconds <= 0:
        parser.error("--rest-timeout-seconds must be > 0")
    if args.rest_concurrency < 0:
        parser.error("--rest-concurrency must be >= 0")
    if args.rest_min_interval_ms < 0:
        parser.error("--rest-min-interval-ms must be >= 0")
    if args.parallel_ips < 1:
        parser.error("--parallel-ips must be >= 1")
    if not 0 < args.min_success_ratio <= 1:
        parser.error("--min-success-ratio must be in (0, 1]")
    if args.use_end_time and args.end_time_ms <= 0 and args.interval not in INTERVAL_MS:
        parser.error(f"--interval {args.interval!r} cannot auto-compute endTime; pass --no-use-end-time or --end-time-ms")
    return args


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
