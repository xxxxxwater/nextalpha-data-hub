#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import getpass
import json
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import quote, urlparse

import httpx


DEFAULT_MAJOR_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT"
DEFAULT_OUTPUT_DIR = "logs/rest_proxy_shard_latency"
DEFAULT_PROXY_IPS = [
    "13.215.112.90",
    "18.143.179.221",
    "54.179.12.101",
    "54.151.163.139",
    "47.129.249.50",
    "54.179.171.23",
    "54.151.136.195",
    "13.212.142.123",
    "18.140.60.83",
    "54.151.204.105",
    "18.136.206.75",
    "52.221.183.79",
    "18.139.222.133",
    "52.77.249.122",
    "47.129.58.105",
    "18.136.207.61",
    "54.179.205.215",
    "52.77.230.93",
    "54.255.200.136",
    "18.136.202.234",
]
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
    "1w": 7 * 24 * 60_000 * 60,
}


@dataclass(frozen=True)
class ProxyEndpoint:
    ip: str
    url: str


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


def ms_text(value: int | None) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(value / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def parse_csv(raw: str) -> list[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def parse_csv_raw(raw: str) -> list[str]:
    return [item.strip() for item in raw.replace("\n", ",").split(",") if item.strip()]


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


def build_proxy_url(ip: str, username: str, password: str, port: int) -> str:
    user = quote(username, safe="")
    pwd = quote(password, safe="")
    return f"http://{user}:{pwd}@{ip}:{port}"


def build_proxy_endpoints(args: argparse.Namespace, password: str) -> list[ProxyEndpoint]:
    if args.proxy_ips_file:
        ips = parse_csv_raw(Path(args.proxy_ips_file).read_text(encoding="utf-8"))
    else:
        ips = parse_csv_raw(args.proxy_ips)
    endpoints: list[ProxyEndpoint] = []
    seen: set[str] = set()
    for ip in ips:
        if ip in seen:
            continue
        seen.add(ip)
        endpoints.append(ProxyEndpoint(ip=ip, url=build_proxy_url(ip, args.proxy_username, password, args.proxy_port)))
    return endpoints


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
    # Binance weeks open on Monday, four days after the Thursday Unix epoch.
    anchor_ms = 4 * INTERVAL_MS["1d"] if interval == "1w" else 0
    current_open_ms = server_time_ms - (server_time_ms - anchor_ms) % interval_ms
    return current_open_ms - 1


async def probe_proxy_endpoint(
    endpoint: ProxyEndpoint,
    target_url: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    start_ms = now_ms()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds), proxy=endpoint.url) as client:
            resp = await client.get(target_url)
            end_ms = now_ms()
            origin = ""
            try:
                body = resp.json()
                origin = str(body.get("origin") or body.get("ip") or "")
            except Exception:
                origin = resp.text[:200]
            return {
                "proxy_ip": endpoint.ip,
                "ok": 200 <= resp.status_code < 300,
                "status_code": resp.status_code,
                "elapsed_ms": end_ms - start_ms,
                "origin": origin,
                "error": "",
            }
    except Exception as exc:
        end_ms = now_ms()
        return {
            "proxy_ip": endpoint.ip,
            "ok": False,
            "status_code": None,
            "elapsed_ms": end_ms - start_ms,
            "origin": "",
            "error": f"{type(exc).__name__}: {exc}",
        }


async def probe_proxy_endpoints(
    endpoints: list[ProxyEndpoint],
    target_url: str,
    timeout_seconds: float,
    parallel: int,
) -> tuple[list[ProxyEndpoint], list[dict[str, Any]]]:
    sem = asyncio.Semaphore(max(1, min(parallel, len(endpoints))))

    async def one(endpoint: ProxyEndpoint) -> dict[str, Any]:
        async with sem:
            result = await probe_proxy_endpoint(endpoint, target_url, timeout_seconds)
            print(json.dumps({"event": "proxy_probe", **result}, ensure_ascii=False), flush=True)
            return result

    results = await asyncio.gather(*(one(endpoint) for endpoint in endpoints))
    ok_ips = {item["proxy_ip"] for item in results if item.get("ok")}
    return [endpoint for endpoint in endpoints if endpoint.ip in ok_ips], results


async def fetch_rest_kline(
    client: httpx.AsyncClient,
    pacer: RequestPacer,
    rest_base: str,
    symbol: str,
    interval: str,
    limit: int,
    end_time_ms: int | None,
    require_exact_close: bool,
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
        status = resp.status_code
        used_weight_1m = resp.headers.get("x-mbx-used-weight-1m")
        result: dict[str, Any] = {
            "symbol": symbol,
            "status": status,
            "http_ok": 200 <= status < 300,
            "data_ok": False,
            "ok": False,
            "rate_limited": status in (418, 429),
            "used_weight_1m": int(used_weight_1m) if used_weight_1m and used_weight_1m.isdigit() else None,
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
        matched_close_ms: int | None = None
        if isinstance(rows, list):
            if end_time_ms is not None:
                for row in reversed(rows):
                    if isinstance(row, list) and len(row) >= 8 and int(row[6]) == end_time_ms:
                        matched_close_ms = int(row[6])
                        break
            elif rows and isinstance(rows[-1], list) and len(rows[-1]) >= 8:
                matched_close_ms = int(rows[-1][6])
        result["matched_close_time_ms"] = matched_close_ms
        result["data_ok"] = matched_close_ms is not None
        if require_exact_close and end_time_ms is not None and matched_close_ms != end_time_ms:
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


def assign_symbols(
    symbols: list[str],
    endpoints: list[ProxyEndpoint],
    symbols_per_proxy: int,
    round_index: int,
    seed: int,
    assignment: str,
) -> list[dict[str, Any]]:
    ordered = list(symbols)
    random.Random(seed + round_index * 1009).shuffle(ordered)
    if assignment == "chunks":
        shards = []
        pos = 0
        for endpoint in endpoints:
            chunk = ordered[pos : pos + symbols_per_proxy]
            pos += symbols_per_proxy
            shards.append({"proxy": endpoint, "symbols": chunk})
        if pos < len(ordered):
            raise ValueError(
                f"Not enough proxy capacity: symbols={len(ordered)} capacity={len(endpoints) * symbols_per_proxy}",
            )
        return [item for item in shards if item["symbols"]]

    shards = [{"proxy": endpoint, "symbols": []} for endpoint in endpoints]
    for idx, symbol in enumerate(ordered):
        shards[idx % len(shards)]["symbols"].append(symbol)
    oversized = [item for item in shards if len(item["symbols"]) > symbols_per_proxy]
    if oversized:
        raise ValueError(
            f"Not enough proxy capacity: symbols={len(ordered)} capacity={len(endpoints) * symbols_per_proxy}",
        )
    return [item for item in shards if item["symbols"]]


async def run_proxy_shard(
    args: argparse.Namespace,
    endpoint: ProxyEndpoint,
    symbols: list[str],
    end_time_ms: int | None,
) -> dict[str, Any]:
    shard_start_ms = now_ms()
    concurrency = args.per_proxy_concurrency if args.per_proxy_concurrency > 0 else len(symbols)
    sem = asyncio.Semaphore(max(1, min(concurrency, len(symbols))))
    pacer = RequestPacer(args.per_proxy_min_interval_ms)

    async def one(client: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
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
            )

    timeout = httpx.Timeout(args.rest_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout, proxy=endpoint.url) as client:
        results = await asyncio.gather(*(one(client, symbol) for symbol in symbols))
    shard_end_ms = now_ms()
    successes = sum(1 for item in results if item.get("ok"))
    failures = len(results) - successes
    rate_limited = sum(1 for item in results if item.get("rate_limited"))
    request_costs = [float(item.get("request_cost_ms") or 0) for item in results]
    return {
        "proxy_ip": endpoint.ip,
        "symbols": symbols,
        "symbol_count": len(symbols),
        "shard_start_ms": shard_start_ms,
        "shard_end_ms": shard_end_ms,
        "shard_wall_ms": shard_end_ms - shard_start_ms,
        "successes": successes,
        "failures": failures,
        "rate_limited": rate_limited,
        "per_request_ms": summarize(request_costs),
        "results": [] if args.no_details else results,
    }


async def run_round(
    args: argparse.Namespace,
    endpoints: list[ProxyEndpoint],
    symbols: list[str],
    round_index: int,
    end_time_ms: int | None,
) -> dict[str, Any]:
    shards = assign_symbols(
        symbols,
        endpoints,
        args.symbols_per_proxy,
        round_index,
        args.seed,
        args.assignment,
    )
    round_start_ms = now_ms()
    shard_results = await asyncio.gather(
        *(run_proxy_shard(args, item["proxy"], item["symbols"], end_time_ms) for item in shards),
    )
    round_end_ms = now_ms()
    successes = sum(int(item["successes"]) for item in shard_results)
    failures = sum(int(item["failures"]) for item in shard_results)
    rate_limited = sum(int(item["rate_limited"]) for item in shard_results)
    shard_wall_values = [float(item["shard_wall_ms"]) for item in shard_results]
    request_values: list[float] = []
    for item in shard_results:
        if item.get("results"):
            request_values.extend(float(result.get("request_cost_ms") or 0) for result in item["results"])
    return {
        "event": "rest_proxy_shard_round",
        "round_index": round_index,
        "round_start_ms": round_start_ms,
        "round_end_ms": round_end_ms,
        "all_done_ms": round_end_ms - round_start_ms,
        "proxy_count_used": len(shard_results),
        "symbol_count": len(symbols),
        "symbols_per_proxy_cap": args.symbols_per_proxy,
        "successes": successes,
        "failures": failures,
        "success_ratio": successes / (successes + failures) if successes + failures else 0.0,
        "rate_limited": rate_limited,
        "shard_wall_ms": summarize(shard_wall_values),
        "per_request_ms": summarize(request_values),
        "shards": shard_results,
    }


def compact_round_for_stdout(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "event": item["event"],
        "round_index": item["round_index"],
        "all_done_ms": item["all_done_ms"],
        "proxy_count_used": item["proxy_count_used"],
        "symbol_count": item["symbol_count"],
        "successes": item["successes"],
        "failures": item["failures"],
        "success_ratio": item["success_ratio"],
        "rate_limited": item["rate_limited"],
        "shard_wall_p95_ms": item["shard_wall_ms"]["p95"],
        "request_p95_ms": item["per_request_ms"]["p95"],
    }


def render_markdown(payload: dict[str, Any], json_name: str, csv_name: str) -> str:
    summary = payload["summary"]
    config = payload["config"]
    lines = [
        "# REST proxy shard latency benchmark",
        "",
        f"- created_at: {payload['created_at']}",
        f"- json: `{json_name}`",
        f"- csv: `{csv_name}`",
        f"- rest_base: `{config['rest_base']}`",
        f"- interval: `{config['interval']}`",
        f"- selected_symbols: {payload['selected_symbol_count']}",
        f"- proxy_count: {len(config['healthy_proxy_ips'])}",
        f"- symbols_per_proxy_cap: {config['symbols_per_proxy']}",
        f"- assignment: `{config['assignment']}`",
        f"- rounds: {config['rounds']}",
        f"- per_proxy_concurrency: {config['per_proxy_concurrency'] or 'symbol_count'}",
        f"- per_proxy_min_interval_ms: {config['per_proxy_min_interval_ms']}",
        f"- target_close_time_ms: {payload['target']['end_time_ms']}",
        f"- target_close_time: {payload['target']['end_time']}",
        "",
        "## Summary",
        "",
        f"- all_done_ms p50: {summary['all_done_ms']['p50']:.2f}",
        f"- all_done_ms p95: {summary['all_done_ms']['p95']:.2f}",
        f"- all_done_ms max: {summary['all_done_ms']['max']:.2f}",
        f"- success_ratio: {summary['success_ratio']:.4f}",
        f"- failures: {summary['failures']}",
        f"- rate_limited: {summary['rate_limited']}",
        "",
        "## Rounds",
        "",
        "| round | all_done_ms | proxy_count | symbols | success_ratio | failures | rate_limited | shard_p50_ms | shard_p95_ms | request_p50_ms | request_p95_ms |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in payload["rounds"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item["round_index"]),
                    f"{item['all_done_ms']:.2f}",
                    str(item["proxy_count_used"]),
                    str(item["symbol_count"]),
                    f"{item['success_ratio']:.4f}",
                    str(item["failures"]),
                    str(item["rate_limited"]),
                    f"{item['shard_wall_ms']['p50']:.2f}",
                    f"{item['shard_wall_ms']['p95']:.2f}",
                    f"{item['per_request_ms']['p50']:.2f}",
                    f"{item['per_request_ms']['p95']:.2f}",
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
    base = out_dir / f"rest_proxy_shards_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    csv_path = base.with_suffix(".csv")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(payload, json_path.name, csv_path.name), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "round_index",
                "all_done_ms",
                "proxy_count_used",
                "symbol_count",
                "success_ratio",
                "successes",
                "failures",
                "rate_limited",
                "shard_p50_ms",
                "shard_p95_ms",
                "request_p50_ms",
                "request_p95_ms",
            ],
        )
        writer.writeheader()
        for item in payload["rounds"]:
            writer.writerow(
                {
                    "round_index": item["round_index"],
                    "all_done_ms": item["all_done_ms"],
                    "proxy_count_used": item["proxy_count_used"],
                    "symbol_count": item["symbol_count"],
                    "success_ratio": item["success_ratio"],
                    "successes": item["successes"],
                    "failures": item["failures"],
                    "rate_limited": item["rate_limited"],
                    "shard_p50_ms": item["shard_wall_ms"]["p50"],
                    "shard_p95_ms": item["shard_wall_ms"]["p95"],
                    "request_p50_ms": item["per_request_ms"]["p50"],
                    "request_p95_ms": item["per_request_ms"]["p95"],
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


async def resolve_symbols(args: argparse.Namespace, metadata_proxy: str | None) -> list[str]:
    if args.symbols_file:
        symbols = read_symbols_file(args.symbols_file)
    elif args.symbols.strip().lower() == "all":
        symbols = await fetch_symbols(args.rest_base, metadata_proxy, args.rest_timeout_seconds)
    elif args.symbols.strip().lower() == "majors":
        symbols = parse_csv(args.major_symbols)
    else:
        symbols = parse_csv(args.symbols)
    symbols = sorted(set(symbols))
    if args.symbol_limit > 0:
        symbols = symbols[: args.symbol_limit]
    return symbols


async def main_async() -> None:
    args = parse_args()
    password = args.proxy_password
    if password is None:
        password = getpass.getpass("Proxy password: ")
    endpoints = build_proxy_endpoints(args, password)
    if not endpoints:
        raise SystemExit("No proxy endpoints configured")

    probe_results: list[dict[str, Any]] = []
    healthy_endpoints = endpoints
    if args.probe_proxies:
        healthy_endpoints, probe_results = await probe_proxy_endpoints(
            endpoints,
            args.proxy_probe_url,
            args.proxy_probe_timeout_seconds,
            args.proxy_probe_parallel,
        )
    if len(healthy_endpoints) < args.required_proxy_count:
        raise SystemExit(
            f"Healthy proxy count {len(healthy_endpoints)} < required_proxy_count {args.required_proxy_count}",
        )

    metadata_proxy = healthy_endpoints[0].url if args.metadata_via_proxy else proxy_from_env(args.rest_base)
    symbols = await resolve_symbols(args, metadata_proxy)
    if not symbols:
        raise SystemExit("No symbols selected")
    if len(symbols) > len(healthy_endpoints) * args.symbols_per_proxy:
        raise SystemExit(
            f"Not enough proxy capacity: symbols={len(symbols)} capacity={len(healthy_endpoints) * args.symbols_per_proxy}",
        )

    server_time = None
    end_time_ms = None
    if args.use_end_time:
        if args.end_time_ms > 0:
            end_time_ms = args.end_time_ms
        else:
            server_time = await fetch_server_time(args.rest_base, metadata_proxy, args.rest_timeout_seconds)
            server_time_ms = int(server_time.get("server_time_ms") or server_time.get("fallback_local_ms") or now_ms())
            end_time_ms = latest_closed_kline_close_ms(server_time_ms, args.interval)

    print(
        json.dumps(
            {
                "event": "rest_proxy_shard_benchmark_start",
                "rounds": args.rounds,
                "symbols": len(symbols),
                "proxy_count": len(healthy_endpoints),
                "symbols_per_proxy": args.symbols_per_proxy,
                "assignment": args.assignment,
                "interval": args.interval,
                "end_time_ms": end_time_ms,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    rounds: list[dict[str, Any]] = []
    for round_index in range(1, args.rounds + 1):
        item = await run_round(args, healthy_endpoints, symbols, round_index, end_time_ms)
        rounds.append(item)
        print(json.dumps(compact_round_for_stdout(item), ensure_ascii=False), flush=True)

    all_done_values = [float(item["all_done_ms"]) for item in rounds]
    total_successes = sum(int(item["successes"]) for item in rounds)
    total_failures = sum(int(item["failures"]) for item in rounds)
    summary = {
        "all_done_ms": summarize(all_done_values),
        "successes": total_successes,
        "failures": total_failures,
        "success_ratio": total_successes / (total_successes + total_failures) if total_successes + total_failures else 0.0,
        "rate_limited": sum(int(item["rate_limited"]) for item in rounds),
    }
    payload = {
        "event": "rest_proxy_shard_latency_benchmark",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "rest_base": args.rest_base,
            "interval": args.interval,
            "symbols": args.symbols,
            "symbols_file": args.symbols_file,
            "symbol_limit": args.symbol_limit,
            "rounds": args.rounds,
            "symbols_per_proxy": args.symbols_per_proxy,
            "assignment": args.assignment,
            "per_proxy_concurrency": args.per_proxy_concurrency,
            "per_proxy_min_interval_ms": args.per_proxy_min_interval_ms,
            "rest_timeout_seconds": args.rest_timeout_seconds,
            "proxy_port": args.proxy_port,
            "proxy_username": args.proxy_username,
            "configured_proxy_ips": [endpoint.ip for endpoint in endpoints],
            "healthy_proxy_ips": [endpoint.ip for endpoint in healthy_endpoints],
            "probe_proxies": args.probe_proxies,
            "metadata_via_proxy": args.metadata_via_proxy,
        },
        "proxy_probe_results": probe_results,
        "server_time": server_time,
        "target": {
            "end_time_ms": end_time_ms,
            "end_time": ms_text(end_time_ms),
        },
        "selected_symbol_count": len(symbols),
        "selected_symbols": symbols,
        "summary": summary,
        "rounds": rounds,
    }
    json_path, md_path, csv_path = write_outputs(payload, args.output_dir)
    print(
        json.dumps(
            {
                "event": "rest_proxy_shard_benchmark_written",
                "json_path": str(json_path),
                "markdown_path": str(md_path),
                "csv_path": str(csv_path),
                "summary": summary,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark all-market Binance REST latency using fixed proxy shards.",
    )
    parser.add_argument("--rest-base", default="https://fapi.binance.com")
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--symbols", default="all", help="'all', 'majors', or comma separated symbols.")
    parser.add_argument("--symbols-file", default="")
    parser.add_argument("--major-symbols", default=DEFAULT_MAJOR_SYMBOLS)
    parser.add_argument("--symbol-limit", type=int, default=0)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--symbols-per-proxy", type=int, default=30)
    parser.add_argument("--assignment", choices=["balanced", "chunks"], default="balanced")
    parser.add_argument("--per-proxy-concurrency", type=int, default=0, help="0 means all symbols in that proxy shard.")
    parser.add_argument("--per-proxy-min-interval-ms", type=int, default=50)
    parser.add_argument("--kline-limit", type=int, default=3)
    parser.add_argument("--use-end-time", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--end-time-ms", type=int, default=0)
    parser.add_argument("--require-exact-close", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--rest-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--proxy-ips", default=",".join(DEFAULT_PROXY_IPS))
    parser.add_argument("--proxy-ips-file", default="")
    parser.add_argument("--proxy-port", type=int, default=3128)
    parser.add_argument("--proxy-username", default="match_user")
    parser.add_argument("--proxy-password", default=None, help="Omit to input securely at startup.")
    parser.add_argument("--probe-proxies", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--proxy-probe-url", default="https://httpbin.org/ip")
    parser.add_argument("--proxy-probe-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--proxy-probe-parallel", type=int, default=20)
    parser.add_argument("--required-proxy-count", type=int, default=20)
    parser.add_argument("--metadata-via-proxy", action="store_true")
    parser.add_argument("--seed", type=int, default=20260520)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--no-details", action="store_true")
    args = parser.parse_args()

    if args.rounds < 1:
        parser.error("--rounds must be >= 1")
    if args.symbols_per_proxy < 1:
        parser.error("--symbols-per-proxy must be >= 1")
    if args.per_proxy_concurrency < 0:
        parser.error("--per-proxy-concurrency must be >= 0")
    if args.per_proxy_min_interval_ms < 0:
        parser.error("--per-proxy-min-interval-ms must be >= 0")
    if args.rest_timeout_seconds <= 0:
        parser.error("--rest-timeout-seconds must be > 0")
    if args.proxy_probe_timeout_seconds <= 0:
        parser.error("--proxy-probe-timeout-seconds must be > 0")
    if args.proxy_probe_parallel < 1:
        parser.error("--proxy-probe-parallel must be >= 1")
    if args.required_proxy_count < 1:
        parser.error("--required-proxy-count must be >= 1")
    if args.use_end_time and args.end_time_ms <= 0 and args.interval not in INTERVAL_MS:
        parser.error(f"--interval {args.interval!r} cannot auto-compute endTime; pass --no-use-end-time or --end-time-ms")
    return args


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
