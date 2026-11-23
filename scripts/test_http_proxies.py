#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from urllib.parse import quote

import httpx

try:
    import requests
except ImportError:  # pragma: no cover - optional local dependency
    requests = None


# This diagnostic CLI requires proxy settings supplied by the operator.
__test__ = False


DEFAULT_PROXIES = [
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


@dataclass
class ProbeResult:
    client: str
    proxy_ip: str
    ok: bool
    status_code: int | None
    elapsed_ms: int
    origin: str
    error: str


def build_proxy_url(ip: str, username: str, password: str, port: int) -> str:
    user = quote(username, safe="")
    pwd = quote(password, safe="")
    return f"http://{user}:{pwd}@{ip}:{port}"


def test_with_requests(ip: str, proxy_url: str, target_url: str, timeout_seconds: float) -> ProbeResult:
    start = time.perf_counter()
    if requests is None:
        return ProbeResult("requests", ip, False, None, 0, "", "requests_not_installed")
    try:
        proxies = {"http": proxy_url, "https": proxy_url}
        resp = requests.get(target_url, proxies=proxies, timeout=timeout_seconds)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        origin = ""
        try:
            body = resp.json()
            origin = str(body.get("origin") or body.get("ip") or "")
        except Exception:
            origin = resp.text[:200]
        return ProbeResult("requests", ip, 200 <= resp.status_code < 300, resp.status_code, elapsed_ms, origin, "")
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return ProbeResult("requests", ip, False, None, elapsed_ms, "", f"{type(exc).__name__}: {exc}")


def test_with_httpx(ip: str, proxy_url: str, target_url: str, timeout_seconds: float) -> ProbeResult:
    start = time.perf_counter()
    try:
        with httpx.Client(proxy=proxy_url, timeout=httpx.Timeout(timeout_seconds)) as client:
            resp = client.get(target_url)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        origin = ""
        try:
            body = resp.json()
            origin = str(body.get("origin") or body.get("ip") or "")
        except Exception:
            origin = resp.text[:200]
        return ProbeResult("httpx", ip, 200 <= resp.status_code < 300, resp.status_code, elapsed_ms, origin, "")
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return ProbeResult("httpx", ip, False, None, elapsed_ms, "", f"{type(exc).__name__}: {exc}")


def parse_ips(raw: str) -> list[str]:
    return [item.strip() for item in raw.replace("\n", ",").split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test authenticated HTTP proxy IPs with requests/httpx.")
    parser.add_argument("--ips", default=",".join(DEFAULT_PROXIES), help="Comma separated proxy IPs.")
    parser.add_argument("--port", type=int, default=3128)
    parser.add_argument("--username", default="match_user")
    parser.add_argument("--password", default=None, help="Proxy password. Omit to input securely.")
    parser.add_argument("--target-url", default="https://httpbin.org/ip")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--client", choices=["both", "requests", "httpx"], default="both")
    parser.add_argument("--parallel", type=int, default=10, help="Proxy IPs to test in parallel.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    password = args.password
    if password is None:
        password = getpass.getpass("Proxy password: ")

    ips = parse_ips(args.ips)
    results: list[ProbeResult] = []
    parallel = max(1, min(args.parallel, len(ips)))

    def test_one_ip(ip: str) -> list[ProbeResult]:
        proxy_url = build_proxy_url(ip, args.username, password, args.port)
        ip_results: list[ProbeResult] = []
        if args.client in ("both", "requests"):
            result = test_with_requests(ip, proxy_url, args.target_url, args.timeout_seconds)
            ip_results.append(result)
        if args.client in ("both", "httpx"):
            result = test_with_httpx(ip, proxy_url, args.target_url, args.timeout_seconds)
            ip_results.append(result)
        return ip_results

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {executor.submit(test_one_ip, ip): ip for ip in ips}
        for future in as_completed(futures):
            for result in future.result():
                results.append(result)
                print_result(result, args.port)

    ok_ips = sorted({item.proxy_ip for item in results if item.ok})
    failed_ips = sorted(set(ips) - set(ok_ips))
    print(
        json.dumps(
            {
                "event": "proxy_test_summary",
                "target_url": args.target_url,
                "port": args.port,
                "tested_ips": len(ips),
                "ok_ips": ok_ips,
                "failed_ips": failed_ips,
                "results": [asdict(item) for item in results],
            },
            ensure_ascii=False,
            indent=2,
        ),
    )


def print_result(result: ProbeResult, port: int) -> None:
    status = "OK" if result.ok else "FAIL"
    detail = f"status={result.status_code} origin={result.origin}" if result.ok else result.error
    print(f"[{result.client:<8}] {status:<4} proxy={result.proxy_ip}:{port} elapsed_ms={result.elapsed_ms} {detail}", flush=True)


if __name__ == "__main__":
    main()
