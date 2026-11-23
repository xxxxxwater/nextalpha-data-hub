#!/usr/bin/env python3
"""
Ops smoke test: open WebSocket to kline endpoint, optionally wait for first frame.

Usage:
  python tests/ops_wss_kline_probe.py
  python tests/ops_wss_kline_probe.py --url wss://datahub.questvector.ai/ws/klines
  python tests/ops_wss_kline_probe.py --url ws://172.33.178.40:8766/ws/klines
  python tests/ops_wss_kline_probe.py --connect-only
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Probe kline WebSocket (ws/wss) connectivity.")
    p.add_argument(
        "--url",
        default="wss://datahub.questvector.ai/ws/klines",
        help="Full WS or WSS URL including path",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="Seconds to wait for first message after connect (ignored with --connect-only)",
    )
    p.add_argument(
        "--connect-only",
        action="store_true",
        help="Only check TCP/TLS/WS handshake; do not wait for a data frame",
    )
    return p.parse_args()


async def _run() -> int:
    args = _parse_args()
    try:
        import websockets
    except ImportError:
        print("FAIL missing_dependency: pip install websockets", flush=True)
        return 2

    url = args.url
    t0 = time.perf_counter()

    try:
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        ) as ws:
            connect_ms = (time.perf_counter() - t0) * 1000
            print(
                json.dumps(
                    {
                        "status": "connected",
                        "url": url,
                        "connect_ms": round(connect_ms, 2),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

            if args.connect_only:
                print(
                    json.dumps(
                        {
                            "status": "ok",
                            "phase": "connect_only",
                            "url": url,
                            "total_ms": round((time.perf_counter() - t0) * 1000, 2),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                return 0

            raw = await asyncio.wait_for(ws.recv(), timeout=args.timeout)
            total_ms = (time.perf_counter() - t0) * 1000
            preview = raw[:500] if isinstance(raw, str) else raw[:500].decode("utf-8", errors="replace")
            out = {
                "status": "ok",
                "url": url,
                "first_frame_ms": round(total_ms, 2),
                "first_frame_bytes": len(raw) if isinstance(raw, (bytes, bytearray)) else len(raw.encode("utf-8")),
                "first_frame_preview": preview.replace("\n", " ")[:200],
            }
            print(json.dumps(out, ensure_ascii=False), flush=True)
            return 0

    except asyncio.TimeoutError:
        print(
            json.dumps(
                {
                    "status": "fail",
                    "url": url,
                    "error": "timeout_waiting_first_frame",
                    "timeout_sec": args.timeout,
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 1
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "fail",
                    "url": url,
                    "error": str(exc).strip() or repr(exc),
                    "error_type": type(exc).__name__,
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 1


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
