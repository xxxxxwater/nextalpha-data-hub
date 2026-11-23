#!/usr/bin/env python3
"""
Print Binance USD-M futures combined-stream WebSocket URLs used for klines,
matching nextalpha_market_watcher.binance_client batching rules.

Example:
  python scripts/print_kline_ws_shard_urls.py --limit 528
"""
from __future__ import annotations

import argparse

import httpx


def fetch_usdt_perp_trading_symbols(rest_base: str) -> list[str]:
    url = f"{rest_base.rstrip('/')}/fapi/v1/exchangeInfo"
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(url)
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
        sym = item.get("symbol")
        if sym:
            symbols.append(sym)
    return sorted(set(symbols))


def build_symbol_batches(symbols: list[str], interval: str, batch_size: int) -> list[list[str]]:
    batches: list[list[str]] = []
    current: list[str] = []
    for symbol in symbols:
        hit_size_limit = len(current) >= max(1, batch_size)
        if current and hit_size_limit:
            batches.append(current)
            current = [symbol]
        else:
            current.append(symbol)
    if current:
        batches.append(current)
    return batches


def stream_base_from_ws_base(ws_base: str) -> str:
    ws_base = ws_base.split("?", 1)[0].rstrip("/")
    if ws_base.endswith("/stream"):
        return ws_base
    if ws_base.endswith("/ws"):
        return f"{ws_base[:-3]}/stream"
    return f"{ws_base}/stream"


def combined_stream_url(stream_base: str, symbols: list[str], interval: str) -> str:
    stream_names = [f"{s.lower()}@kline_{interval}" for s in symbols]
    return f"{stream_base}?streams={'/'.join(stream_names)}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Print Binance kline combined WS URLs per shard.")
    ap.add_argument(
        "--ws-base",
        default="wss://fstream.binance.com/market/stream",
        help="Same as config binance.ws_base (prod default)",
    )
    ap.add_argument(
        "--rest-base",
        default="https://fapi.binance.com",
        help="Same as config binance.rest_base",
    )
    ap.add_argument("--interval", default="1h", help="Kline interval, e.g. 1h")
    ap.add_argument("--batch-size", type=int, default=200, help="Symbols per WS connection")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "After sorting, keep only the first N symbols (lexicographic). "
            "Use only if you intentionally want a prefix slice; default is full exchange list."
        ),
    )
    ap.add_argument(
        "-o",
        "--output",
        default=None,
        help="Write full shard URLs to this file (avoids terminal truncation).",
    )
    args = ap.parse_args()

    symbols = fetch_usdt_perp_trading_symbols(args.rest_base)
    if args.limit is not None:
        symbols = symbols[: max(0, args.limit)]

    batches = build_symbol_batches(symbols, args.interval, args.batch_size)
    stream_base = stream_base_from_ws_base(args.ws_base)

    print(f"rest_base={args.rest_base}")
    print(f"ws_base(config)={args.ws_base}")
    print(f"stream_base(effective)={stream_base}")
    print(f"interval={args.interval!r} batch_size={args.batch_size}")
    print(f"symbol_count={len(symbols)} shard_count={len(batches)}")
    lines: list[str] = []
    for i, batch in enumerate(batches):
        url = combined_stream_url(stream_base, batch, args.interval)
        print()
        print(f"--- shard {i + 1}/{len(batches)} symbols={len(batch)} url_chars={len(url)} ---")
        print(f"first_stream={batch[0].lower()}@kline_{args.interval} last_stream={batch[-1].lower()}@kline_{args.interval}")
        print(url)
        lines.append(f"# shard {i + 1}/{len(batches)} symbols={len(batch)} chars={len(url)}")
        lines.append(url)
        lines.append("")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print()
        print(f"wrote {args.output!r}")


if __name__ == "__main__":
    main()
