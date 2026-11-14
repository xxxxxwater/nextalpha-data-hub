# Binance USDT-M K线 WebSocket 订阅

订阅 Binance U本位合约 **528** 个交易对的 K 线收盘推送，并在同一根 K 线周期内全部到齐后输出中文汇总日志。默认周期为 **3m**，面向低延迟场景做了接收与处理分离。

数据源：[Binance Futures WebSocket Market Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams)

## 功能

- 同时订阅 528 个 `symbol@kline_{interval}` 流（默认 6 路分片，每路约 88 个 symbol）
- 仅处理 **已收盘** K 线（`k.x == true`）
- 位图统计：同一开盘时间下 528 个 symbol 全部收盘后打印汇总
- 低延迟设计：收包与解析解耦、字节级快速过滤、`orjson` 解析、异步日志队列、关闭 WebSocket 压缩

## 目录结构

```
binance-kline-5m-ws/
├── main.py                 # 主程序
├── parse_streams.py        # 从订阅文件生成 symbol 列表与分片 URL
├── requirements.txt
├── symbols.txt             # 528 个交易对（小写，如 btcusdt）
├── klines_ws_3m_528.txt    # 3m 分片 WebSocket URL（可选，动态分片时非必须）
└── klines_ws_5m_528.txt    # 历史 5m 配置（参考）
```

## 环境要求

- Python 3.9+
- 可访问 `wss://fstream.binance.com`

## 快速开始

```bash
cd binance-kline-5m-ws

python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python main.py
```

启动后示例输出：

```
symbols=528 interval=3m shards=6 shard_size~88 processors=6 log_each=False
[shard 1] connecting...
[shard 1] connected streams=88
...
```

## 汇总日志示例

当一根 K 线周期内 528 个交易对全部收盘后：

```
*** 全部 528 个交易对 3m K线已收盘 ***
  开盘时间: 2026-05-15 08:00:00.000Z
  K线结束: 2026-05-15 08:02:59.999Z
  首条收包: 2026-05-15 08:02:59.512Z symbol=btcusdt shard=1
  末条收包: 2026-05-15 08:03:00.177Z symbol=ethusdt shard=2
  首条处理完成: 2026-05-15 08:02:59.513Z
  最后一条处理完成: 2026-05-15 08:03:00.177Z
  全量汇总完成: 2026-05-15 08:03:00.178Z
  距开盘耗时: 180178 ms
  收盘至首条收包: -487 ms
  收盘至末条收包: +178 ms
  收盘至全量汇总完成: +179 ms
  全市场 Binance WS事件时间分布跨度(max E - min E): 665 ms
  全市场 WS入站分布跨度(first recv -> last recv): 665.421 ms
  全市场 处理完成分布跨度(first parse -> last parse): 664.902 ms
  首个 Binance事件(E) -> 末条收包: 700 ms
  单条 Binance WS事件(E)->本机收包: n=528 本次=1.000ms avg=1.200ms p95=3.000ms min=0.000ms max=8.000ms
  本机收包->解析完成: n=528 本次=0.084ms avg=0.110ms p95=0.210ms min=0.030ms max=1.200ms
  按 Binance WS 分片统计: 第1路：symbols=88 E跨度=660ms 入站跨度=661.012ms；...
  末条收包至汇总耗时: 0.542 ms
```

| 字段 | 含义 |
|------|------|
| 开盘时间 | K 线 `k.t`（周期开始） |
| K线结束 | K 线 `k.T`（周期结束） |
| 首条收包 / 末条收包 | 本机从 WebSocket 读到该批首条/末条 `k.x == true` 的时间 |
| 最后一条处理完成 | 第 528 条收盘消息处理完成时刻 |
| 全量汇总完成 | 汇总逻辑执行完成时刻 |
| 距开盘耗时 | 从开盘到汇总完成的毫秒数 |
| 收盘至首条/末条收包 | `first/last recv_ms - k.T`，用于看收盘后首条、末条到达本机的延迟 |
| 全市场 Binance WS事件时间分布跨度 | `max(E) - min(E)`，对齐主服务 Lark 的 Binance 事件时间跨度 |
| 全市场 WS入站分布跨度 | `last recv_ms - first recv_ms`，对齐主服务 Lark 的入站分布跨度 |
| 首个 Binance事件(E) -> 末条收包 | `last recv_ms - min(E)`，诊断 Binance 事件时间与本机收包的混合口径 |
| 单条 Binance WS事件(E)->本机收包 | 每条 closed kline 的 `recv_ms - E` 统计 |
| 本机收包->解析完成 | 每条 closed kline 从 reader 入队时间到 processor 完成解析的统计 |
| 按 Binance WS 分片统计 | 每条 WS 连接内的 `E跨度` 和 `入站跨度` |
| 末条收包至汇总耗时 | 最后一条消息收包到汇总完成的毫秒数，仅反映本机末段处理延迟，不代表全市场到齐跨度 |

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KLINE_INTERVAL` | `3m` | K 线周期，如 `3m`、`5m`、`1h` |
| `SHARD_SIZE` | `88` | 每个 WebSocket 连接的 symbol 数量 |
| `DYNAMIC_SHARDS` | `true` | 运行时按 `symbols.txt` 动态拼接 URL |
| `PROCESSOR_WORKERS` | `6` | 消息解析 worker 数量 |
| `LOG_EACH_KLINE` | `false` | 是否打印每条收盘 K 线完整 JSON |
| `LOG_PROGRESS_EVERY` | `0` | 进度日志间隔（0=关闭） |
| `BINANCE_WS_PROXY_URL` | 空 | 显式 WebSocket 代理，如 `socks5://127.0.0.1:10808` 或 `http://127.0.0.1:10808` |
| `PING_INTERVAL` | `20` | 客户端主动 ping 间隔秒；设为 `0` 可关闭客户端主动 ping |
| `PING_TIMEOUT` | `60` | 客户端主动 ping 的 pong 等待秒；设为 `0` 可关闭超时 |
| `USE_UVLOOP` | `true` | 尝试使用 uvloop（需 `pip install uvloop`） |

### 常用示例

```bash
# 默认 3m，低延迟（不打印每条 K 线）
python main.py

# 改为 5m
KLINE_INTERVAL=5m python main.py

# 本地代理端口 10808；如果 10808 是 socks5 代理
BINANCE_WS_PROXY_URL=socks5://127.0.0.1:10808 PING_INTERVAL=0 KLINE_INTERVAL=5m python main.py

# 如果 10808 是 HTTP/HTTPS 代理
BINANCE_WS_PROXY_URL=http://127.0.0.1:10808 PING_INTERVAL=0 KLINE_INTERVAL=5m python main.py

# 调试：打印每条收盘 K 线
LOG_EACH_KLINE=true python main.py

# 更小分片，减轻单连接在收盘时的消息突发
SHARD_SIZE=66 PROCESSOR_WORKERS=8 python main.py
```

## 从 1h 订阅文件生成配置

若手头有原始的 `klines_ws_1h_528.txt`（或任意 `@kline_*` 格式文件）：

```bash
python parse_streams.py "/path/to/klines_ws_1h_528(1).txt" \
  --interval 3m \
  --shard-size 88 \
  -o .
```

会生成：

- `symbols.txt`
- `klines_ws_3m_528.txt`

`main.py` 在 `DYNAMIC_SHARDS=true` 时优先用 `symbols.txt` 动态建连，与上述文件内容一致即可。

## 架构简述

```
┌─────────────┐     ┌──────────────┐     ┌────────────────┐     ┌─────────────┐
│ Shard WS x6 │ ──► │  raw_queue   │ ──► │ Processor xN   │ ──► │ tracker_loop│
│ 仅入队+时间戳 │     │              │     │ 过滤+orjson解析 │     │ 位图统计528  │
└─────────────┘     └──────────────┘     └────────────────┘     └──────┬──────┘
                                                                        │
                                                                 ┌──────▼──────┐
                                                                 │ log_writer  │
                                                                 │ 异步写stdout │
                                                                 └─────────────┘
```

1. **接收线程**：WebSocket 循环只做 `put_nowait` 与 `time.time_ns()`，不阻塞。
2. **快速过滤**：未收盘消息通过 `"x":true` 字节匹配跳过 JSON 解析。
3. **单消费者汇总**：`tracker_loop` 用位图 O(1) 更新，避免全局锁竞争。
4. **异步日志**：打印走独立队列，队列满时丢弃日志，不拖慢行情路径。

## 注意事项

- 需稳定网络；断线后会自动重连（指数退避）。
- 528 路流在 K 线收盘瞬间会有突发流量，可通过减小 `SHARD_SIZE` 增加连接数缓解。
- 本工具仅做行情订阅与统计，不含交易、鉴权或持久化存储。
- 项目目录名仍为 `binance-kline-5m-ws`，与默认周期无关；周期由 `KLINE_INTERVAL` 控制。

## License

仅供学习与内部使用；使用 Binance API 请遵守其服务条款。


## Bounded batch-statistics history

`TRACKER_MAX_BATCHES` (default 128, minimum 1) bounds the combined number of
pending and completed candle-open timestamps retained by the bitmap tracker.
It is a capacity window, not a wall-clock timeout, and is independent of interval.
Each pending batch stores at most one sample per configured symbol.

On a newer batch when full, the oldest retained open timestamp is removed from
all tracker containers. An incomplete batch emits a `[batch-expired]` summary
with received and missing counts through the existing non-blocking log queue
(which may drop messages when full). Completed-batch deduplication is bounded by
the same window. Frames at or before the eviction watermark, or older than the
oldest retained timestamp when full, are ignored, never recreated. Within the
window, out-of-order frames can still complete a batch exactly once.

This is in-memory monitoring state, not historical market storage. Restart resets
the window. It does not change subscription/reconnect or raw/tracker queue
capacity; it does not bound the memory of the entire process.
