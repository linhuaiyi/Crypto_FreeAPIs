# Sub-Project Operations Guide

本文档描述两个子项目的运行方式、参数说明和日常运维操作。

---

## 1. Project Structure

```
Crypto_FreeAPIs/
├── fetchers/          # 共享: 各交易所数据采集器
├── storage/           # 共享: ParquetStore, ChunkedBuffer
├── utils/             # 共享: RateLimiter, get_logger, ConfigLoader
├── processors/        # 共享: Greeks, VolSurface, TimeAligner 等
├── pipeline/          # 共享: 策略配置
├── tests/             # 共享测试
├── scripts/
│   ├── pull_data.sh                    # 远程拉取 deribit 数据
│   └── verify_collected_data.py        # 数据质量验证
│
├── deribit-options-data-collector/     # 子项目1: 期权+合约实时采集
│   ├── launch.py                       # 启动入口
│   ├── config_strategy.yaml            # 配置文件
│   ├── data/                           # 本地 parquet 数据
│   └── logs/                           # 运行日志
│
└── ohlcv-collector/                    # 子项目2: OHLCV K线批量采集
    ├── launch.py                       # 启动入口
    ├── config.yaml                     # 配置文件
    └── data/                           # 本地 parquet 数据
```

两个子项目通过 `sys.path` 共享根目录下的 `fetchers/`, `storage/`, `utils/`, `processors/` 模块。

---

## 2. Sub-Project 1: deribit-options-data-collector

### 2.1 Overview

Deribit 期权 + 合约策略数据实时采集系统。单进程多线程架构，支持 WS 实时推送和 REST 轮询。

**数据类型**: options_greeks, options_ticker, vol_surface, mark_price, funding_rate, margin_params, spot_price, basis, risk_free_rate

**交易所**: Deribit (WS+REST), Binance (REST), Hyperliquid (REST), FRED (REST)

### 2.2 Launch Command

```bash
# 在项目根目录执行
cd /path/to/Crypto_FreeAPIs

# 实盘持续采集
python deribit-options-data-collector/launch.py --mode live

# 60秒验证测试
python deribit-options-data-collector/launch.py --mode test

# 仅启动 P0 级采集器
python deribit-options-data-collector/launch.py --mode live --strategies P0

# 启动 P0 + P1 级
python deribit-options-data-collector/launch.py --mode live --strategies P0,P1
```

### 2.3 Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `--mode` | `live`, `test` | `live` | `live`=持续运行, `test`=60秒后退出 |
| `--strategies` | `all`, `P0`, `P0,P1` | `all` | 策略优先级过滤 |

### 2.4 Collector Architecture

```
P0 (实时):
  WS-Bridge      → QuoteFetcher WebSocket → ChunkedBuffer
  MarkPrice       → 30s 轮询, Binance + Deribit
  SpotPrice       → 1s 轮询, Binance
  GreeksProcessor → 5s 轮询, Deribit option chain

P1 (低频):
  FundingRate   → 8h 轮询, Binance + Deribit + Hyperliquid
  MarginParams  → 24h 轮询, Deribit
  BasisVol      → 10s 计算, Binance spot vs perp

P2 (日频):
  RiskFreeRate  → 24h 轮询, FRED
```

### 2.5 Config File

`deribit-options-data-collector/config_strategy.yaml`

```yaml
global:
  data_dir: "./deribit-options-data-collector/data"
  log_level: "INFO"

storage:
  chunked_buffer:
    max_rows: 100000
    flush_interval_sec: 300     # 5分钟 flush 一次到磁盘
  compression: "zstd"

api:
  binance:
    base_url_spot: "https://api.binance.com/api/v3"
    rate_limit_rpm: 2800
  deribit:
    base_url: "https://www.deribit.com/api/v2"
    ws_url: "wss://www.deribit.com/ws/api/v2"
    rate_limit_rps: 20
    ws_max_channels_per_conn: 300
    ws_max_connections: 6
  hyperliquid:
    base_url: "https://api.hyperliquid.xyz"
    rate_limit_rpm: 120
  fred:
    base_url: "https://api.stlouisfed.org/fred/series/observations"
    rate_limit_per_hour: 120
```

### 2.6 Output

```
deribit-options-data-collector/data/
└── {exchange}/{data_type}/{symbol}_{YYYY-MM-DD}.parquet

deribit-options-data-collector/logs/
├── collector.log              # RotatingFileHandler, 50MB x 5
└── last_audit.md              # 退出时的数据完整性审计
```

### 2.7 Server Operations

#### SSH Connection

```bash
ssh -i ~/.ssh/id_rsa root@217.76.63.39
```

| Item | Value |
|------|-------|
| Host | `217.76.63.39` |
| User | `root` |
| SSH Key | `~/.ssh/id_rsa` |
| Project Path | `/opt/Crypto_FreeAPIs` |
| Venv | `/opt/Crypto_FreeAPIs/venv` |
| tmux Session | `crypto` |

#### Project Update

```bash
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "cd /opt/Crypto_FreeAPIs && git pull"
```

#### Service Management (tmux)

```bash
# 查看是否运行
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "tmux has-session -t crypto 2>/dev/null && echo 'RUNNING' || echo 'STOPPED'"

# 启动服务
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "tmux new-session -d -s crypto && tmux send-keys -t crypto 'source /opt/Crypto_FreeAPIs/venv/bin/activate && python /opt/Crypto_FreeAPIs/deribit-options-data-collector/launch.py --mode live' Enter"

# 查看实时日志
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "tmux capture-pane -t crypto -p | tail -30"

# 停止服务 (优雅退出, flush buffer)
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "tmux send-keys -t crypto C-c"

# 完整重启流程: 停止 → 更新 → 启动
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "tmux send-keys -t crypto C-c"  # stop
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "cd /opt/Crypto_FreeAPIs && git pull"  # update
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "sleep 3 && tmux send-keys -t crypto 'source /opt/Crypto_FreeAPIs/venv/bin/activate && python /opt/Crypto_FreeAPIs/deribit-options-data-collector/launch.py --mode live' Enter"  # restart
```

#### Data Management

```bash
# 查看远程数据文件
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "find /opt/Crypto_FreeAPIs/deribit-options-data-collector/data -name '*.parquet' -type f | sort"

# 查看磁盘使用
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "du -sh /opt/Crypto_FreeAPIs/deribit-options-data-collector/data/"

# 检查日志错误
ssh -i ~/.ssh/id_rsa root@217.76.63.39 "grep -c ERROR /opt/Crypto_FreeAPIs/deribit-options-data-collector/logs/collector.log"
```

#### Local Data Pull

```bash
# 拉取昨天的数据 (下载后自动清理远程文件)
bash scripts/pull_data.sh

# 拉取指定日期
bash scripts/pull_data.sh 2026-05-15

# 批量拉取多天
for d in 2026-05-14 2026-05-15 2026-05-16; do bash scripts/pull_data.sh $d; done
```

**Script**: `scripts/pull_data.sh`

| Variable | Value |
|----------|-------|
| `REMOTE_HOST` | `root@217.76.63.39` |
| `REMOTE_DIR` | `/opt/Crypto_FreeAPIs/deribit-options-data-collector/data` |
| `LOCAL_DIR` | `./deribit-options-data-collector/data` |

**Flow**: scan remote → scp download → verify local exists → batch rm remote

---

## 3. Sub-Project 2: ohlcv-collector

### 3.1 Overview

OHLCV (Open-High-Low-Close-Volume) K线数据批量采集管线。支持历史回填和每日增量更新。

**数据类型**: OHLCV candles (open, high, low, close, volume)

**交易所**: Binance Spot, Binance USDT-M Futures, Deribit, Hyperliquid

### 3.2 Launch Command

```bash
# 在项目根目录执行
cd /path/to/Crypto_FreeAPIs

# 每日增量更新 (默认 1d)
python ohlcv-collector/launch.py --mode daily

# 历史回填 365 天, 1d 周期
python ohlcv-collector/launch.py --mode backfill --days 365

# 回填多个周期
python ohlcv-collector/launch.py --mode backfill --days 90 --timeframes 1m,15m,30m,1h,4h,1d,1w

# 单标的测试
python ohlcv-collector/launch.py --mode single --exchange binance_spot --symbol BTC --days 7 --timeframe 1d

# 自定义配置
python ohlcv-collector/launch.py --mode daily --config /path/to/config.yaml
```

### 3.3 Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `--mode` | `backfill`, `daily`, `single` | `daily` | 运行模式 |
| `--exchange` | exchange name | - | single 模式必需 |
| `--symbol` | symbol name | - | single 模式必需 |
| `--days` | int | `365` | 回填天数 |
| `--timeframe` | single tf | `1d` | 单个 K 线周期 |
| `--timeframes` | comma-separated | `1d` | 多个 K 线周期, 如 `1m,15m,1h,1d` |
| `--config` | file path | `ohlcv-collector/config.yaml` | 配置文件路径 |

### 3.4 Mode Details

| Mode | Description | Behavior |
|------|-------------|----------|
| `backfill` | 历史回填 | 从 `start_ms` 开始采集每个标的, ParquetStore 自动去重 |
| `daily` | 每日增量 | 检查最后时间戳, 仅拉取缺失部分 |
| `single` | 单标的测试 | 指定单个交易所+标的快速验证 |

### 3.5 Supported Timeframes

```
1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
```

### 3.6 Config File

`ohlcv-collector/config.yaml`

```yaml
exchanges:
  binance_spot:
    enabled: true
    base_url: "https://api.binance.com/api/v3"
    rate_limit:
      requests_per_minute: 2800
    symbols:
      BTC: "BTCUSDT"
      ETH: "ETHUSDT"
      SOL: "SOLUSDT"
      # ... 14 symbols total

  binance_usdm:
    enabled: true
    base_url: "https://fapi.binance.com/fapi/v1"
    rate_limit:
      requests_per_minute: 100
    symbols:
      BTC: "BTCUSDT"
      # ... 10 symbols total

  deribit:
    enabled: true
    base_url: "https://www.deribit.com/api/v2"
    rate_limit:
      requests_per_second: 15
    symbols:
      BTC: "BTC-PERPETUAL"
      ETH: "ETH-PERPETUAL"
      SOL: "SOL-PERPETUAL"

  hyperliquid:
    enabled: true
    base_url: "https://api.hyperliquid.xyz"
    rate_limit:
      requests_per_minute: 1000
    symbols:
      BTC: "BTC"
      ETH: "ETH"
      # ... 12 symbols total (kPEPE, kBONK etc.)

global:
  timeframe: "1d"
  data_dir: "./ohlcv-collector/data"
  default_history_days: 365
  max_retries: 3
  retry_delay_seconds: 2
```

### 3.7 Output

```
ohlcv-collector/data/
└── {exchange}/{symbol}_{timeframe}.parquet   # 每个标的一个文件, 持续追加

# 示例
ohlcv-collector/data/binance_spot/BTC_1d.parquet
ohlcv-collector/data/binance_spot/ETH_1d.parquet
ohlcv-collector/data/deribit/BTC_1d.parquet
```

**注意**: OHLCV 的存储模式与 deribit 子项目不同 — 每个标的一个 parquet 文件，按时间戳追加去重，而非按日期分文件。

### 3.8 Storage: ParquetStore

`ParquetStore.save()` 自动处理:
- **去重**: 按 (timestamp, exchange, symbol) 合并，保留最新
- **排序**: 写入后按 timestamp 升序排列
- **原子写入**: 先写临时文件再 rename

---

## 4. Key Differences

| | deribit-options-data-collector | ohlcv-collector |
|---|---|---|
| **类型** | 实时采集 (WS + REST 轮询) | 批量采集 (REST) |
| **运行模式** | 持续运行 (tmux) | 一次性运行 (cron) |
| **数据频率** | tick 级 (1s~30s) | K线级 (1m~1M) |
| **存储引擎** | ChunkedBuffer (内存缓冲 + 定期 flush) | ParquetStore (直接读写) |
| **文件组织** | 按日期分文件 `{symbol}_{date}.parquet` | 按标的分文件 `{symbol}_{tf}.parquet` |
| **配置文件** | `config_strategy.yaml` | `config.yaml` |
| **服务器部署** | tmux 常驻 | 按需执行 |
| **数据拉取** | `scripts/pull_data.sh` | 本地执行即可 |

---

## 5. Testing

```bash
# 运行所有测试
cd /path/to/Crypto_FreeAPIs
python -m pytest tests/ -v

# 数据质量验证
python scripts/verify_collected_data.py --no-log
```
