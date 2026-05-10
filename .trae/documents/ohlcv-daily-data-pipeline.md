# OHLCV 每日数据采集管线 — 实施计划

## 一、项目概述

构建一个 Python 自动化管线，每日从 **Binance（现货 + USDT-M 合约）**、**Deribit**、**Hyperliquid** 四个数据源采集 OHLCV（开高低收量）数据，覆盖 14 个加密货币标的。

> **运行环境**：项目根目录下创建 Python venv 虚拟环境，所有后续操作均在该虚拟环境中执行。

## 二、标的与交易所覆盖矩阵

| 标的 | Binance Spot | Binance USDT-M Futures | Deribit (Perp) | Hyperliquid (Perp) |
|------|:---:|:---:|:---:|:---:|
| BTC  | ✅ BTCUSDT | ✅ BTCUSDT | ✅ BTC-PERP | ✅ BTC |
| ETH  | ✅ ETHUSDT | ✅ ETHUSDT | ✅ ETH-PERP | ✅ ETH |
| SOL  | ✅ SOLUSDT | ✅ SOLUSDT | ✅ SOL-PERP | ✅ SOL |
| BNB  | ✅ BNBUSDT | ✅ BNBUSDT | ❌ | ❌ 可能无 |
| XRP  | ✅ XRPUSDT | ✅ XRPUSDT | ❌ | ✅ XRP |
| ADA  | ✅ ADAUSDT | ✅ ADAUSDT | ❌ | ✅ ADA |
| DOGE | ✅ DOGEUSDT | ✅ DOGEUSDT | ❌ | ✅ DOGE |
| SHIB | ✅ SHIBUSDT | ✅ SHIBUSDT | ❌ | ⚠️ 待验证 |
| PEPE | ✅ PEPEUSDT | ✅ PEPEUSDT | ❌ | ✅ PEPE |
| WIF  | ✅ WIFUSDT | ✅ WIFUSDT | ❌ | ✅ WIF |
| BONK | ✅ BONKUSDT | ✅ BONKUSDT | ❌ | ✅ BONK |
| FLOKI| ✅ FLOKIUSDT | ✅ FLOKIUSDT | ❌ | ⚠️ 待验证 |
| LINK | ✅ LINKUSDT | ✅ LINKUSDT | ❌ | ✅ LINK |
| AVAX | ✅ AVAXUSDT | ✅ AVAXUSDT | ❌ | ✅ AVAX |

> **说明**：Deribit 仅有 BTC/ETH/SOL 的永续合约；Hyperliquid 需在运行时动态查询可用标的；Binance 现货和 USDT-M 合约覆盖全部 14 个标的。

## 三、技术选型

| 项 | 选择 | 理由 |
|----|------|------|
| 语言 | Python 3.10+ | 数据处理生态成熟 |
| HTTP 客户端 | `requests` | 轻量、稳定 |
| 数据存储 | Parquet (主) + CSV (备份) | Parquet 压缩率高、读取快 |
| 调度 | 内置 `schedule` 或 cron/任务计划 | 无额外依赖 |
| 配置 | YAML 配置文件 | 可读性好 |

## 四、项目结构

```
FreeAPIs/
├── venv/                      # Python 虚拟环境
├── config.yaml                # 全局配置
├── requirements.txt           # 依赖
├── run.py                     # 入口脚本（支持全量回填 & 增量更新）
├── fetchers/
│   ├── __init__.py
│   ├── base.py                # BaseFetcher 抽象基类
│   ├── binance.py             # Binance OHLCV 采集器（现货 + USDT-M 合约）
│   ├── deribit.py             # Deribit OHLCV 采集器
│   └── hyperliquid.py         # Hyperliquid OHLCV 采集器
├── models/
│   ├── __init__.py
│   └── ohlcv.py               # OHLCV 数据模型
├── storage/
│   ├── __init__.py
│   └── parquet_store.py       # Parquet 存储管理
├── utils/
│   ├── __init__.py
│   ├── rate_limiter.py        # 速率限制器
│   └── logger.py              # 日志工具
└── data/                      # 数据输出目录
    ├── binance/
    │   ├── spot/              # Binance 现货数据
    │   └── usdm/              # Binance USDT-M 合约数据
    ├── deribit/
    └── hyperliquid/
```

## 五、API 详情与调用方式

### 5.1 Binance Spot（现货）

- **端点**：`GET https://api.binance.com/api/v3/klines`
- **认证**：无需
- **参数**：`symbol`, `interval`, `startTime`, `endTime`, `limit` (最大 1000)
- **速率限制**：6000 weight/分钟/IP，klines 每次消耗 2 weight（约 3000 次/分钟）
- **日K采集**：每次请求最多获取 1000 根日线，单次即可覆盖数年
- **响应格式**：数组嵌套数组，`[open_time, open, high, low, close, volume, close_time, quote_volume, ...]`

### 5.2 Binance USDT-M 永续合约

- **端点**：`GET https://fapi.binance.com/fapi/v1/klines`
- **认证**：无需
- **参数**：`symbol`, `interval`, `startTime`, `endTime`, `limit` (最大 **1500**)
- **速率限制**：2400 req/分钟/IP，每次消耗 20 weight（约 120 次/分钟）
- **日K采集**：每次请求最多获取 1500 根日线
- **响应格式**：与现货相同，`[open_time, open, high, low, close, volume, close_time, quote_volume, ...]`
- **注意**：USDT-M 合约与现货使用不同的 base URL 和 symbol 命名空间，AVAX 合约 symbol 为 `AVAXUSDT`（与现货相同）

### 5.3 Deribit

- **端点**：`GET https://www.deribit.com/api/v2/public/get_tradingview_chart_data`
- **认证**：无需
- **参数**：`instrument_name`, `start_timestamp`, `end_timestamp`, `resolution`
- **速率限制**：20 req/s/IP
- **日K采集**：`resolution="1D"`，每次最多返回约 10000 根
- **响应格式**：并行数组 `{ticks, open, high, low, close, volume}`
- **注意**：仅支持 BTC-PERP、ETH-PERP、SOL-PERP

### 5.4 Hyperliquid

- **端点**：`POST https://api.hyperliquid.xyz/info`
- **认证**：无需
- **请求体**：`{"type": "candleSnapshot", "req": {"coin": "BTC", "interval": "1d", "startTime": ...}}`
- **速率限制**：~1200 req/min/IP
- **日K采集**：`interval="1d"`
- **响应格式**：对象数组 `[{t, T, s, i, o, c, h, l, v, n}, ...]`
- **注意**：需在运行时调用 `{"type": "meta"}` 动态获取可用标的列表

## 六、核心模块设计

### 6.1 统一 OHLCV 数据模型

```python
# 统一所有交易所返回为同一格式
class OHLCV:
    timestamp: int          # 开盘时间 (ms)
    open: float
    high: float
    low: float
    close: float
    volume: float           # 基础货币成交量
    quote_volume: float     # 计价货币成交量 (USDT/USD)
    exchange: str           # 交易所名
    symbol: str             # 统一标的符号 (如 BTC)
    timeframe: str          # K线周期 (如 1d)
```

### 6.2 BaseFetcher 抽象基类

```python
class BaseFetcher(ABC):
    def __init__(self, config, rate_limiter):
        ...
    
    @abstractmethod
    def fetch_ohlcv(self, symbol, timeframe, start_ts, end_ts) -> list[OHLCV]:
        """获取指定标的、时间范围的 OHLCV 数据"""
        ...
    
    @abstractmethod
    def get_symbol_mapping(self) -> dict[str, str]:
        """返回 {统一标的名: 交易所标的名} 的映射"""
        ...
    
    def fetch_daily(self, symbol, end_ts=None) -> list[OHLCV]:
        """获取最新一天的数据（增量更新用）"""
        ...
```

### 6.3 RateLimiter 速率限制器

- 基于 token bucket 算法
- 每个交易所实例配置不同的速率限制参数
- 自动 sleep 等待

### 6.4 Parquet 存储

- 文件组织方式：`data/{exchange_type}/{symbol}_{timeframe}.parquet`
  - Binance Spot: `data/binance_spot/{symbol}_{timeframe}.parquet`
  - Binance USDT-M: `data/binance_usdm/{symbol}_{timeframe}.parquet`
- 每个标的一个文件，追加写入
- 自动去重（基于 timestamp）
- 支持读取最后一条记录的时间戳，用于增量更新起点

### 6.5 配置文件 (config.yaml)

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
      BNB: "BNBUSDT"
      XRP: "XRPUSDT"
      ADA: "ADAUSDT"
      DOGE: "DOGEUSDT"
      SHIB: "SHIBUSDT"
      PEPE: "PEPEUSDT"
      WIF: "WIFUSDT"
      BONK: "BONKUSDT"
      FLOKI: "FLOKIUSDT"
      LINK: "LINKUSDT"
      AVAX: "AVAXUSDT"

  binance_usdm:
    enabled: true
    base_url: "https://fapi.binance.com/fapi/v1"
    rate_limit:
      requests_per_minute: 100    # 留余量（每分钟最多120次）
    symbols:
      BTC: "BTCUSDT"
      ETH: "ETHUSDT"
      SOL: "SOLUSDT"
      BNB: "BNBUSDT"
      XRP: "XRPUSDT"
      ADA: "ADAUSDT"
      DOGE: "DOGEUSDT"
      SHIB: "SHIBUSDT"
      PEPE: "PEPEUSDT"
      WIF: "WIFUSDT"
      BONK: "BONKUSDT"
      FLOKI: "FLOKIUSDT"
      LINK: "LINKUSDT"
      AVAX: "AVAXUSDT"

  deribit:
    enabled: true
    base_url: "https://www.deribit.com/api/v2"
    rate_limit:
      requests_per_second: 15     # 留余量
    symbols:
      BTC: "BTC-PERP"
      ETH: "ETH-PERP"
      SOL: "SOL-PERP"

  hyperliquid:
    enabled: true
    base_url: "https://api.hyperliquid.xyz"
    rate_limit:
      requests_per_minute: 1000   # 留余量
    symbols:
      BTC: "BTC"
      ETH: "ETH"
      SOL: "SOL"
      XRP: "XRP"
      ADA: "ADA"
      DOGE: "DOGE"
      PEPE: "PEPE"
      WIF: "WIF"
      BONK: "BONK"
      LINK: "LINK"
      AVAX: "AVAX"
      # SHIB 和 FLOKI 需运行时验证

global:
  timeframe: "1d"
  data_dir: "./data"
  default_history_days: 365       # 默认回填天数
  log_level: "INFO"
```

## 七、运行模式

### 7.1 历史回填模式

```bash
python run.py --mode backfill --days 365
```

- 从 N 天前开始，批量拉取所有标的的所有历史日K数据
- 支持断点续传（检测已有数据的最后时间戳）

### 7.2 每日增量更新模式

```bash
python run.py --mode daily
```

- 读取每个 parquet 文件的最后时间戳
- 仅拉取缺失的新数据并追加
- 适合 cron/任务计划每日定时运行

### 7.3 单标的测试模式

```bash
python run.py --mode single --exchange binance_spot --symbol BTC --days 30
python run.py --mode single --exchange binance_usdm --symbol BTC --days 30
```

## 八、错误处理策略

1. **网络重试**：自动重试 3 次，指数退避
2. **速率限制**：检测 429 响应，自动等待后重试
3. **标的不可用**：Hyperliquid 动态检测，跳过不可用的标的并记录日志
4. **数据验证**：检查 OHLCV 数据的完整性（无空值、high >= open/close >= low）
5. **去重**：写入前基于 timestamp 去重

## 九、实施步骤（按顺序执行）

1. **创建 venv 虚拟环境**：在项目根目录下创建 Python 虚拟环境
2. **创建项目骨架**：目录结构、`requirements.txt`、`config.yaml`
3. **实现 utils 模块**：`rate_limiter.py`、`logger.py`
4. **实现 models 模块**：`ohlcv.py` 数据模型
5. **实现 storage 模块**：`parquet_store.py` Parquet 读写与去重
6. **实现 Binance 采集器**：`fetchers/binance.py`（含 spot 和 USDT-M 两个数据源）
7. **实现 Deribit 采集器**：`fetchers/deribit.py`
8. **实现 Hyperliquid 采集器**：`fetchers/hyperliquid.py`（含动态标的验证）
9. **实现入口脚本**：`run.py`（支持 backfill / daily / single 三种模式）
10. **端到端测试**：运行单标的测试验证全流程
11. **全量回填测试**：对所有交易所和标的执行历史数据回填

## 十、依赖

```
requests>=2.31.0
pandas>=2.1.0
pyarrow>=14.0.0
pyyaml>=6.0
```

无需数据库，无需付费 API，所有数据端点均为公开免费。
