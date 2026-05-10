# Crypto Quant Data Pipeline — 独立项目开发 Prompt

> 将此文件全部内容复制到新 session。项目将在独立目录下从零创建，不依赖任何现有项目。

---

## 一、你要构建什么

一个 Python 数据采集与存储框架，专门服务于 **BTC/ETH 期权 + 永续合约量化交易系统**。

数据来源只有两个交易所：
- **Deribit** — BTC/ETH 期权（IV、OI、Ticker、DVOL 波动率指数）+ 永续合约
- **Binance** — BTC/ETH 永续合约（K线、资金费率、订单簿）

这些数据最终要喂给三条策略路径：

| 路径 | 策略 | 它需要什么数据 | 实时性 |
|------|------|--------------|--------|
| Path3 | IV 期限结构 / 波动率制度识别 | DVOL 指数 + 永续 OHLCV | 1 分钟 |
| Path2 | Gamma 暴露 / 做市商对冲流预测 | 全量期权合约 OI + IV（30s） | 30 秒 |
| Path1 | 波动率曲面套利 | 全量期权实时 IV + 订单簿（100ms） | 100 毫秒 |

**请按阶段 1 → 2 → 3 的顺序开发，每完成一个阶段即可交付使用。**

---

## 二、技术栈

- Python 3.11+
- asyncio + aiohttp（REST 并发）
- websockets 库（阶段 3 的 WebSocket）
- pandas + pyarrow（Parquet 存储）
- logging（结构化日志，不用 print）
- 项目自管理依赖（pyproject.toml 或 requirements.txt）

---

## 三、项目结构

```
crypto_quant_data/
├── pyproject.toml
├── README.md
├── src/
│   └── crypto_quant_data/
│       ├── __init__.py
│       ├── config.py               # API URL、限速参数、数据目录等配置
│       ├── models.py               # 数据模型（dataclass / Pydantic）
│       │
│       ├── collectors/             # 数据采集器
│       │   ├── __init__.py
│       │   ├── base.py             # BaseCollector 抽象基类
│       │   ├── deribit_dvol.py     # 阶段1: DVOL 指数采集
│       │   ├── deribit_options.py  # 阶段2: 期权 OI/IV 批量采集
│       │   ├── deribit_ws.py       # 阶段3: WebSocket 实时采集
│       │   ├── binance_klines.py   # 阶段1: 永续 K线采集
│       │   └── binance_funding.py  # 阶段1: 资金费率采集
│       │
│       ├── storage/                # 数据持久化
│       │   ├── __init__.py
│       │   └── parquet_store.py    # Parquet 读写、按日分片、去重
│       │
│       ├── scheduler.py            # asyncio 调度器，管理多采集器并发
│       └── pipeline.py             # CLI 入口，编排阶段 1/2/3
│
├── data/                           # 数据存储目录（.gitignore）
│   └── store/
│       ├── BTC/
│       │   ├── dvol/
│       │   │   ├── 2025-01-01.parquet
│       │   │   └── ...
│       │   ├── options_ticker/
│       │   └── ...
│       ├── ETH/
│       ├── BTCUSDT/
│       │   ├── kline_5m/
│       │   ├── funding_rate/
│       │   └── ...
│       └── ETHUSDT/
│
└── tests/
    ├── test_dvol.py
    ├── test_klines.py
    └── test_storage.py
```

---

## 四、数据模型（models.py）

请定义以下核心数据类：

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class DvolTick:
    """DVOL 波动率指数数据点"""
    currency: str           # "BTC" or "ETH"
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float

@dataclass(frozen=True)
class OptionTicker:
    """单个期权合约的快照"""
    instrument_name: str    # e.g. "BTC-28MAR26-80000-C"
    timestamp: datetime
    underlying_price: float # 标的当前价格
    mark_price: float       # 期权标记价格（BTC 计价）
    bid_price: float
    ask_price: float
    bid_iv: float           # Bid 隐含波动率（小数，如 0.65 = 65%）
    ask_iv: float
    mark_iv: float
    open_interest: float    # OI（合约数量）
    volume_24h: float
    settlement_period: str  # "day", "week", "month", "quarter"

@dataclass(frozen=True)
class KlineBar:
    """K线数据"""
    symbol: str             # "BTCUSDT"
    interval: str           # "5m"
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: datetime
    quote_volume: float
    trades: int

@dataclass(frozen=True)
class FundingRate:
    """资金费率"""
    symbol: str
    funding_time: datetime
    funding_rate: float     # 如 0.0001 = 0.01%
    mark_price: float
```

---

## 五、各阶段详细 API 规格

### 阶段 1：Path3 数据管线（先做这个）

#### 5.1.1 DVOL 采集器

**接口**: `GET https://www.deribit.com/api/v2/public/get_volatility_index_data`

参数:
- `currency`: "BTC" 或 "ETH"
- `start_timestamp`: 毫秒时间戳（可选）
- `end_timestamp`: 毫秒时间戳（可选）

响应:
```json
{
  "result": [
    {
      "timestamp": 1704067200000,
      "open": 62.34,
      "high": 63.12,
      "low": 61.78,
      "close": 62.56
    }
  ]
}
```
IV 值单位是**年化百分比**（如 62.34 = 62.34%）。

采集逻辑:
1. 启动时：拉取过去 365 天历史数据（分段请求，每次 1 个月）
2. 稳态：每 60 秒请求最新 1 分钟数据
3. 不需要认证（公共 API）

#### 5.1.2 Binance 永续 K 线采集器

**接口**: `GET https://fapi.binance.com/fapi/v1/klines`

参数:
- `symbol`: "BTCUSDT" 或 "ETHUSDT"
- `interval`: "5m"
- `limit`: 1500（单次最大）
- `startTime`: 毫秒时间戳（可选）
- `endTime`: 毫秒时间戳（可选）

响应: 二维数组，每行 `[open_time, open, high, low, close, volume, close_time, quote_volume, trades, ...]`

采集逻辑:
1. 启动时：从 365 天前开始，每次拉 1500 条 5m K线（约 5.2 天），循环直到当前
2. 稳态：每 5 分钟拉取最新 1 条
3. 限速：单次请求 weight=5，总限 2400 weight/min

#### 5.1.3 Binance 资金费率采集器

**接口**: `GET https://fapi.binance.com/fapi/v1/fundingRate`

参数:
- `symbol`: "BTCUSDT" 或 "ETHUSDT"
- `limit`: 1000（单次最大）
- `startTime` / `endTime`: 毫秒时间戳

响应: `[{"symbol": "BTCUSDT", "fundingTime": ..., "fundingRate": "0.00010000", "markPrice": "..."}]`

采集逻辑:
1. 启动时：拉取过去 365 天历史（每 8h 一条，约 1095 条，1 次请求即可）
2. 稳态：每 8 小时拉取最新

---

### 阶段 2：Path2 数据管线

#### 5.2.1 期权合约列表

**接口**: `GET https://www.deribit.com/api/v2/public/get_instruments`

参数: `currency=BTC&kind=option` 或 `currency=ETH&kind=option`

响应: 数组，每项含 `instrument_name`, `strike`, `option_type`（call/put）, `expiration_timestamp`, `settlement_period`, `min_trade_amount`, `contract_size` 等

#### 5.2.2 期权合约 Ticker 批量采集

**接口**: `GET https://www.deribit.com/api/v2/public/ticker`

参数: `instrument_name=BTC-28MAR26-80000-C`

响应关键字段:
```json
{
  "result": {
    "instrument_name": "BTC-28MAR26-80000-C",
    "timestamp": 1711622400000,
    "underlying_price": 83250.5,
    "mark_price": 0.0254,
    "bid_price": 0.0248,
    "ask_price": 0.0260,
    "bid_iv": 0.62,
    "ask_iv": 0.64,
    "mark_iv": 0.63,
    "open_interest": 1250.0,
    "stats": {"volume": 350.0}
  }
}
```
注意：`mark_price` 和 `bid/ask_price` 以 **BTC 计价**（非 USD），`*_iv` 是年化波动率小数。

采集逻辑:
1. 启动时拉合约列表（BTC ~200 个，ETH ~150 个）
2. 每 30 秒批量请求所有合约 Ticker
3. 限速 20 req/s → 200 合约分 10 批，每批 20，批间 sleep 50ms → 2 秒完成
4. 不需要认证

#### 5.2.3 期权合约 Summary（备用，含 OI）

**接口**: `GET https://www.deribit.com/api/v2/public/get_summary`

参数: `instrument_name=BTC-28MAR26-80000-C`

响应含 `open_interest`, `volume`, `bid_price`, `ask_price` 等。Ticker 接口已经包含 OI，此接口可选作为交叉验证。

---

### 阶段 3：Path1 数据管线（可延后）

#### 5.3.1 Deribit WebSocket

连接地址: `wss://www.deribit.com/ws/api/v2`

订阅格式:
```json
{
  "jsonrpc": "2.0",
  "method": "public/subscribe",
  "params": {
    "channels": [
      "ticker.BTC-28MAR26-80000-C.100ms",
      "ticker.BTC-28MAR26-85000-C.100ms",
      "book.BTC-28MAR26-80000-C.10.100ms"
    ]
  }
}
```

需要管理 200-300 个频道，自动重连，数据采样后落盘（每分钟保存一次 IV 曲面快照）。

#### 5.3.2 Binance WebSocket

永续深度: `wss://fstream.binance.com/ws/btcusdt@depth20@100ms`

---

## 六、存储设计（storage/parquet_store.py）

### 6.1 目录结构

```
data/store/
├── BTC/
│   ├── dvol/                    # DvolTick, 每日一个文件
│   │   ├── 2025-01-01.parquet
│   │   └── ...
│   └── options_ticker/          # OptionTicker, 每日一个文件
│       ├── 2025-01-01.parquet
│       └── ...
├── ETH/
│   ├── dvol/
│   └── options_ticker/
├── BTCUSDT/
│   ├── kline_5m/
│   └── funding_rate/
└── ETHUSDT/
    ├── kline_5m/
    └── funding_rate/
```

### 6.2 Schema（Parquet 列定义）

**dvol/**: `currency(str), timestamp(datetime64[ms]), open(float64), high(float64), low(float64), close(float64)`

**options_ticker/**: `instrument_name(str), timestamp(datetime64[ms]), underlying_price(float64), mark_price(float64), bid_price(float64), ask_price(float64), bid_iv(float64), ask_iv(float64), mark_iv(float64), open_interest(float64), volume_24h(float64), settlement_period(str)`

**kline_5m/**: `symbol(str), interval(str), open_time(datetime64[ms]), open(float64), high(float64), low(float64), close(float64), volume(float64), close_time(datetime64[ms]), quote_volume(float64), trades(int64)`

**funding_rate/**: `symbol(str), funding_time(datetime64[ms]), funding_rate(float64), mark_price(float64)`

### 6.3 读写接口

```python
class ParquetStore:
    def __init__(self, base_dir: str = "data/store"): ...

    def save(self, currency: str, data_type: str, df: pd.DataFrame) -> None:
        """按日期分片写入，自动去重（按 timestamp 列）"""

    def load(self, currency: str, data_type: str,
             start_date: str, end_date: str) -> pd.DataFrame:
        """加载日期范围内的所有分片，返回合并后的 DataFrame"""

    def latest_timestamp(self, currency: str, data_type: str) -> Optional[datetime]:
        """获取最新一条数据的时间戳（用于增量拉取）"""
```

---

## 七、调度器设计（scheduler.py）

```python
class Scheduler:
    """管理多个采集器的并发运行"""

    async def add(self, collector: BaseCollector, interval_seconds: float): ...

    async def run(self):
        """启动所有采集器，每个按自己的频率循环"""

    async def stop(self):
        """优雅关闭：等待当前采集周期完成"""

class BaseCollector(ABC):
    """采集器基类"""

    @abstractmethod
    async def fetch_history(self, store: ParquetStore) -> None:
        """启动时拉取历史数据"""

    @abstractmethod
    async def fetch_incremental(self, store: ParquetStore) -> None:
        """稳态时拉取增量数据"""

    @abstractmethod
    async def close(self) -> None:
        """关闭连接"""
```

---

## 八、CLI 入口（pipeline.py）

```bash
# 阶段 1: DVOL + OHLCV + FundingRate
python -m crypto_quant_data.pipeline --stage 1

# 阶段 2: 加入期权 OI/IV 采集
python -m crypto_quant_data.pipeline --stage 2

# 阶段 3: 加入 WebSocket 实时采集
python -m crypto_quant_data.pipeline --stage 3

# 仅拉取历史数据（不启动实时循环）
python -m crypto_quant_data.pipeline --stage 1 --history-only

# 指定数据目录
python -m crypto_quant_data.pipeline --stage 1 --data-dir /mnt/data/crypto
```

---

## 九、错误处理和健壮性要求

1. **网络错误**: aiohttp ClientError → 指数退避重试（最多 3 次，间隔 1s/2s/4s）
2. **限速 429**: 读取 `Retry-After` 头，等待后重试
3. **数据校验**: 落盘前过滤异常值
   - IV: `0.01 < bid_iv < 5.0`（1% - 500%），否则置 NaN
   - 价格: `> 0`
   - OI: `>= 0`
4. **去重**: 写入前按 timestamp 去重，保留最新值
5. **断连恢复**: WebSocket 断连后自动重连，重连期间标记数据 stale
6. **日志**: 使用 logging 模块，INFO 级别记录每次采集的结果条数和耗时
7. **优雅关闭**: 监听 SIGINT/SIGTERM，完成当前写入后退出

---

## 十、验证标准

### 阶段 1 完成后应能运行：

```python
from crypto_quant_data.storage import ParquetStore

store = ParquetStore()

# 历史数据存在且完整
dvol = store.load("BTC", "dvol", "2025-06-01", "2026-05-01")
assert len(dvol) > 500_000  # ~1 年分钟数据

klines = store.load("BTCUSDT", "kline_5m", "2025-06-01", "2026-05-01")
assert len(klines) > 100_000  # ~1 年 5 分钟数据

funding = store.load("BTCUSDT", "funding_rate", "2025-06-01", "2026-05-01")
assert len(funding) > 1000  # ~1 年 8 小时数据

# 数据连续性检查
assert dvol["timestamp"].is_monotonic_increasing
assert klines["open_time"].is_monotonic_increasing
```

### 阶段 2 完成后额外能运行：

```python
# 期权 Ticker 数据
tickers = store.load("BTC", "options_ticker", "2026-05-01", "2026-05-06")
assert len(tickers) > 50_000  # 200 合约 × 多轮采集 × 多天

# 每个合约都有数据
unique_contracts = tickers["instrument_name"].nunique()
assert unique_contracts > 100  # 至少 100 个活跃合约
```

---

## 十一、注意事项

1. **Deribit 公共 API 不需要 API Key**，阶段 1-2 全部用公共接口
2. **Binance 也不需要 Key** 就能拉取历史 K线和资金费率
3. 首先完成阶段 1，确保 DVOL + Klines + FundingRate 能稳定运行
4. 阶段 2 的期权数据量较大（~100 万条/天），注意 Parquet 压缩效率
5. 阶段 3 的 WebSocket 复杂度高，先确保 1-2 稳定再做
6. 代码需支持 Windows 和 Linux
