# 期权+永续合约组合策略 — 数据基础设施开发 Prompt

> 将此文件内容完整复制到新 session 中作为任务上下文

---

## 项目背景

这是一个加密货币量化交易系统项目，基于 **Deribit** 交易所的 BTC/ETH 期权和永续合约，实现 6 类期权+永续组合策略。

### 目标策略矩阵

| 策略 | 频率 | 方向敞口 | 核心数据依赖 | 优先级 |
|------|------|----------|-------------|--------|
| Short Strangle + 永续对冲 | 日级（1-3次再平衡） | 中性 | 期权bid/ask、组合Delta、保证金率 | P0 |
| 合成备兑看涨（Synthetic Covered Call） | 周级移仓 | 温和多 | Call权利金、永续价格、资金费率 | P0 |
| 领口策略（Collar） | 月级移仓 | 温和多 | Call/Put权利金差、Delta | P0 |
| 资金费率套利 + 期权尾部保护 | 日级 | 中性 | 资金费率、基差、Put权利金 | P1 |
| Gamma Scalping（中低频版） | 日级（1-2次再平衡） | 中性 | 实时Greeks、永续bid/ask | P1 |
| 波动率期限结构套利（Calendar Spread） | 周级 | 中性 | 近远端IV、两组Greeks | P2 |

### 策略协同关系

- **P0 三策略**互斥运行：根据市场状态（IV百分位、波动率环境）选择其一
- **P1 两策略**可在特定时机叠加到P0之上
- **P2** 作为独立模块，在期限结构异常时触发

---

## 现有项目结构

```
strategy_research/
├── core/
│   ├── strategy_base.py      # 策略基类（已有 Signal, Position, MarketData, StrategyBase）
│   ├── __init__.py
│   └── examples.py
├── docs/
│   ├── prompts/              # 开发prompt文档
│   └── strategy_plans/       # 策略详细执行文档
├── research_reports/         # 策略研报
├── data/                     # 数据存储目录（已有部分数据管线）
│   ├── collectors/
│   ├── storage/
│   └── pipeline.py
├── config/
│   └── sources.yaml
└── CLAUDE.md
```

策略基类 `core/strategy_base.py` 已定义：`Side`, `Signal`, `Position`, `MarketData`, `RiskMetrics`, `StrategyBase`。新代码应复用这些数据类。

---

## 当前已有的数据能力

- BTC/ETH 期权合约列表获取（Deribit REST）
- BTC/ETH 期权日频历史数据获取
- DVOL 波动率指数采集（阶段1已实现）
- 永续 OHLCV + Funding Rate（阶段1已实现）

---

## 开发任务：期权+永续组合策略数据管线

### 目标

构建完整的 Python 数据采集与计算框架，按优先级分三个阶段实现：

---

### 阶段 1：核心行情数据（P0策略必需）

#### 1.1 期权历史数据（回测用）

| 数据项 | 来源API | 频率 | 存储 |
|--------|---------|------|------|
| **期权合约列表** | `GET /api/v2/public/get_instruments?currency=BTC&kind=option` | 启动时拉取 | SQLite缓存 |
| **期权日线OHLCV** | `GET /api/v2/public/get_tradingview_chart_data?instrument_name={name}&resolution=1D` | 每日 | Parquet |
| **期权Bid/Ask/Mark快照** | `GET /api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option` | 日快照 | Parquet |
| **Greeks快照（Δ/Γ/Θ/V）** | 包含在 book_summary 响应中 | 日快照 | Parquet |
| **IV快照** | 从 mark_price 反推，或用 Tardis.dev 历史 | 日快照 | Parquet |
| **OI + 成交量** | 包含在 book_summary 响应中 | 日快照 | Parquet |

**数据模型（期权日线快照）：**

```python
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class OptionSnapshot:
    timestamp: datetime          # 快照时间
    symbol: str                  # e.g. "BTC"
    instrument_name: str         # e.g. "BTC-28MAR26-80000-C"
    strike: float                # 行权价
    expiry: datetime             # 到期日
    option_type: str             # "call" | "put"
    bid_price: float             # 买价
    ask_price: float             # 卖价
    mark_price: float            # 标记价格
    underlying_price: float      # 标的价格
    iv: float                    # 隐含波动率
    delta: float                 # Delta
    gamma: float                 # Gamma
    theta: float                 # Theta
    vega: float                  # Vega
    open_interest: float         # 未平仓量
    volume_24h: float            # 24h成交量
    dte: int                     # 距到期天数
```

#### 1.2 永续合约数据（回测+实盘共用）

| 数据项 | 来源API | 频率 | 存储 |
|--------|---------|------|------|
| **永续OHLCV** | Deribit `GET /api/v2/public/get_tradingview_chart_data?instrument_name=BTC-PERPETUAL&resolution=60` | 小时级 | Parquet |
| **资金费率历史** | Deribit `GET /api/v2/public/get_funding_rate_history?instrument_name=BTC-PERPETUAL` | 8h | Parquet |
| **标记价格历史** | Deribit `GET /api/v2/public/get_mark_price_history?instrument_name=BTC-PERPETUAL` | 分钟级 | Parquet |
| **基差序列** | 永续价格 - 指数价格（计算值） | 小时级 | Parquet |

**数据模型（永续数据）：**

```python
@dataclass(frozen=True)
class PerpData:
    timestamp: datetime
    symbol: str                  # "BTC" | "ETH"
    open: float
    high: float
    low: float
    close: float
    volume: float
    funding_rate: float          # 当前资金费率
    mark_price: float            # 标记价格
    index_price: float           # 指数价格
    basis: float                 # mark_price - index_price
    basis_pct: float             # basis / index_price * 100
    open_interest: float         # 持仓量
```

#### 1.3 派生指标（每日计算并存储）

| 指标 | 计算方式 | 用途 |
|------|----------|------|
| **ATM IV** | 最近行权价的ATM期权IV | 波动率基准 |
| **IV期限结构** | ATM IV × 到期日（7d/14d/30d/60d/90d） | 判断Contango/Backwardation |
| **IV偏斜（25D Skew）** | 25Δ Call IV - 25Δ Put IV | 尾部风险定价 |
| **IV百分位（IV Rank）** | (当前IV - 52w最低) / (52w最高 - 52w最低) × 100 | 策略选择信号 |
| **已实现波动率（RV）** | 日收益率标准差 × √252（20日滚动） | IV-RV比较 |
| **IV-RV价差** | ATM IV - 30日RV | 判断期权贵贱 |
| **Put-Call比率** | Put OI总和 / Call OI总和 | 市场情绪 |
| **VWVOL（加权IV）** | 按OI加权的整体IV | 市场整体波动率水平 |

**数据模型（派生指标）：**

```python
@dataclass(frozen=True)
class DerivedMetrics:
    timestamp: datetime
    symbol: str
    atm_iv: float
    iv_7d: float
    iv_14d: float
    iv_30d: float
    iv_60d: float
    iv_90d: float
    skew_25d: float              # 25Δ偏斜
    iv_rank: float               # 0-100
    realized_vol_20d: float      # 20日已实现波动率
    iv_rv_spread: float          # IV - RV
    put_call_ratio: float        # OI加权
    total_call_oi: float
    total_put_oi: float
    total_call_volume: float
    total_put_volume: float
```

---

### 阶段 2：实盘实时数据（P0+P1策略运行必需）

#### 2.1 WebSocket 实时行情

| 数据项 | WebSocket频道 | 推送频率 | 用途 |
|--------|--------------|----------|------|
| **期权Ticker** | `ticker.{instrument_name}.100ms` | 100ms | 实时价格、IV、Greeks |
| **期权Order Book** | `book.{instrument_name}.10` | 100ms | 买卖深度、滑价估算 |
| **永续Ticker** | `ticker.BTC-PERPETUAL` | 100ms | 对冲下单价格 |
| **永续Order Book** | `book.BTC-PERPETUAL.10` | 100ms | 对冲滑价评估 |
| **指数价格** | `ticker.BTC_USDC` | 持续 | 定价基准 |
| **DVOL** | `ticker.BTC-DVOL` | 持续 | 波动率实时监控 |
| **用户持仓推送** | `user.{account_id}` | 事件驱动 | 持仓变动通知 |
| **用户订单推送** | `user.{account_id}` | 事件驱动 | 订单状态更新 |

**实时数据模型：**

```python
@dataclass(frozen=True)
class RealtimeOptionTick:
    timestamp: datetime
    instrument_name: str
    bid_price: float
    ask_price: float
    mark_price: float
    underlying_price: float
    iv: float
    delta: float
    gamma: float
    theta: float
    vega: float
    bid_iv: float                # 买方隐含波动率
    ask_iv: float                # 卖方隐含波动率

@dataclass(frozen=True)
class RealtimePerpTick:
    timestamp: datetime
    instrument_name: str         # e.g. "BTC-PERPETUAL"
    bid_price: float
    ask_price: float
    mark_price: float
    index_price: float
    funding_rate: float
    open_interest: float
```

#### 2.2 账户/风控实时数据

| 数据项 | API | 频率 | 用途 |
|--------|-----|------|------|
| **持仓列表** | `/private/get_positions` | 每次再平衡前 | 组合Greeks计算 |
| **账户摘要** | `/private/get_account_summary` | 30s轮询 | 保证金监控 |
| **初始/维持保证金** | Portfolio Margin API | 持续 | 爆仓预警 |
| **组合Delta** | 从持仓Greeks汇总 | 持续 | 再平衡触发 |
| **未实现盈亏** | 账户API | 持续 | 盈亏跟踪 |

**风控数据模型：**

```python
@dataclass(frozen=True)
class PortfolioState:
    timestamp: datetime
    total_delta: float           # 组合总Delta（含期权+永续）
    total_gamma: float           # 组合总Gamma
    total_theta: float           # 组合总Theta（日衰减）
    total_vega: float            # 组合总Vega
    margin_balance: float        # 保证金余额
    initial_margin: float        # 初始保证金占用
    maintenance_margin: float    # 维持保证金占用
    margin_usage_pct: float      # 保证金使用率
    unrealized_pnl: float        # 未实现盈亏
    position_count: int          # 持仓合约数
```

#### 2.3 实时信号/触发器

| 信号名 | 触发条件 | 动作 | 检查频率 |
|--------|----------|------|----------|
| `delta_rebalance` | \|组合Delta\| > 阈值（默认0.15） | 永续对冲 | 30s |
| `roll_needed` | 单腿 \|Delta\| > 0.35 或距行权价 < 2% | 移仓 | 5min |
| `iv_spike` | IV偏离20日均值 > 2σ | 调仓/暂停 | 1min |
| `funding_flip` | 资金费率符号翻转 | 策略参数调整 | 8h |
| `margin_warning` | MM使用率 > 70% | 减仓/追加 | 30s |
| `margin_critical` | MM使用率 > 90% | 紧急平仓 | 10s |
| `expiry_approach` | DTE < 5天 | 移仓提醒 | 日级 |
| `iv_regime_change` | IV Rank跨过30/70 | 策略切换 | 日级 |

---

### 阶段 3：高级数据（P2策略 + 优化用）

| 数据项 | 来源 | 频率 | 用途 |
|--------|------|------|------|
| **完整IV曲面快照** | 所有期权tickers聚合 | 1min | 期限结构套利 |
| **Gamma敞口（GEX）按行权价** | OI × Gamma × 100 按strike聚合 | 5min | 判断dealer对冲方向 |
| **最大痛点（Max Pain）** | 各到期日计算 | 日级 | 到期日价格吸引 |
| **历史IV曲面** | Tardis.dev（付费，~$100/月） | 日快照 | 回测 |
| **大户交易流** | Deribit block trade API | 实时 | 方向参考 |

---

## 代码结构要求

```
data/
├── __init__.py
├── config.py                      # 数据源配置
├── models.py                      # 所有数据模型（OptionSnapshot, PerpData, etc.）
├── collectors/
│   ├── __init__.py
│   ├── base_collector.py          # 采集器基类
│   ├── deribit_option_rest.py     # 期权REST采集（合约列表、快照、历史）
│   ├── deribit_option_ws.py       # 期权WebSocket（实时tick）
│   ├── deribit_perp_rest.py       # 永续REST采集（OHLCV、资金费率、标记价格）
│   ├── deribit_perp_ws.py         # 永续WebSocket（实时tick、订单簿）
│   ├── deribit_account.py         # 账户数据（持仓、保证金）
│   └── derived_calculator.py      # 派生指标计算（IV曲面、期限结构、偏斜等）
├── storage/
│   ├── __init__.py
│   ├── parquet_store.py           # Parquet存储引擎
│   ├── schema.py                  # Parquet schema定义
│   └── cache.py                   # SQLite缓存（合约列表等）
├── signals/
│   ├── __init__.py
│   ├── rebalance_signal.py        # 再平衡信号检测
│   ├── roll_signal.py             # 移仓信号检测
│   └── risk_signal.py             # 风控信号检测（保证金、IV异常）
├── scheduler.py                   # 调度器（管理采集频率和并发）
└── pipeline.py                    # 管线编排

strategy/
├── __init__.py
├── combined/
│   ├── __init__.py
│   ├── base_combined.py           # 组合策略基类（含Greeks管理）
│   ├── short_strangle_hedged.py   # Short Strangle + 永续对冲
│   ├── synthetic_covered_call.py  # 合成备兑看涨
│   ├── collar.py                  # 领口策略
│   ├── funding_arb_enhanced.py    # 资金费率套利 + 期权保护
│   ├── gamma_scalping.py          # Gamma Scalping（中低频版）
│   └── calendar_spread.py         # 期限结构套利
└── risk/
    ├── __init__.py
    ├── portfolio_manager.py       # 组合管理（Greeks汇总、保证金计算）
    └── signal_router.py           # 信号路由（根据市场状态选择策略）
```

---

## 技术要求

### 语言和框架

- Python 3.11+
- asyncio + aiohttp（REST并发请求）
- websockets 库（WebSocket连接管理）
- pandas + pyarrow（Parquet读写）
- numpy（波动率计算、Greeks计算）
- 数据类复用 `core/strategy_base.py` 中已有的结构

### 设计原则

1. **不可变数据模型**：所有 `@dataclass(frozen=True)` ，不修改已有对象
2. **每个采集器独立运行**，互不阻塞
3. **统一的错误处理**：API失败时指数退避重试，WebSocket断连时自动重连
4. **数据落盘前校验**：IV > 0, OI >= 0, 价格 > 0, timestamp 单调递增
5. **限速控制**：Deribit 20 req/s, WebSocket 订阅有上限
6. **优雅关闭**：收到 SIGINT/SIGTERM 时保存已采集数据后退出
7. **关注点分离**：数据采集 / 派生计算 / 信号检测 / 策略执行 各层独立

### Deribit API 认证说明

- **公共数据**（ticker, summary, instruments, DVOL, OHLCV）：**不需要 API Key**
- **私有数据**（持仓、保证金、订单）：需要 API Key + Secret（HMAC签名）
- 阶段 1 仅使用公共 API，无需任何账户
- 阶段 2 的账户数据需要认证，但可以先在 Testnet 免费测试

### Deribit API 限速规则

- REST 公共 API：20 req/s
- REST 私有 API：按 nonce 排序，无明确速率限制
- WebSocket：单连接最多订阅 300 个频道
- 使用 `await asyncio.sleep(0.05)` 在批量请求间加入间隔
- 如果返回 429，指数退避重试

---

## 阶段 1 的具体实现要求（请优先完成）

### 1. 数据模型定义 (`models.py`)

定义上述所有 frozen dataclass。额外增加：

```python
@dataclass(frozen=True)
class IVSurface:
    """某时刻的完整IV曲面"""
    timestamp: datetime
    symbol: str
    strikes: tuple[float, ...]        # 行权价数组
    expiries: tuple[datetime, ...]     # 到期日数组
    iv_matrix: tuple[tuple[float, ...], ...]  # strike × expiry 的IV矩阵

@dataclass(frozen=True)
class StrategySignal:
    """策略触发的交易信号"""
    timestamp: datetime
    strategy_name: str
    signal_type: str             # "open" | "close" | "rebalance" | "roll"
    instrument: str
    side: str                    # "buy" | "sell"
    quantity: float
    reason: str                  # 触发原因描述
    urgency: str                 # "low" | "medium" | "high" | "critical"
```

### 2. 期权快照采集器 (`deribit_option_rest.py`)

```python
class OptionSnapshotCollector:
    """采集全量期权链的日快照"""

    async def get_instruments(self, currency: str) -> list[dict]:
        """获取所有期权合约列表"""

    async def get_all_tickers(self, currency: str) -> list[dict]:
        """批量获取所有期权的ticker（bid/ask/mark/greeks/iv/oi）"""

    async def get_historical_ohlcv(self, instrument_name: str, resolution: str, start_ts: int, end_ts: int) -> list[dict]:
        """获取单个合约的历史K线"""

    async def collect_daily_snapshot(self, currency: str) -> list[OptionSnapshot]:
        """采集每日完整快照，返回不可变对象列表"""
```

关键设计要点：
- `get_book_summary_by_currency` 一次返回某币种所有期权摘要
- 但不含 Greeks 详细值，需对重点合约额外调 `ticker` 接口
- Deribit 限速 20 req/s，BTC ~200 合约需分批

### 3. 永续数据采集器 (`deribit_perp_rest.py`)

```python
class PerpCollector:
    """采集永续合约历史和实时数据"""

    async def get_ohlcv(self, instrument_name: str, resolution: str = "60", start_ts: int = None, end_ts: int = None) -> list[PerpData]:
        """获取OHLCV"""

    async def get_funding_rate_history(self, instrument_name: str, start_ts: int = None, end_ts: int = None) -> list[dict]:
        """获取历史资金费率"""

    async def get_mark_price_history(self, instrument_name: str, resolution: str = "1") -> list[dict]:
        """获取标记价格历史"""
```

### 4. 派生指标计算器 (`derived_calculator.py`)

```python
class DerivedCalculator:
    """从原始数据计算派生指标"""

    def calculate_atm_iv(self, snapshots: list[OptionSnapshot], target_dte: int) -> float:
        """计算指定DTE的ATM IV"""

    def calculate_iv_term_structure(self, snapshots: list[OptionSnapshot]) -> dict[str, float]:
        """计算IV期限结构 {7d: iv, 14d: iv, 30d: iv, 60d: iv, 90d: iv}"""

    def calculate_skew(self, snapshots: list[OptionSnapshot], expiry: datetime) -> float:
        """计算25Delta偏斜"""

    def calculate_iv_rank(self, iv_history: list[float], current_iv: float) -> float:
        """计算IV百分位（0-100）"""

    def calculate_realized_vol(self, prices: list[float], window: int = 20) -> float:
        """计算已实现波动率"""

    def calculate_iv_surface(self, snapshots: list[OptionSnapshot]) -> IVSurface:
        """构建完整IV曲面"""

    def calculate_daily_metrics(self, symbol: str, date: datetime) -> DerivedMetrics:
        """计算并返回当日所有派生指标"""
```

### 5. 存储 (`storage/parquet_store.py`)

```python
class ParquetStore:
    """Parquet存储引擎"""

    def save_snapshots(self, snapshots: list[OptionSnapshot]) -> None:
        """保存期权快照，按日期分片"""

    def save_perp_data(self, data: list[PerpData]) -> None:
        """保存永续数据"""

    def save_derived(self, metrics: list[DerivedMetrics]) -> None:
        """保存派生指标"""

    def load(self, symbol: str, data_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """查询数据"""
```

文件路径格式：`data/store/{symbol}/{data_type}/{date}.parquet`

### 6. 管线入口 (`pipeline.py`)

```python
# CLI启动
# python -m data.pipeline --mode historical --currency BTC --start 2025-01-01 --end 2026-05-01
# python -m data.pipeline --mode realtime --currency BTC,ETH
# python -m data.pipeline --mode derived --currency BTC --recalculate
```

---

## 验证标准

完成后应能通过以下测试：

```python
# 1. 期权快照
snapshots = store.load("BTC", "option_snapshot", "2026-04-01", "2026-05-01")
assert len(snapshots) > 100      # 每日约200+合约
assert "delta" in snapshots.columns
assert "iv" in snapshots.columns

# 2. 永续数据
perp = store.load("BTC", "perp_hourly", "2025-01-01", "2026-05-01")
assert len(perp) > 8000          # 约1年的小时数据
assert "funding_rate" in perp.columns
assert "basis_pct" in perp.columns

# 3. 派生指标
derived = store.load("BTC", "derived_daily", "2025-06-01", "2026-05-01")
assert len(derived) > 300        # 约1年
assert "iv_rank" in derived.columns
assert "iv_rv_spread" in derived.columns

# 4. IV曲面
surface = calculator.calculate_iv_surface(snapshots_today)
assert len(surface.strikes) > 20
assert len(surface.expiries) > 5

# 5. 信号检测
signal = rebalance_detector.check(portfolio_state)
# 当 |delta| > 0.15 时应触发
assert signal is not None
assert signal.signal_type == "rebalance"
```

---

## 关键Deribit API端点汇总

### 公共REST API（无需认证）

```
# 期权合约列表
GET /api/v2/public/get_instruments?currency=BTC&kind=option&expired=false

# 全量期权摘要（含OI、mark_price、underlying_price）
GET /api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option

# 单个期权ticker（含Greeks）
GET /api/v2/public/ticker?instrument_name=BTC-28MAR26-80000-C

# 永续合约摘要
GET /api/v2/public/get_book_summary_by_currency?currency=BTC&kind=future

# 永续历史K线
GET /api/v2/public/get_tradingview_chart_data?instrument_name=BTC-PERPETUAL&resolution=60&start_ts={ms}&end_ts={ms}

# 资金费率历史
GET /api/v2/public/get_funding_rate_history?instrument_name=BTC-PERPETUAL&start_ts={ms}&end_ts={ms}

# 标记价格历史
GET /api/v2/public/get_mark_price_history?instrument_name=BTC-PERPETUAL&resolution=1

# DVOL波动率指数
GET /api/v2/public/get_volatility_index_data?currency=BTC

# 指数价格
GET /api/v2/public/get_index_price?index_name=btc_usd

# 期权历史K线
GET /api/v2/public/get_tradingview_chart_data?instrument_name=BTC-28MAR26-80000-C&resolution=1D&start_ts={ms}&end_ts={ms}
```

### WebSocket频道（公共）

```
# 订阅期权ticker
ticker.{instrument_name}.100ms    → {bid/ask/mark/iv/greeks}

# 订阅期权订单簿
book.{instrument_name}.10         → {bids/asks depth}

# 订阅永续ticker
ticker.BTC-PERPETUAL              → {bid/ask/mark/index/funding}

# 订阅永续订单簿
book.BTC-PERPETUAL.10             → {bids/asks depth}

# 订阅指数
ticker.BTC_USDC                   → {index price}

# 订阅DVOL
ticker.BTC-DVOL                   → {volatility index}

# 订阅用户频道（需认证）
user.{account_id}                 → {positions, orders, trades}
```

### 私有REST API（需认证）

```
# 持仓查询
GET /api/v2/private/get_positions?currency=BTC

# 账户摘要（含保证金）
GET /api/v2/private/get_account_summary?currency=BTC

# 下单
POST /api/v2/private/buy   {instrument_name, amount, price, type}
POST /api/v2/private/sell  {instrument_name, amount, price, type}

# 平仓
POST /api/v2/private/close_position {instrument_name, type, price}
```

### WebSocket认证流程

```
1. 连接 wss://www.deribit.com/ws/api/v2
2. 发送 auth 请求：
   {
     "jsonrpc": "2.0",
     "id": 1,
     "method": "public/auth",
     "params": {
       "grant_type": "client_signature",
       "client_id": "{api_key}",
       "data": "{nonce}",
       "signature": "{hmac_sha256_hex}"
     }
   }
3. 认证成功后订阅用户频道
```

---

## 数据量估算

| 数据类型 | 单日记录数 | 年存储（Parquet压缩） |
|----------|-----------|---------------------|
| BTC期权快照（日） | ~200条 | ~5 MB |
| ETH期权快照（日） | ~150条 | ~4 MB |
| 永续小时线 | ~24条 | < 1 MB |
| 资金费率 | ~3条 | < 1 MB |
| 派生指标 | ~1条 | < 1 MB |
| 实时tick（运行时） | 内存中 | 不落盘 |
| **1年总计** | | **~50 MB** |

非常轻量，个人笔记本完全够用。

---

## 参考资料

### 策略详细文档

- `docs/strategy_plans/path1_vol_surface_arbitrage.md` — IV曲面套利
- `docs/strategy_plans/path2_gamma_exposure.md` — Gamma暴露
- `docs/strategy_plans/path3_iv_term_structure.md` — IV期限结构

### 外部参考

- [Deribit API 官方文档](https://docs.deribit.com/)
- [Deribit Testnet](https://test.deribit.com/) — 免费测试环境
- [schepal/delta_hedge](https://github.com/schepal/delta_hedge) — Python Deribit delta对冲工具
- [ivanvgreiff/gamma-scalping-algorithm](https://github.com/ivanvgreiff/gamma-scalping-algorithm) — Gamma Scalping完整实现
- [Tardis.dev](https://tardis.dev/) — 付费历史数据（回测用）
- [Greeks.live](https://greeks.live/) — 免费期权Greeks聚合

### 现有代码

- `core/strategy_base.py` — 已有 `MarketData`, `Signal`, `Position`, `Side`, `RiskMetrics`, `StrategyBase`
- `data/` — 已有阶段1管线（DVOL + Binance OHLCV + Funding Rate）
