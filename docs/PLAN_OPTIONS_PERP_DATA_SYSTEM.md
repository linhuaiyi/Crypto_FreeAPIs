# 实施计划：期权+合约策略数据采集系统

> 创建日期: 2026-05-07
> 基于: PROJECT_STARTUP_TEMPLATE.md v2.0
> 范围: Sprint 1 + Sprint 2 任务
> 版本: v3.0 (面向生产: WebSocket 优先、存储生命周期、日历对齐)
> 部署环境: 4 vCPU / 8 GB RAM / 75 GB NVMe (云端采集节点)
> 架构: 云端采集 + 定期同步回本地归档与投研

---

## 一、需求重述

构建支持 6 类期权+永续合约组合策略的完整数据采集系统：

| 策略 | 核心数据需求 | 优先级 |
|------|-------------|--------|
| Short Strangle + 永续对冲 | 期权 bid/ask、Delta、保证金率、永续价格 | P0 |
| 合成备兑看涨 | Call 权利金、永续价格、资金费率 | P0 |
| 领口策略 | Call/Put 权利金差、Delta | P0 |
| 资金费率套利 + 期权保护 | 资金费率、基差、Put 权利金 | P1 |
| Gamma Scalping | 实时 Greeks、永续 bid/ask | P1 |
| 波动率期限结构套利 | 近远端 IV、两组 Greeks | P2 |

**当前状态**: 基础 OHLCV 采集 + 期权 Greeks 计算已实现 70%，缺少关键衍生数据（资金费率、基差、波动率曲面、主力映射等）。

---

## 二、差距分析

### 已完成 (可直接复用)
- [x] Deribit REST API 客户端（限速、重试）
- [x] 期权链解析器（instrument_name 解析、到期日筛选）
- [x] Black-Scholes Greeks 计算（delta/gamma/vega/theta/rho）
- [x] 隐含波动率 Newton-Raphson 计算
- [x] Parquet 存储（去重、追加、统计）
- [x] 基础 OHLCV 采集（Binance/Deribit/Hyperliquid）
- [x] Binance 永续合约 OHLCV

### 缺失 (需新建)
| # | 模块 | 描述 | 策略依赖 |
|---|------|------|----------|
| M1 | 资金费率采集器 | Binance/Deribit/Hyperliquid funding rate | P0 合成备兑、P1 费率套利 |
| M2 | 标记价格采集器 | 永续合约 mark price + index price | P0 全部策略 |
| M3 | 基差计算器 | 现货-永续基差、期权合成基差 | P1 费率套利 |
| M4 | 无风险利率曲线 | FRED API 对接 + 插值 | P0 Greeks 精度 |
| M5 | 主力合约映射 | OI 自动识别主/次主力 | P0 全部策略 |
| M6 | 波动率曲面构建 | ATM IV、Skew、期限结构 | P2 波动率套利 |
| M7 | 数据质量验证 | 间隙检测 + 异常值过滤 | 全部策略 |
| M8 | 统一数据管线 | 期权+永续一体化采集调度 | 全部策略 |
| M9 | L1 订单簿采集器 | 期权/永续 Best Bid/Ask 实时快照 | P0 Short Strangle、P1 Gamma Scalping |
| M10 | 保证金参数采集器 | 期权/永续 IM/MM 基准率 | P0 Short Strangle、P1 全部对冲策略 |
| M11 | 截面时间对齐器 | 期权(低频)+合约(高频) as-of join 对齐 | 全部组合策略 |
| M12 | 现货价格补充 | Binance Spot 现货 OHLCV/Ticker（基差计算上游） | M3 基差计算器依赖 |
| M13 | 存储生命周期管理 | Chunked 缓冲写入、Hive 分区、云端 14 天自动清理 | 全部模块（生产约束） |

---

## 三、实施计划

### Phase 1: 基础衍生数据采集 (P0 - 核心数据层)

#### Task 1.1: 资金费率采集模块 `fetchers/funding_rate.py`
**目标**: 从 Binance/Deribit/Hyperliquid 采集历史+实时资金费率

新建文件:
- `fetchers/funding_rate.py` — 统一资金费率接口

实现内容:
```
class FundingRateFetcher:
    - Binance: GET /fapi/v1/fundingRate (历史) + /fapi/v1/premiumIndex (实时)
    - Deribit: GET /public/get_funding_chart_data (历史)
    - Hyperliquid: POST /info endpoint (fundingRate)
    - 输出: {timestamp, exchange, symbol, funding_rate, mark_price, index_price}
    - 存储: data/{exchange}_funding/{symbol}_funding.parquet
```

依赖: 现有 `BaseFetcher` 模式、`ParquetStore`

#### Task 1.2: 永续标记价格采集模块 `fetchers/mark_price.py`
**目标**: 采集永续合约 mark_price 和 index_price

新建文件:
- `fetchers/mark_price.py` — 标记价格采集器

实现内容:
```
class MarkPriceFetcher:
    - Binance: /fapi/v1/premiumIndex
    - Deribit: /public/get_mark_price_history
    - Hyperliquid: /info (midPx, markPx)
    - 输出: {timestamp, exchange, symbol, mark_price, index_price, basis}
    - 存储: data/{exchange}_mark/{symbol}_mark.parquet
```

#### Task 1.3: 无风险利率模块 `fetchers/risk_free_rate.py`
**目标**: 对接 FRED API，构建连续复利无风险利率曲线

新建文件:
- `fetchers/risk_free_rate.py` — FRED API 对接
- `utils/interpolation.py` — 样条插值工具

实现内容:
```
class RiskFreeRateFetcher:
    - FRED API: DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS5, DGS10, DGS30
    - 本地缓存 (避免频繁请求)
    - 连续复利转换: r_continuous = ln(1 + r_annual)
    - 样条插值: 生成任意期限的无风险利率
    - 降级方案: 若 FRED 不可用，使用固定 5% 利率

    # 日历对齐 (V3.0 新增)
    - 问题: FRED 仅在美国工作日更新，周末和法定节假日无新数据
    - 方案: 前值填充 (ffill) — 非交易日自动沿用上一个有效工作日的利率
    - 实现:
        1. 拉取原始 FRED 序列 (可能跳过周末/节假日)
        2. 生成完整日历 (365 天，含周末/节假日)
        3. 对缺失日期执行 ffill，确保每一天都有利率值
        4. 对未来日期沿用最近一个有效值
    - 美国法定节假日列表 (内置):
        New Year's Day, MLK Day, Presidents' Day, Good Friday,
        Memorial Day, Juneteenth, Independence Day, Labor Day,
        Thanksgiving, Christmas (按 NYSE 日历)
    - 输出: 每日一条 {date, tenor, rate_continuous, rate_annual, is_trading_day}
    - 健壮性: 插值后的利率曲线在非交易日保持连续无跳变
```

#### Task 1.4: 主力合约映射模块 `utils/main_contract.py`
**目标**: 基于 OI 自动识别主力/次主力期权合约

新建文件:
- `utils/main_contract.py` — 主力映射逻辑

实现内容:
```
class MainContractMapper:
    - 获取全量期权合约 + OI
    - 按到期日分组
    - 每组内按 OI 排序，取 Top-N
    - ATM 合约自动识别（最接近平值的 strike）
    - 输出: {expiry, main_calls, main_puts, atm_strike}
```

#### Task 1.5: 实时买卖盘/Tick 采集模块 `fetchers/quote_fetcher.py`
**目标**: 采集期权和永续合约的 L1 订单簿（Best Bid / Best Ask）快照

**背景**: 期权价差通常占 mark_price 的 1-5%，仅依赖 Mark Price 进行回测会产生严重的滑点幻觉。Short Strangle 策略的盈亏直接取决于入场/出场 bid-ask 中点，Gamma Scalping 更需要实时 spread 估算对冲成本。

**V3.0 升级**: 底层协议从纯 REST 轮询升级为「WebSocket 订阅优先，REST 快照为辅」。1 秒级 REST 轮询 ~1700 个期权合约极易触发限流（Deribit 20 req/s），WebSocket 可单连接订阅 300 个频道，按需扩连。

新建文件:
- `fetchers/quote_fetcher.py` — L1 报价采集器（WS+REST 双模）
- `fetchers/ws_orderbook.py` — WebSocket 增量订单簿维护引擎

实现内容:
```
class QuoteFetcher:
    # 双模架构: WebSocket 增量 + REST 快照校准

    # 模式 1: WebSocket 实时订阅 (主路径)
    # ┌─────────────────────────────────────────────────────┐
    # │ WS 连接池 (Deribit)                                  │
    # │  Connection 1: BTC 永续 + 主力期权 (up to 300 ch)    │
    # │  Connection 2: ETH 永续 + 主力期权 (up to 300 ch)    │
    # │  Connection 3: SOL 永续 + 次主力期权 (up to 300 ch)  │
    # │                                                      │
    # │ 订阅频道:                                             │
    # │  Deribit: ticker.{instrument_name}.100ms              │
    # │  Binance: <symbol>@bookTicker                         │
    # │  Hyperliquid: 所有订阅通过单连接                       │
    # └─────────────────────────────────────────────────────┘

    class WSOrderbookEngine:
        - 维护本地 L1 订单簿状态 (dict: instrument -> {bid, ask, bid_sz, ask_sz})
        - 收到 ticker.push 时更新本地状态
        - 按固定间隔 (1s) 从本地状态截取快照，送入写入缓冲区
        - 心跳检测: 30s 无消息触发自动重连
        - 重连时先 REST 快照校准，再恢复 WS 订阅

    # 模式 2: REST 快照 (辅助路径)
    # 用途: 启动时初始快照、WS 断连降级、历史回补
    # Deribit: GET /public/ticker (batch 50/请求)
    # Binance: GET /fapi/v1/ticker/bookTicker
    # Hyperliquid: POST /info (levels=1)

    # 数据输出格式 (统一，无论来源)
    - 输出: {timestamp, instrument_name, exchange, source (ws/rest),
             bid_price, ask_price, bid_size, ask_size,
             mid_price, spread, spread_bps,
             bid_iv, ask_iv (仅期权)}
    - 存储: 通过 ChunkedBuffer 写入 (见 Task 2.6)
    - 截取频率: 1s 快照 (从 WS 维护的本地状态截取)
    - 特殊处理: 期权 bid=0 或 ask=0 表示单边无报价，标记 `no_bid`/`no_ask`

    # 连接管理
    - 最大 WS 连接数: 6 (3x Deribit + 1x Binance + 1x Hyperliquid + 1 备用)
    - 每连接最大订阅: 300 个频道 (Deribit 限制)
    - 主力合约优先订阅，非主力合约降级到 REST 轮询 (10s 间隔)
```

**WebSocket 订阅优先级**:
```
Priority 1 (WS): 永续 BTC/ETH/SOL + 主力期权 ATM±2 strikes
Priority 2 (WS): 次主力期权、近到期期权
Priority 3 (REST 10s): 远到期、深度 OTM 期权
```

**关键设计决策**:
- mid_price = (bid + ask) / 2 作为回测入场价基准，优于 mark_price
- spread_bps = (ask - bid) / mid * 10000 用于策略滑点估算
- 期权 bid_iv / ask_iv 从 Deribit WS ticker 直接获取
- WS 断连时自动降级到 REST 轮询，确保数据连续
- 本地订单簿状态占用内存极小 (~1700 合约 × 6 字段 ≈ 100 KB)

依赖: `websockets` 库 (已在 requirements)、`storage/chunked_buffer.py` (Task 2.6)

#### Task 1.6: 保证金与风控参数模块 `fetchers/margin_params.py`
**目标**: 获取期权和永续合约的初始保证金（IM）和维持保证金（MM）基准率

**背景**: Short Strangle 等卖出策略占用动态保证金，保证金率直接影响资金利用率（Lev = 1/IM）和爆仓线。没有保证金数据，回测无法计算真实收益（需扣除保证金占用成本），也无法模拟强平场景。

新建文件:
- `fetchers/margin_params.py` — 保证金参数采集器

实现内容:
```
class MarginParamsFetcher:
    - Deribit: GET /public/get_instruments (返回 contract_size, tick_size,
      max_leverage 等规格字段)
    - Deribit 保证金模拟: GET /private/get_margins (需认证，可降级)
    - Deribit 公开替代: GET /public/get_contract_size + 静态保证金表
    - Binance: GET /fapi/v1/exchangeInfo (maintMarginPercent, requiredMarginPercent)
    - 输出: {timestamp, instrument_name, instrument_type,
             initial_margin_rate, maintenance_margin_rate,
             max_leverage, contract_size, tick_size,
             min_order_size}
    - 存储: data/margin_params/{exchange}_margin_params.parquet
    - 采集频率: 日级（保证金率变动不频繁）
    - 降级方案: 无认证时，使用 Deribit 公开的阶梯保证金表硬编码
```

**Deribit 保证金阶梯表** (公开数据，可内置):
```
# 期权卖方保证金 = Max(基础保证金, 最小保证金)
# 基础保证金 = 期权费 + Max(x% * 标的 - OTM金额, y% * 标的)
# 阶梯: BTC/ETH 各约 5 档，按名义价值分档
```

**关键设计决策**:
- 优先从 API 获取实时参数，降级到内置静态表
- 输出统一为比率（0-1），而非绝对金额，便于跨币种比较
- 日级采集即可，保证金参数不随 tick 变化

依赖: 现有 `BaseFetcher` 模式、`ParquetStore`

---

### Phase 2: 数据处理与增强 (P1 - 质量层)

#### Task 2.1: 数据间隙检测器 `processors/gap_detector.py`
**目标**: 检测 >60s 的数据间隙，记录并支持插值

新建文件:
- `processors/gap_detector.py` — 间隙检测
- `processors/__init__.py` — 处理模块入口

实现内容:
```
class GapDetector:
    - 输入: 时间序列 DataFrame
    - 检测: 连续记录时间差 > 阈值（默认 60s）
    - 输出: {gap_start, gap_end, gap_duration, affected_instruments}
    - 插值策略: 前值填充（OHLCV）、线性插值（Greeks）
    - 日志: WARNING 级别记录间隙
```

#### Task 2.2: 异常值过滤器 `processors/outlier_filter.py`
**目标**: Z-Score > 5 的异常值标记/过滤

新建文件:
- `processors/outlier_filter.py`

实现内容:
```
class OutlierFilter:
    - 滚动窗口 Z-Score 计算（窗口 100 期）
    - 标记策略: 标记但不删除，增加 `is_outlier` 列
    - 过滤字段: mark_price, iv, volume, oi
    - 配置: z_threshold=5, window_size=100
```

#### Task 2.3: 波动率曲面构建器 `processors/vol_surface.py`
**目标**: 基于 ATM IV 构建 Skew 和期限结构

新建文件:
- `processors/vol_surface.py`

实现内容:
```
class VolatilitySurfaceBuilder:
    - ATM IV: 最接近平值合约的 IV
    - 25-Delta Risk Reversal: 25Δ Call IV - 25Δ Put IV
    - Butterfly: (25Δ Call IV + 25Δ Put IV) / 2 - ATM IV
    - Term Structure: 不同到期日的 ATM IV 序列
    - IV Rank: 当前 IV 在过去 N 天的百分位
    - 输出: Parquet 快照（每日）
```

#### Task 2.4: 基差计算器 `processors/basis_calculator.py`
**目标**: 独立处理器，计算现货-永续基差及期权合成基差

**背景**: 原计划 M3 仅在标记价格中顺带输出 basis 字段，无法满足"资金费率套利"策略对基差时序精细化分析的需求。基差是费率套利策略的核心信号——当基差偏离均值时，存在期现套利窗口。需要独立计算器支持多维度基差（现货-永续、期权合成-现货、跨交易所基差）。

**上游数据依赖**: Binance Spot 现货价格（已有 `BinanceSpotFetcher`，需确保 BTC/ETH 现货 OHLCV 被采集）

新建文件:
- `processors/basis_calculator.py` — 基差计算处理器

实现内容:
```
class BasisCalculator:
    # 1. 现货-永续基差 (Spot-Perp Basis)
    basis_spot_perp = perp_mark_price - spot_price
    basis_pct = basis_spot_perp / spot_price * 100

    # 2. 期权合成基差 (Synthetic Basis)
    # 合成多头 = Long Call + Short Put (同strike同expiry)
    synthetic_price = call_mid - put_mid + strike * discount_factor
    basis_synthetic = perp_mark_price - synthetic_price

    # 3. 跨交易所基差
    basis_cross_exchange = perp_price_exchange_A - perp_price_exchange_B

    # 4. 基差年化收益率 (Annualized Basis)
    annualized_basis = basis_pct * (365 / days_to_expiry)

    输出: {timestamp, symbol, basis_type,
           spot_price, perp_price, basis, basis_pct,
           annualized_basis, days_to_expiry}
    存储: data/processed/basis/{symbol}_basis.parquet
```

**关键设计决策**:
- 输入来自多个采集器（spot OHLCV + perp mark_price + options mid），因此放在 Phase 2 作为处理器
- 合成基差需要 ATM strike 的 Call/Put pair，依赖 Task 1.4 主力映射
- 基差时序数据直接驱动资金费率套利策略的入场/出场信号

依赖: T1.2 (标记价格), T1.4 (主力映射), T1.5 (期权 bid/ask), BinanceSpotFetcher (现货数据)

#### Task 2.5: 截面数据时间对齐器 `processors/time_aligner.py`
**目标**: 将不同频率的数据源（期权低频 ~1min/次，永续高频 ~1s/次）对齐为统一时间截面

**背景**: 期权+永续组合策略需要将期权 Greeks、永续价格、资金费率等多源数据合并为一个策略切片。但各数据源采集频率不同：期权 Ticker ~1s（但仅对主力合约采集时实际可能更稀疏）、永续 OHLCV 1min/1h、资金费率 8h、Greeks 日级。直接按时间戳 inner join 会导致大量记录丢失，必须使用 as-of join 向后填充。

新建文件:
- `processors/time_aligner.py` — 时间对齐工具

实现内容:
```
class TimeAligner:
    # 核心方法: as-of join (pandas merge_asof)
    # 将低频数据向后填充到高频时间轴上
    # 例如: 期权 Greeks (1min) 对齐到 永续 ticker (1s)

    def align_to_target(
        self,
        target_df: pd.DataFrame,     # 高频目标时间轴 (如永续 1s ticker)
        source_df: pd.DataFrame,     # 低频数据源 (如期权 Greeks 1min)
        on: str = 'timestamp',       # 时间列
        direction: str = 'backward', # as-of 方向: backward=前值填充
        tolerance: int = 60000,      # 容差: 最多回溯 60s
        columns: list = None         # 需要对齐的列
    ) -> pd.DataFrame

    # 策略切片构建
    def build_strategy_slice(
        self,
        base_timestamps: pd.Series,  # 基准时间轴
        data_sources: Dict[str, pd.DataFrame],  # {name: DataFrame}
        tolerance_ms: Dict[str, int] = None     # 每个源的容差
    ) -> pd.DataFrame:
        """
        示例输入:
          base_timestamps = 永续 1s ticker 的时间戳序列
          data_sources = {
              'options_greeks': df_greeks,      # 1min 频率
              'funding_rate': df_funding,       # 8h 频率
              'spot_price': df_spot,            # 1min 频率
              'margin_params': df_margin,       # 1d 频率
          }
          tolerance_ms = {
              'options_greeks': 60000,    # 1min
              'funding_rate': 28800000,   # 8h
              'spot_price': 60000,        # 1min
              'margin_params': 86400000,  # 24h
          }

        输出: 统一时间轴的宽表 DataFrame
        """

    # 数据新鲜度标记
    # 每个对齐的列增加 _age_ms 后缀，标记数据实际时间戳与目标时间戳的差值
    # 例如: greeks_delta_age_ms = 30000 表示 Greeks 数据滞后 30s
    # 策略可据此决定是否信任该数据点
```

**关键设计决策**:
- 使用 `pandas.merge_asof` 实现，性能优于循环
- 每个 `_age_ms` 字段让策略层自行决定数据新鲜度阈值
- 默认 tolerance 按数据源频率的 2 倍设置，避免使用过旧数据
- 输出为宽表（列数 = Σ 各源字段数），便于策略直接消费

依赖: 无外部依赖，纯 pandas 操作；但实际数据流依赖 Phase 1 全部模块的输出

#### Task 2.6: 存储管线与生命周期管理 `storage/chunked_buffer.py` + `scripts/prune_cloud_data.sh`
**目标**: 面向生产环境（4 vCPU / 8 GB RAM / 75 GB NVMe）的存储优化，解决三个核心问题：内存安全、磁盘限制、高效回测 I/O。

**背景**: 云端采集节点资源受限。1 秒快照 × ~1700 期权合约 = ~150K 条/分钟，直接逐条写 Parquet 会产生海量小文件且拖垮 I/O；8 GB 内存下大数据集 merge_asof 有 OOM 风险；75 GB NVMe 在持续采集下约 2-3 周即可写满。

新建文件:
- `storage/chunked_buffer.py` — 分块缓冲写入引擎
- `scripts/prune_cloud_data.sh` — 云端数据定时清理脚本

实现内容:
```
# ── Part A: Chunked 分块缓冲写入 ──

class ChunkedBuffer:
    """
    内存中积攒一定量数据后再 Flush 到 Parquet。
    避免海量小文件 + 减少 I/O 次数 + 防止 8GB 内存 OOM。
    """

    def __init__(
        self,
        max_rows: int = 100_000,        # 每 10 万条 Flush 一次
        max_memory_mb: int = 200,        # 或内存超过 200MB 时 Flush
        flush_interval_sec: int = 300,   # 或最长 5 分钟强制 Flush
    ):
        self._buffer: Dict[str, pd.DataFrame] = {}  # key -> 累积 DataFrame
        self._row_counts: Dict[str, int] = {}

    def append(self, key: str, df: pd.DataFrame):
        """追加数据到缓冲区，满足任一阈值时自动 Flush"""
        if key not in self._buffer:
            self._buffer[key] = df
        else:
            self._buffer[key] = pd.concat(
                [self._buffer[key], df], ignore_index=True
            )
        self._row_counts[key] = len(self._buffer[key])

        if self._should_flush(key):
            self.flush(key)

    def _should_flush(self, key: str) -> bool:
        """任一条件满足即触发 Flush"""
        if self._row_counts[key] >= self.max_rows:
            return True
        if sys.getsizeof(self._buffer[key]) >= self.max_memory_mb * 1024 * 1024:
            return True
        return False

    def flush(self, key: str = None):
        """将缓冲区数据写入 Parquet (追加模式 + 去重)"""
        if key:
            self._write_parquet(key, self._buffer.pop(key))
        else:
            for k in list(self._buffer.keys()):
                self._write_parquet(k, self._buffer.pop(k))

    def _write_parquet(self, key: str, df: pd.DataFrame):
        """写入 Hive 分区格式的 Parquet (见 Part C)"""
        ...

# ── Part B: 云端数据定时清理 (Prune) ──
# 配合 Crontab 使用，每天凌晨执行

# scripts/prune_cloud_data.sh:
#   #!/bin/bash
#   # 云端仅保留最近 14 天的热数据
#   # 超过 14 天的数据文件由 rclone/rsync 同步到本地归档后删除
#   KEEP_DAYS=14
#   DATA_DIR="/opt/crypto-data"
#
#   # 1. 同步到本地归档服务器 (rclone)
#   rclone sync $DATA_DIR local-archive:/archive/crypto-data/ \
#       --transfers 4 --checkers 8
#
#   # 2. 删除云端超过 KEEP_DAYS 的数据
#   find $DATA_DIR -name "*.parquet" -mtime +$KEEP_DAYS -delete
#
#   # 3. 清理空目录
#   find $DATA_DIR -type d -empty -delete
#
#   # 4. 输出磁盘使用报告
#   df -h $DATA_DIR >> /var/log/data-prune.log

# Crontab 配置:
# 0 3 * * * /opt/crypto-data/scripts/prune_cloud_data.sh

# ── Part C: Hive 分区存储格式 ──

# 存储路径规范 (V3.0):
#   data/{exchange}/{data_type}/date={YYYY-MM-DD}/{symbol}.parquet
#
# 示例:
#   data/deribit/tickers/date=2026-05-07/BTC-28MAR26-60000-C.parquet
#   data/deribit/quotes/date=2026-05-07/BTC-PERPETUAL.parquet
#   data/binance/funding/date=2026-05-07/BTCUSDT.parquet
#   data/deribit/options_greeks/date=2026-05-07/BTC-28MAR26-60000-C.parquet
#   data/processed/basis/date=2026-05-07/BTC_basis.parquet
#
# 优势:
#   1. 按日期分区 → prune 时 find -mtime 精确匹配
#   2. pandas.read_parquet 支持分区过滤:
#      pd.read_parquet("data/deribit/quotes/",
#          filters=[("date", ">=", "2026-04-01")])
#   3. PySpark/DuckDB 等引擎原生支持 Hive 分区扫描
#   4. rclone/rsync 同步时可按日期目录增量传输

# Parquet 文件优化参数:
#   - 压缩: ZSTD (比 Snappy 更高压缩率，CPU 开销可接受)
#   - Row Group 大小: 128 MB (平衡读取并发和内存占用)
#   - 列类型: 使用精确类型 (float64 而非 float32, int64 时间戳)
```

**关键设计决策**:
- 三重 Flush 触发条件（行数/内存/时间），确保数据不丢失
- Hive 分区格式 `date=YYYY-MM-DD` 是业界标准，兼容 pandas/PySpark/DuckDB
- ZSTD 压缩比 Snappy 节省 ~30% 磁盘空间，在 NVMe 上 CPU 开销可忽略
- 云端 14 天保留 + rclone 同步：本地拥有全量历史，云端仅保留热数据
- 磁盘预算估算: 14天 × ~2GB/天 ≈ 28GB，75GB NVMe 富余 ~47GB 给系统和日志

依赖: 替代现有 `storage/parquet_store.py` 的直接使用，所有写入改走 `ChunkedBuffer`

#### Task 3.1: 策略数据管线 `pipeline/strategy_pipeline.py`
**目标**: 按策略需求编排数据采集流程

新建文件:
- `pipeline/strategy_pipeline.py` — 策略管线
- `pipeline/__init__.py`
- `pipeline/strategy_configs.py` — 策略数据需求配置

实现内容:
```
class StrategyDataPipeline:
    - 策略注册: 每个策略声明其数据需求
    - 调度引擎: 根据策略优先级编排采集顺序
    - 数据依赖: 自动解析采集依赖图
    - 输出: 按策略组织的数据集

    策略配置示例:
    Short Strangle + 永续对冲:
        需要: options_ticker(BTC), options_greeks, perp_price(BTC), funding_rate
        频率: 1s (实盘), 1h (回测)
        优先级: P0
```

#### Task 3.2: 统一运行入口 `run_strategy_data.py`
**目标**: 替代分散的运行脚本，提供统一 CLI

新建文件:
- `run_strategy_data.py` — 统一入口

实现内容:
```
python run_strategy_data.py --mode daily --strategies all
python run_strategy_data.py --mode daily --strategies short_strangle
python run_strategy_data.py --mode backfill --days 90 --strategies funding_arb
python run_strategy_data.py --mode validate --check-gaps --check-outliers
```

#### Task 3.3: 配置统一化 `config_strategy.yaml`
**目标**: 合并 config.yaml + config_options.yaml + 新增策略配置

---

### Phase 4: 测试与验证

#### Task 4.1: 单元测试 (覆盖率 >= 90%)
- `tests/test_funding_rate.py` — 资金费率采集
- `tests/test_mark_price.py` — 标记价格
- `tests/test_risk_free_rate.py` — 无风险利率
- `tests/test_main_contract.py` — 主力映射
- `tests/test_quote_fetcher.py` — L1 报价采集
- `tests/test_margin_params.py` — 保证金参数
- `tests/test_gap_detector.py` — 间隙检测
- `tests/test_outlier_filter.py` — 异常过滤
- `tests/test_vol_surface.py` — 波动率曲面
- `tests/test_basis_calculator.py` — 基差计算
- `tests/test_time_aligner.py` — 时间对齐
- `tests/test_strategy_pipeline.py` — 管线集成

#### Task 4.2: 集成测试
- 端到端采集 15 分钟验证
- 数据质量 KPI 检查脚本

---

## 四、文件清单

### 新建文件 (~25 个)
| 文件路径 | 用途 | Phase |
|----------|------|-------|
| `fetchers/funding_rate.py` | 资金费率采集 | 1 |
| `fetchers/mark_price.py` | 标记价格采集 | 1 |
| `fetchers/risk_free_rate.py` | FRED 无风险利率 (含日历 ffill) | 1 |
| `fetchers/quote_fetcher.py` | L1 报价/买卖盘采集 (WS+REST 双模) | 1 |
| `fetchers/ws_orderbook.py` | WebSocket 增量订单簿引擎 | 1 |
| `fetchers/margin_params.py` | 保证金参数采集 | 1 |
| `utils/interpolation.py` | 样条插值工具 | 1 |
| `utils/main_contract.py` | 主力合约映射 | 1 |
| `storage/chunked_buffer.py` | 分块缓冲写入 + Hive 分区 | 2 |
| `processors/__init__.py` | 处理模块入口 | 2 |
| `processors/gap_detector.py` | 数据间隙检测 | 2 |
| `processors/outlier_filter.py` | 异常过滤 | 2 |
| `processors/vol_surface.py` | 波动率曲面 | 2 |
| `processors/basis_calculator.py` | 基差计算器 | 2 |
| `processors/time_aligner.py` | 截面时间对齐器 | 2 |
| `scripts/prune_cloud_data.sh` | 云端 14 天数据清理脚本 | 2 |
| `pipeline/__init__.py` | 管线模块入口 | 3 |
| `pipeline/strategy_pipeline.py` | 策略数据管线 | 3 |
| `pipeline/strategy_configs.py` | 策略需求配置 | 3 |
| `run_strategy_data.py` | 统一运行入口 | 3 |
| `config_strategy.yaml` | 策略管线配置 | 3 |
| `tests/test_quote_fetcher.py` | 报价采集测试 | 4 |
| `tests/test_margin_params.py` | 保证金参数测试 | 4 |
| `tests/test_basis_calculator.py` | 基差计算测试 | 4 |
| `tests/test_time_aligner.py` | 时间对齐测试 | 4 |
| `tests/test_chunked_buffer.py` | 缓冲写入测试 | 4 |
| `tests/test_strategy_integration.py` | 集成测试 | 4 |

### 修改文件 (~6 个)
| 文件路径 | 变更 | Phase |
|----------|------|-------|
| `fetchers/__init__.py` | 导出新模块 (funding_rate, mark_price, quote_fetcher, ws_orderbook, margin_params) | 1 |
| `storage/parquet_store.py` | 重构为 Hive 分区格式 + 接入 ChunkedBuffer | 1 |
| `options_collector.py` | 集成无风险利率到 Greeks 计算、使用 quote_fetcher 的 bid/ask | 1 |
| `requirements.txt` | 新增 scipy, websockets>=12.0, zstd | 1 |
| `config.yaml` | 新增 funding_rate/mark_price/quotes/margin/ws 配置 | 3 |
| `deribit-options-data-collector/` | 独立子项目的 WS 客户端对接 ws_orderbook.py | 1 |

---

## 五、依赖关系

```
Phase 0 (基础设施) — 优先于一切:
  T2.6 ChunkedBuffer + Hive 分区 + prune 脚本 ── 所有写入模块的基础

Phase 1 (基础数据层) — 6 个模块可并行 (依赖 T2.6 的存储接口):
  T1.1 资金费率 ──────── 依赖 T2.6 (存储接口)
  T1.2 标记价格 ──────── 依赖 T2.6 (存储接口)
  T1.3 无风险利率 ────── 依赖 T2.6 (存储接口)
  T1.4 主力合约映射 ──── 依赖 T2.6 (存储接口)
  T1.5 L1 报价/买卖盘 ── 依赖 T2.6 (存储接口) + websockets 库
  T1.6 保证金参数 ────── 依赖 T2.6 (存储接口)

Phase 2 (质量层) — 依赖 Phase 1:
  T2.1 间隙检测 ──────── 依赖 Phase 1 输出数据
  T2.2 异常过滤 ──────── 依赖 Phase 1 输出数据
  T2.3 波动率曲面 ────── 依赖 T1.3 (利率), T1.4 (主力)
  T2.4 基差计算器 ────── 依赖 T1.2 (标记价格), T1.4 (主力映射),
                          T1.5 (期权 bid/ask), BinanceSpotFetcher (现货)
  T2.5 时间对齐器 ────── 无硬依赖 (纯 pandas 操作),
                          但实际数据流依赖 Phase 1 全部模块

Phase 3 (编排层) — 依赖 Phase 1 + 2:
  T3.1 策略管线 ──────── 依赖全部 Phase 1/2 模块
  T3.2 统一入口 ──────── 依赖 T3.1
  T3.3 配置统一 ──────── 依赖 T3.1

Phase 4 (测试): 伴随开发
```

---

## 六、风险评估

### 6.1 硬件资源风险 (4 vCPU / 8 GB RAM / 75 GB NVMe)

| 风险 | 等级 | 缓解措施 | 状态 |
|------|------|----------|------|
| 磁盘写满导致采集中断 | HIGH | prune 脚本 14 天自动清理 + rclone 同步到本地 | T2.6 已规划 |
| 内存 OOM (高频数据缓冲) | HIGH | ChunkedBuffer 200MB 上限 + 三重 Flush 触发 | T2.6 已规划 |
| WS 连接过多导致 CPU 满载 | MEDIUM | 最大 6 个 WS 连接 + 非主力降级 REST | T1.5 已规划 |
| 多采集器并发导致内存超限 | MEDIUM | 全局内存预算分配: 缓冲 200MB + 进程开销 2GB + 预留 6GB | 配置层控制 |
| NVMe I/O 瓶颈 (高频 Flush) | LOW | ZSTD 压缩减少 I/O + Hive 分区减少文件数 | T2.6 已规划 |

### 6.2 数据源风险

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| FRED API 限流 (120 req/h) | MEDIUM | 本地缓存 + 日历 ffill + 降级到固定利率 |
| FRED 周末/节假日无数据 | LOW | ffill 机制自动沿用前一个交易日利率 |
| Deribit funding_rate API 不返回历史 | MEDIUM | 使用 Binance 历史数据 + 当前的实时数据 |
| WS 断连导致数据间隙 | MEDIUM | 心跳检测 30s + 自动重连 + REST 快照校准 |
| 期权 bid/ask 频繁为零（深度不足） | MEDIUM | 标记 no_bid/no_ask，回测使用 mark_price 降级 |
| 保证金参数需认证 API | MEDIUM | 降级到 Deribit 公开阶梯保证金表硬编码 |

### 6.3 系统集成风险

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| 波动率曲面计算复杂度 | LOW | 先实现 ATM IV + Skew，后扩展 |
| 测试覆盖率不达标 | LOW | 边开发边测试，确保每个模块 >= 80% |
| Hyperliquid funding rate 格式差异 | LOW | 统一抽象层处理格式差异 |
| merge_asof 大数据集内存压力 | LOW | 按日分块处理，避免全量内存加载 |
| rclone 同步中断导致云端数据堆积 | LOW | prune 脚本仅在 rclone 成功后删除 + 告警 |

### 6.4 资源预算参考

```
云端 75 GB NVMe 预算分配:
├── 系统 + Python 环境        ~10 GB
├── 数据热存储 (14 天)         ~28 GB  (14天 × ~2GB/天)
├── 日志 + 监控                ~2 GB
├── 缓冲 + 临时文件            ~5 GB
└── 安全裕量                   ~30 GB

8 GB RAM 预算分配:
├── Python 进程基础            ~500 MB
├── WS 连接 (6个)              ~100 MB
├── ChunkedBuffer 缓冲         ~200 MB (硬上限)
├── pandas DataFrame (处理)    ~1-2 GB (按需分块)
├── OS + 系统                  ~2 GB
└── 安全裕量                   ~3 GB
```

---

## 七、验收标准

| 验收项 | 条件 | 验证方法 |
|--------|------|----------|
| 资金费率可采集 | BTC/ETH 跨 3 交易所 | `pytest tests/test_funding_rate.py` |
| Greeks 有效率 >= 95% | Black-Scholes 补算后 | 数据质量脚本 |
| 主力映射正确 | 与 Deribit 网页端对比 | 单元测试 |
| 波动率曲面可用 | ATM IV + Skew 输出 | Parquet 验证 |
| 期权 bid/ask 可采集 | WS 订阅主力合约，bid/ask 非零率 >= 90% | `pytest tests/test_quote_fetcher.py` |
| WS 断连自动恢复 | 模拟断连后 30s 内自动重连 + REST 校准 | 单元测试 (mock WS) |
| 保证金参数可获取 | IM/MM 参数非空 | `pytest tests/test_margin_params.py` |
| 基差计算正确 | spot-perp basis 与手动计算偏差 < 0.1% | `pytest tests/test_basis_calculator.py` |
| 时间对齐无丢失 | as-of join 后记录数 == 目标时间轴行数 | `pytest tests/test_time_aligner.py` |
| 无风险利率日历完整 | 非交易日 ffill 后无空值，365 天连续 | `pytest tests/test_risk_free_rate.py` |
| Hive 分区格式正确 | `pd.read_parquet` 可按 date 分区过滤 | `pytest tests/test_chunked_buffer.py` |
| ChunkedBuffer 内存安全 | 缓冲区不超过 200MB 上限 | 压力测试 |
| 云端磁盘不溢出 | prune 脚本执行后磁盘使用 < 50% | 运维验证 |
| 策略管线运行 | `run_strategy_data.py --mode daily` | 端到端测试 |
| 测试覆盖率 >= 90% | pytest --cov | CI 脚本 |
| 数据间隙 < 1% | 15 分钟采集验证 | 间隙检测报告 |

---

## 八、建议实施顺序

**Phase 0 (Day 1)**: 存储基础设施 (阻塞后续所有模块)
- T2.6 ChunkedBuffer + Hive 分区 + prune 脚本 (所有写入的基石)

**Phase 1 (Day 2-4)**: 并行开发 6 个基础数据模块
- 优先 P0: T1.5 L1 报价/WS 订阅 (最复杂，提前启动) + T1.6 保证金参数
- 优先 P0: T1.1 资金费率 + T1.2 标记价格 (策略直接依赖)
- 其次: T1.3 无风险利率 (含日历 ffill) + T1.4 主力映射

**Phase 2 (Day 5-7)**: 数据处理增强
- T2.1 间隙检测 → T2.2 异常过滤 (数据质量基础)
- T2.4 基差计算器 (依赖 T1.2 + T1.4 + T1.5)
- T2.5 时间对齐器 (纯 pandas，可独立先行)
- T2.3 波动率曲面 (依赖 T1.3 + T1.4，最后实现)

**Phase 3 (Day 8-9)**: 统一管线
- T3.1 策略管线 → T3.2 统一入口 → T3.3 配置

**Phase 4 (伴随)**: 测试
- 每个模块完成后立即编写测试
- T2.6 ChunkedBuffer 压力测试 (模拟 24h 连续写入)
