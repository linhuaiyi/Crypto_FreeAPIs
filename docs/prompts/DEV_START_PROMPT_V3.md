# 开发启动 Prompt：期权+合约策略数据采集系统 V3.0

> 用途：在新 Claude Code 会话中粘贴此 prompt，启动 V3.0 计划的实施
> 生成日期：2026-05-07

---

## 粘贴以下内容到新会话

```
请阅读项目计划文档 `docs/PLAN_OPTIONS_PERP_DATA_SYSTEM.md`，然后按照其中的实施计划开始开发。

## 项目背景

这是一个加密货币期权+永续合约数据采集系统，位于 `d:\WORKSPACE\DataFetch\Crypto\FreeAPIs`。项目已有基础 OHLCV 采集能力（Binance/Deribit/Hyperliquid），现在需要扩展为支持 6 类期权+永续组合策略的完整数据采集系统。

部署环境：4 vCPU / 8 GB RAM / 75 GB NVMe 云端采集节点，架构为"云端采集 + 定期同步回本地归档"。

## 现有代码结构（已实现，可复用）

```
FreeAPIs/
├── fetchers/
│   ├── base.py              # BaseFetcher 抽象基类（限速、重试、backoff）
│   ├── binance.py           # Binance Spot + USDT-M OHLCV
│   ├── deribit.py           # Deribit 永续 OHLCV + DeribitOptionsFetcher
│   ├── deribit_options.py   # 期权 OHLCV（DeribitOptionsFetcher）
│   └── hyperliquid.py       # Hyperliquid 永续 OHLCV
├── models/ohlcv.py          # OHLCV dataclass
├── storage/parquet_store.py # Parquet 存储（去重、追加、统计）
├── utils/
│   ├── logger.py            # 日志
│   └── rate_limiter.py      # 令牌桶限速器
├── options_collector.py     # 独立期权采集脚本（含 Black-Scholes Greeks、IV、波动率曲面）
├── run.py                   # 主入口（backfill/daily/single 模式）
├── run_options.py           # 期权采集入口
├── config.yaml              # 主配置（4 交易所）
├── config_options.yaml      # 期权配置
└── deribit-options-data-collector/  # 高级期权采集子项目（asyncio + WS + SQLite + Prometheus）
```

关键模式：
- `BaseFetcher` 提供 `fetch_with_backoff()` 统一重试逻辑
- `ParquetStore` 提供 `save()` / `get_last_timestamp()` / `get_stats()`
- `OHLCV` dataclass 有 `to_dict()` / `from_dict()` 方法
- `options_collector.py` 中已有 `VolatilityCalculator`（Black-Scholes Greeks + Newton-Raphson IV）

## 开发计划（V3.0，详见 PLAN_OPTIONS_PERP_DATA_SYSTEM.md）

### 实施顺序（严格遵守）

**Phase 0 (最先)**: Task 2.6 — ChunkedBuffer + Hive 分区存储 + prune 脚本
- 这是所有后续模块的写入基础，必须最先完成
- 实现 `storage/chunked_buffer.py`：三重 Flush 触发（10万行/200MB/5min）
- Hive 分区路径：`data/{exchange}/{data_type}/date=YYYY-MM-DD/{symbol}.parquet`
- 实现 `scripts/prune_cloud_data.sh`：rclone 同步 + find -mtime +14 清理

**Phase 1 (可并行)**: 6 个数据采集模块
- T1.5 L1 报价采集（WS 优先 + REST 降级）— 最复杂，最先启动
  - 新建 `fetchers/ws_orderbook.py`：WS 增量订单簿引擎（心跳30s、自动重连、REST校准）
  - 新建 `fetchers/quote_fetcher.py`：双模采集器（WS主路径 + REST辅助路径）
  - WS 连接池：3x Deribit + 1x Binance + 1x Hyperliquid，每连接最多300频道
- T1.1 资金费率：`fetchers/funding_rate.py`（Binance/Deribit/Hyperliquid）
- T1.2 标记价格：`fetchers/mark_price.py`（mark_price + index_price）
- T1.3 无风险利率：`fetchers/risk_free_rate.py`（FRED API + 日历 ffill + 连续复利 + 样条插值）
  - 关键：FRED 周末/节假日无数据，必须 ffill 补全 365 天
  - 附带 `utils/interpolation.py` 样条插值工具
- T1.4 主力合约映射：`utils/main_contract.py`（OI 排序 + ATM 识别）
- T1.6 保证金参数：`fetchers/margin_params.py`（IM/MM + 降级到静态阶梯表）

**Phase 2**: 5 个数据处理模块
- T2.1 间隙检测：`processors/gap_detector.py`
- T2.2 异常过滤：`processors/outlier_filter.py`（Z-Score > 5 标记）
- T2.3 波动率曲面：`processors/vol_surface.py`（ATM IV + Skew + Term Structure）
- T2.4 基差计算器：`processors/basis_calculator.py`（现货-永续 + 合成 + 跨交易所）
- T2.5 时间对齐器：`processors/time_aligner.py`（pandas merge_asof + _age_ms 新鲜度标记）

**Phase 3**: 统一管线
- T3.1 策略管线 + T3.2 统一 CLI 入口 + T3.3 配置统一化

**Phase 4**: 测试覆盖 >= 90%

## 编码规范

- Python 3.11+，所有函数签名必须有 type hints
- 遵循 PEP 8，使用 dataclass / NamedTuple
- 每个模块完成后立即编写对应测试
- 文件不超过 800 行，函数不超过 50 行
- 不可变数据优先（frozen=True dataclass）
- 错误显式处理，不静默吞异常
- 日志用 logging 模块，不用 print()
- Parquet 压缩用 ZSTD

## 关键约束

1. **内存安全**：ChunkedBuffer 硬上限 200MB，全局内存预算 8GB
2. **磁盘安全**：Hive 分区 + 14 天 prune，75GB NVMe 预留 30GB
3. **WS 限流**：Deribit 单连接 300 频道，最大 6 个 WS 连接
4. **REST 限流**：Deribit 20 req/s，Binance 2800 req/min，FRED 120 req/h
5. **测试覆盖率**：>= 90%

## 开始指令

请先阅读 `docs/PLAN_OPTIONS_PERP_DATA_SYSTEM.md` 获取完整计划细节，然后从 **Phase 0 (Task 2.6 ChunkedBuffer)** 开始实施。实施前先用 TodoWrite 工具创建任务列表跟踪进度。每完成一个模块，立即编写测试并运行验证。
```
