# panel-collector — 统一 REST 批处理采集器

> 2026-08-26 合并而成：吸收 `ohlcv-collector`（REST 横截面面板）+ `binance-data-collector`（vision 归档下载器）。
> 两者本就同机制（HTTP 请求-响应、日频批处理、无状态），合并消除"采集器散乱"与同流双源。

## 三模式

```bash
python3 panel-collector/launch.py daily      # 日频防过期（cron 挂这个）
python3 panel-collector/launch.py backfill --days 90 [--symbols BTC,ETH] [--types ohlcv,funding]
python3 panel-collector/launch.py verify     # 自校验，退出码 1 = 有 MISSING/STALE（接告警）
```

## 数据类型 × 数据源

| data_type | binance 系 | hyperliquid / deribit | 窗口限制 |
|---|---|---|---|
| ohlcv（K线多周期） | **vision 归档**（daily T-1 + backfill；官方终稿） | REST（无归档，daily 必跑） | REST ≈3 天窗 |
| funding | vision 月档（backfill）+ REST（daily） | REST（HL 窗口约 30 天） | HL 无归档 |
| oi（持仓量） | REST `/futures/data/openInterestHist` | — | **⚠ 硬窗 30 天，过期即焚** |
| sentiment（多空情绪） | REST `topLongShortAccountRatio` + `takerlongshortRatio` | — | **⚠ 同 30 天** |

## 实测教训（2026-08-27 首跑）

1. **fapi klines REST 对本机 403**（区域性封锁；同域的 fundingRate/指标族反而通）——所以 binance 系 K 线一律走 vision CDN，REST 仅兜底
2. **vision T-1 日档在 UTC 03:14 尚未发布**——VPS cron 排 **08:10 Berlin（=06:10 UTC）**；⚠ VPS crontab 是 Berlin 时间，别按 UTC 排（踩过：05:10 Berlin=03:10 UTC 过早）
3. usdm base_url 必须含 `/fapi/v1`（截断=REST 兜底 403，曾误判为区域封锁）
3. Binance 指标族时间戳**本来就是毫秒**（首次实现误乘 1000 已修）；taker 端点字段是 `buySellRatio` 非 `longShortRatio`
4. 裸请求交易所必带 UA（403/451 假象）——已在 Session 层统一设置

## 去重规则（与湖/v2 的关系）

- **deribit funding 不采**：湖真源 `raw.deribit.funding_rate`（v2 采集器）
- binance BTC/ETH funding 与 v2 `freeapis_v2` 段重叠：入湖时以 v2 为准；本采集器照采（供 14 符号横截面）
- BTC/ETH ohlcv 与 nc（`raw.binance.hive`）重叠：深度数据以湖为准，本采集器面板供横截面研究

## 落盘与部署位

- 落盘：`panel-collector/data/{exchange}/{SYM}_{slot}.parquet`（slot=周期|funding|oi|sentiment；原子写+timestamp keep='last' 去重）
- 部署位（规划）：VPS cron 日频（不可回补流不能依赖本机开机）→ lake_pull 拉取段 → 入湖（后续 F4 提案）
- 状态：**development**（2026-08-27 BTC 全类型实网首跑通过；全面板+cron 上线待部署批准）

## 前身（legacy，数据仍在原地待入湖决策）

- `../ohlcv-collector/`：2025-10-18→2026-08-16 十个月横截面（2.5G，frozen）
- `../binance-data-collector/`：2026-04→07 vision 归档（53M，tool，被本工具取代）
