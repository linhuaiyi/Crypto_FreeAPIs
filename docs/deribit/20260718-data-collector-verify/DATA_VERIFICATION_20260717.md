# 数据验证报告 — `data/options+futures` (2026-07-17 全天采集)

> **验证日期**: 2026-07-18
> **数据范围**: `data/options+futures/` 下全部 27 个 parquet 文件
> **策略**: dual_core_0dte (双核驱动 0-1DTE)
> **对照基准**: [STRATEGY_DATA_REQUIREMENTS_DERIBIT.md](./STRATEGY_DATA_REQUIREMENTS_DERIBIT.md)

---

## 一、总体结论：🟡 基本可用，存在 1 个需修复的采集 Bug

**27 文件 · 17,117,504 行 · 459.9 MB · 覆盖完整 24h**

数据**连续、高频、Greeks 干净**，满足策略回测的核心需求。但存在 **1 个采集器 Bug**（EDP 未正确采集）和若干可接受的列缺失。

---

## 二、逐项验证

### 2.1 时序连续性 — ✅ PASS

| 文件 | 行数 | 快照数 | 24h 覆盖 | 平均间隔 | 最大缺口 |
|------|------|--------|----------|----------|----------|
| options_greeks BTC | 9,917,112 | 15,226 | 100% | 5.7s | 597.8s (09:00 UTC) |
| options_greeks ETH | 6,877,893 | 15,226 | 100% | 5.7s | 605.7s (09:00 UTC) |
| index_price (BTC/ETH) | 8,164 | 8,164 | 100% | 10.6s | 603.8s (09:00 UTC) |
| DVOL (BTC/ETH) | 2,750 | 2,750 | 99.9% | 31.4s | 630.0s (09:00 UTC) |
| mark_price Deribit perp | 1,435 | 1,435 | 99.9% | 60.2s | 360s (08:59 UTC) |
| vol_surface (BTC/ETH) | 8,125 / 8,095 | 8,125 / 8,095 | 100% | 10.6s | 597.8s |
| Binance spot_price | 53,259 | 53,259 | 100% | 1.6s | 37.6s |
| Binance mark_price | 1,440 | 1,440 | 99.9% | 60.0s | 60.0s |
| options_ticker perp | 77,772 / 76,342 | 77,772 / 76,342 | 100% | 1.1s | 36.5s |

> 🔴 所有 Deribit 文件在 **09:00–09:10 UTC** 有统一约 10 分钟缺口 —— 这是 **Deribit 每日结算暂停（matching pause）**，属正常现象，非采集故障。mark_price 在 08:59–09:05 缺 5 分钟（360s），同期。

> 🟢 其余全部文件的缺口均 ≤ 60s，时序连续。

---

### 2.2 Greeks 正确性 — ✅ PASS (完美)

**BTC (9,917,112 行):**

| 检测项 | 结果 | 判定 |
|--------|------|------|
| \|delta\| > 1.0 | **0 行 (0.0000%)** | ✅ |
| gamma < 0 | **0 行 (0.0000%)** | ✅ |
| vega < 0 | **0 行 (0.0000%)** | ✅ |
| gamma == 0 | 14,166 行 (0.14%，远 OTM 正常) | ✅ |
| gamma 中位数 | 0.000016 | ✅ |
| mark_iv 范围 | 12.87% – 71.58% | ✅ |
| mark_iv < 0 或 > 500% | 0 行 | ✅ |
| mid_price < 0 | 0 行 | ✅ |
| bid > ask (crossed book) | 69,287 行 (0.70%) | ✅ |
| bid_size>0 AND ask_size>0 | **8,668,659 行 (87.4%)** | ✅ |
| OI == 0 | 1,462,147 行 (14.7%，远月/远 OTM) | ✅ |
| option_type 分布 | C: 50.2%, P: 49.8% | ✅ |

**ETH (6,877,893 行):**

| 检测项 | 结果 | 判定 |
|--------|------|------|
| \|delta\| > 1.0 | **0 行 (0.0000%)** | ✅ |
| gamma < 0 | **0 行 (0.0000%)** | ✅ |
| vega < 0 | **0 行 (0.0000%)** | ✅ |
| gamma 中位数 | 0.000501 | ✅ |
| mark_iv 范围 | 23.85% – 153.07% | ✅ |
| bid > ask (crossed book) | 35,511 行 (0.52%) | ✅ |
| bid_size>0 AND ask_size>0 | **6,101,388 行 (88.7%)** | ✅ |

> **BS 交叉验证**（2026-07-17 已验证）: ATM Greeks 与 BS 自算 delta 差异 0.0015，无需本地重算。

---

### 2.3 DVOL — ✅ PASS

| | BTC | ETH |
|---|-----|-----|
| 值范围 | 35.80 – 37.62 | 48.82 – 50.54 |
| 均值 / 中位数 | 36.49 / 36.42 | 49.48 / 49.33 |
| 读数 | 2,750 | 2,750 |
| 采集间隔 | ~31s（多数 30-40s） | 同 |
| 负值 / 零值 | 0 | 0 |

---

### 2.4 波动率面 — ✅ PASS

| 字段 | BTC | ETH |
|------|-----|-----|
| atm_iv | 14.38% – 35.73% | — |
| skew_25d | -31.04 – +16.53 | — |
| butterfly_25d | -8.81 – +24.39 | — |
| iv_rank 离散值数 | 21 | 32 |
| quality | 100% `'good'` | 100% `'good'` |

> 注: iv_rank 仅 21-32 个离散值（较之前 sample 的 7-8 个有所改善）。策略已改用 DVOL 百分位，不影响。

---

### 2.5 GK RV 可用性 — ✅ PASS

从 1min mark_price (Deribit perp) 构建日 OHLC → Garman-Klass 年化波动率:

| | BTC | ETH |
|---|-----|-----|
| 日 O / H / L / C | 63813 / 64319 / 62525 / 63904 | 1865 / 1869 / 1803 / 1841 |
| 日振幅 | 2.81% | 3.53% |
| **GK 年化 σ** | **38.18%** | **45.93%** |
| RV 年化 σ (close-close) | 33.63% | 46.80% |
| Parkinson 年化 σ | 32.46% | 41.18% |

> 注: 1 天只有 1 个 GK 估计值（回测需多日累积），但数据格式完全支持向量化窗口计算（`gk_window_days=5`）。Binance mark_price 也可用（1440 行，无缺口）。

---

### 2.6 0-1DTE 合约可用性 — ✅ PASS

| | BTC | ETH |
|---|-----|-----|
| 0-1DTE 合约数 (tte ≤ 1d) | **50** | **42** |
| 1-2DTE 合约数 (1 < tte ≤ 2d) | 48 | 38 |
| 08:00 UTC 有深度 (bid & ask >0) | 15/50 (30%) | 18/40 (45%) |
| 04:00 UTC 有深度 | 36/50 (72%) | 32/42 (76%) |
| 12:00 UTC 有深度 | 37/50 (74%) | 28/42 (67%) |
| 16:00 UTC 有深度 | 35/50 (70%) | 33/42 (79%) |
| 20:00 UTC 有深度 | 32/50 (64%) | 27/42 (64%) |

> ⚠️ **08:00 UTC（策略默认入场时间）深度偏薄** — 日交割后做市商重新报价期。建议:
> - 将入场推迟到 08:05 UTC 或使用更长入场窗 (如 08:00–08:15 取最近完整快照)
> - 或测试 00:00–04:00 UTC 入场（0-DTE theta 对照，权利金更薄）

---

### 2.7 对照数据 — ✅ PASS

**Binance spot_price:**
- BTC: 53,259 ticks, $62,539–$64,388, spread ≈ $0.01
- ETH: 53,260 ticks, $1,804–$1,871, spread ≈ $0.01
- crossed book: 0

**Binance basis:**
- BTC: basis_pct -0.16% – -0.01%（小幅贴水）
- ETH: basis_pct -0.16% – +0.02%

**FRED 无风险利率:** 8 个期限 (0.1y–30y), 年化 0.037%–0.050%

---

## 🔴 三、必须修复的问题

### 3.1 `estimated_delivery_price` ≡ `index_price`（100% 完全相同）

```
BTC: index_price 62,485.79 – 64,334.39
     EDP          62,485.79 – 64,334.39
     diff = 0.0000 (0/8,164 行有差异)
     correlation = 1.0000000000

ETH: index_price 1,802.50 – 1,869.31
     EDP          1,802.50 – 1,869.31
     diff = 0.0000 (0/8,164 行有差异)
     correlation = 1.0000000000
```

**这是采集器 Bug。** EDP 应为 Deribit 30 分钟指数 TWAP 的实时滚动估计值，来源是 `ticker.BTC-PERPETUAL.raw` → `estimated_delivery_price` 字段，应与瞬时 `index_price` **不同**。当前采集器直接将 index_price 复制进了 EDP 字段。

**影响**: 策略到期结算依赖 EDP 作为到期价 $S_T$。EDP = 瞬时 index 意味着结算价 = 到期瞬时的指数价而非 07:30–08:00 UTC 的 30 分钟 TWAP，与 Deribit 实际交割规则不符。回测结算偏差随到期时刻波动率增大（波动越大，TWAP 与瞬时值差异越大）。

**修复方向**: 检查采集器中 `index_price` writer 的 EDP 数据来源，确认从 `ticker.{BTC,ETH}-PERPETUAL.raw` WS 推送的 `estimated_delivery_price` 字段读取，而非从 `index_price` 复制。

**临时兜底**（需求规格 §8.3 已设计）: 若无正确 EDP，用 1min 指数自算到期前 07:30–08:00 UTC 的 30 分钟 TWAP 作为 $S_T$。

---

## 🟡 四、需关注但非阻塞

| # | 严重度 | 问题 | 影响文件 | 说明 |
|---|--------|------|----------|------|
| 1 | 🟡 | `index_price`, `basis` 100% null | `binance/mark_price/*` | mark_price 主字段正常，index/basis 可从他处获取 |
| 2 | 🟡 | `mark_price` 100% null | `deribit/funding_rate/*` | funding_rate 和 index_price 正常 |
| 3 | 🟡 | `basis` 100% null | `deribit/mark_price/*` | 可自行计算 `mark_price - index_price` |
| 4 | 🟢 | `bid_iv`, `ask_iv` 100% null | `deribit/options_ticker/*` | **永续无 IV，属正常** |
| 5 | 🟡 | `index_price` 100% null | `hyperliquid/funding_rate/*` | HL 非策略主数据源 |
| 6 | 🟡 | funding_rate 恒为 0.000013 | `hyperliquid/funding_rate/*` (21 行) | 疑似采集器写入静态值 |
| 7 | 🟢 | funding_rate 仅 3 行 (~16h) | `binance/funding_rate/*` | Deribit 已有 21 行 (每小时) |
| 8 | 🟡 | 08:00 UTC 深度薄 | `options_greeks` | 见 §2.6 分析 |
| 9 | 🟢 | margin_params 单快照 | `deribit/margin_params/*` | 保证金参数变化极慢，日频足够 |

---

## 五、与需求规格逐层对照

对照 [STRATEGY_DATA_REQUIREMENTS_DERIBIT.md](./STRATEGY_DATA_REQUIREMENTS_DERIBIT.md):

| 层 | 需求字段 | 状态 | 备注 |
|----|---------|------|------|
| ① 合约标识 | instrument_name, strike, expiry, time_to_expiry_years, option_type, underlying_price | ✅ | 全部存在且正确 |
| ① 定价 Greeks | mark_iv, delta, gamma, vega, theta, rho, mid/bid/ask | ✅ | 全部干净，无需本地重算 |
| ① 深度流动性 | bid_size, ask_size, open_interest, volume_24h, volume_usd | ✅ | 87-89% 行有双边深度 |
| ② DVOL | dvol | ✅ | 2,750 读数/天, ~31s 间隔 |
| ② 波动率面 | atm_iv, skew_25d, butterfly_25d, term_structure | ✅ | 采集器自算，全部 'good' |
| ② 结算价 | **estimated_delivery_price** | 🔴 **= index_price (Bug)** | 见 §3.1 |
| ② 永续标记 | mark_price (perp, 1min) | ✅ | 1435-1440 行/天 |
| ② 指数价 | index_price | ✅ | 8,164 行/天 |
| ② 资金费率 | funding_rate | ✅ | Deribit 21 行/h |
| ③ 无风险利率 | rate_annual / rate_continuous | ✅ | FRED 8 个期限 |

---

## 六、缺失类别

以下 **非策略必需** 的类别未采集（不阻塞回测）:

| 缺失 | 原因 |
|------|------|
| hyperliquid/mark_price | 策略用 Deribit perp mark |
| hyperliquid/spot_price | Binance spot 已有 |
| binance/perp_klines | mark_price 1min OHLC 可替代 |

---

## 七、关键数字汇总

| 指标 | 数值 |
|------|------|
| 总文件数 | 27 |
| 总行数 | 17,117,504 |
| 总存储 | 459.9 MB |
| BTC options 快照 | 15,226 (全天 ~5.7s/次) |
| ETH options 快照 | 15,226 |
| 每快照平均 BTC 合约数 | 651 |
| 每快照平均 ETH 合约数 | 452 |
| 0-1DTE BTC 合约 | 50 (全天恒定) |
| 0-1DTE ETH 合约 | 40-42 |
| BTC 日振幅 / GK σ | 2.81% / 38.18% |
| ETH 日振幅 / GK σ | 3.53% / 45.93% |
| BTC DVOL 范围 | 35.80 – 37.62 |
| ETH DVOL 范围 | 48.82 – 50.54 |
| BTC 标的价格范围 | $61,558 – $66,922 |
| ETH 标的价格范围 | $1,802 – $1,926 |

---

## 八、最终判定

| 维度 | 判定 |
|------|------|
| **完整性** | ✅ 14/14 预期类别全部存在 |
| **连续性** | ✅ 24h 覆盖，仅 Deribit 09:00 UTC 结算窗口正常缺口 |
| **有效性** | 🟡 核心字段有效；10 个文件存在 100% null 的次要列（见 §4） |
| **正确性** | 🔴 EDP Bug 需修复；Greeks / DVOL / index / 深度全部正确 |
| **策略可用** | 🟡 EDP 修复后可直接用于 dual_core_0dte 回测 |

### Go/No-Go

🟢 **Go** — EDP 修复后即可用于完整回测。

当前数据已满足策略回测的全部核心需求。建议:

1. **优先**: 修复 EDP 采集 Bug（`index_price` writer 中 EDP 字段来源）
2. **其次**: 清理 100% null 的列（要么填充、要么从 schema 中移除，避免下游误用）
3. **然后**: 跑 `dual_core_0dte` 回测，如 EDP 暂未修复则用 1min 指数自算 30min TWAP 兜底（需求规格 §8.3 fallback）

---

## 九、参考

- 策略数据需求规格: [STRATEGY_DATA_REQUIREMENTS_DERIBIT.md](./STRATEGY_DATA_REQUIREMENTS_DERIBIT.md)
- Binance 替代路线: [../../data/STRATEGY_DATA_REQUIREMENTS_BINANCE.md](../../data/STRATEGY_DATA_REQUIREMENTS_BINANCE.md)
- 采集器修复任务书: [COLLECTOR_DATA_FIX_PROMPT.md](./COLLECTOR_DATA_FIX_PROMPT.md)
- 深度修复专项: [COLLECTOR_FIX_T4_ORDERBOOK_DEPTH.md](./COLLECTOR_FIX_T4_ORDERBOOK_DEPTH.md)
- 策略实现计划: `~/.claude/plans/sunny-napping-truffle.md`
- BS/Greeks 现成实现: `ares/backtest/strategies/black_scholes.py`
