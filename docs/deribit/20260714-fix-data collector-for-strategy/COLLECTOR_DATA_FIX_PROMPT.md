# Deribit 期权数据采集器 — 修复与扩展任务书（Handoff Prompt）

> 本文档为转交到「Deribit 期权数据采集器」独立项目的修改指令。请按本文档实施并在回传数据前完成第 4 节全部验收。背景：下游有一个"双核驱动 0-1DTE 期权量化策略"回测系统依赖本采集器的输出，当前产出的数据存在多处质量缺陷与字段缺口，已使下游策略被迫打大量补丁（本地 BS 重算 Greeks、用 atm_iv 代理 DVOL、用 perp mark 代替交割指数价等）。本任务从采集根源修复，使下游可去掉所有补丁。

---

## 0. 背景与目标

下游策略需要以下数据能力（当前缺失或损坏）：

1. **可信的 Greeks**（delta/gamma/vega/theta）——用于结构选腿与特征。
2. **DVOL 恐慌指数历史**——用于仓位管理的"VIX-rank 缩放"。
3. **真实标的指数价**——Deribit 期权按到期前 30 分钟（07:30–08:00 UTC）指数 TWAP 交割，必须用真指数价而非永续 mark。
4. **订单簿深度**（bid_size/ask_size）——评估 0-1DTE 深虚值腿的真实流动性与滑点。
5. **成交量与未平仓量**（volume / open_interest）——流动性过滤。
6. **连续可信的 iv_rank**——波动率历史百分位。

好消息：本采集器的 REST 客户端**已实现**了 DVOL / order_book / greeks / summary / index_price 等端点，但多数**从未被调用**或**取到后未持久化**。因此本任务以"接线 + 修 bug"为主，不是从零开发。

> 参考（本仓库内的副本）：`ref_docs/deribit-options-data-collector/`。其中 `src/deribit_options_collector/api/rest_client.py` 已含 `get_volatility_index_data`/`get_order_book`/`get_greeks`/`get_summary`/`get_index_price`；`models.py` 已含 `open_interest`/`volume_24h`/`index_price` 字段；`storage/parquet_store.py` 已能写这些列。请在你们真实项目里定位对应实现。

---

## 1. 现状问题（已验证的数据症状，非推测）

对实际产出数据 `data/options+futures/deribit/` 的实测：

| # | 症状 | 实测证据 |
|---|------|---------|
| Q1 | **存量 Greeks 全损坏** | ATM call（iv=43）stored `delta=1.0`、`gamma=1.9e-28`；正确 BS delta≈0.5、gamma≈1e-4。多数行 delta 卡在 0/1/−1，gamma/vega 趋近 0 |
| Q2 | **`vol_surface.iv_rank` 是离散假值** | 全量 distinct 仅 `{40, 60, 80, 100}`，非连续 0–100 百分位 |
| Q3 | **`mark_price.index_price` 100% NULL** | deribit mark_price 的 index_price 列全空 |
| Q4 | **`options_ticker` 仅含永续** | instrument 只有 `BTC-PERPETUAL`/`ETH-PERPETUAL`，无个券期权；`bid_iv/ask_iv` 100% NULL |
| G1 | **无 DVOL 指数数据** | 全仓库无任何 dvol 文件/列 |
| G3 | **options_greeks 无订单簿深度** | schema 无 `bid_size`/`ask_size` |
| G4 | **无期权 volume / open_interest** | options_greeks 无成交量与持仓量 |
| G5 | **funding_rate 标的缺失** | deribit 仅 `BTC-PERPETUAL`（无 ETH）；binance 仅 `BTCUSDT`（无 ETH） |

> 可信字段（不要动）：`options_greeks.iv`（`iv_source='rest_api'`=Deribit 官方 mark_iv）、`mid_price`/`bid_price`/`ask_price`、`underlying_price`。

端点调用实测（在参考副本里）：

| 端点（REST 客户端已实现） | 实际被调用次数 | 后果 |
|---|---|---|
| `get_volatility_index_data`（DVOL） | 0 | 无 DVOL |
| `get_order_book`（深度） | 取了但未持久化 size | 无 bid_size/ask_size |
| `get_summary`（OI/volume） | 0 | 无 OI/volume |
| `get_index_price`（真指数） | 0 | index_price NULL |
| `get_greeks`（交易所 greeks） | 取了但被本地坏 GreeksProcessor 覆盖 | Greeks 损坏 |

---

## 2. 修复任务（按性价比排序）

### T1 修复 Greeks 损坏（修 Q1）—— 最高优先
- **症状定位**：产出 Greeks 的处理器（参考副本中为 `processors/greeks_processor.py::GreeksProcessor`，由 `launch.py::GreeksProcessorThread` 驱动）计算结果错误：ATM call delta=1.0、gamma≈1e-28。疑似 T 单位错误、S/K 比较反相、或 T→0 未做下限保护导致奇异。
- **修复方案（推荐）**：**优先采用交易所直出 Greeks**。Deribit `public/get_greeks`（参考副本 `rest_client.get_greeks`）返回含 `delta/gamma/vega/theta` 的官方值。将其作为主数据源写入；仅当交易所未返回时，才用**正确的 BS 实现**回退（且对 `T` 做下限保护，如 `T=max(T, 1/87600)`≈1h，防 gamma 溢出）。
- **禁止**：继续用当前坏的自算逻辑覆盖。若保留自算分支，必须通过第 4 节的 delta/gamma 数值校验。

### T2 采集 DVOL 指数（修 G1）—— 高
- **端点**：`public/get_volatility_index_data`（参考副本 `rest_client.get_volatility_index_data`），标的 `BTC`/`ETH`（Deribit DVOL）。
- **实现**：新增一个采集线程（cadence 10–60s），落盘到独立目录，例如 `deribit/dvol/BTC.parquet`、`deribit/dvol/ETH.parquet`。
- **字段**：见第 3 节契约。

### T3 采集真实标的指数价（修 Q3）—— 高
- **端点**：`public/get_index_price`（参考副本 `rest_client.get_index_price`）。
- **实现**：填充 `mark_price.index_price`（当前全 NULL）；或新增独立 `deribit/index_price/{BTC,ETH}.parquet`。**交割结算必须用此真指数**，不可用永续 mark 顶替（含基差）。

### T4 持久化订单簿深度（修 G3）—— 中高
- **端点**：`public/get_order_book`（已调用但丢列）。
- **实现**：在 options 快照输出中补 `bid_size`/`ask_size`（顶档）；若可行，额外存 2–3 档 L2（`bids`/`asks` 为 list 或宽表 `bid_1_price/bid_1_size/...`）。

### T5 采集成交量与未平仓量（修 G4）—— 中高
- **端点**：`public/get_summary`（参考副本 `rest_client.get_summary`，0 调用）。
- **实现**：在 options 快照补 `open_interest`、`volume_24h`、`volume`/`total_volume`。models 与 parquet_store 已支持这些字段（参考副本已确认），接上线即可。

### T6 修复 iv_rank（修 Q2）—— 中
- **症状**：`vol_surface.iv_rank` 仅取 `{40,60,80,100}`，来自 vol_surface builder（参考副本 `BasisVolProcessorThread`/`VolatilitySurfaceBuilder`）的离散化错误。
- **修复方案（二选一）**：
  - (a) 修 builder，使 iv_rank = 当前 atm_iv 在过去 N 天（默认 30）的连续百分位 `(iv-min)/(max-min)*100`，值域连续 0–100；
  - (b) 干脆**只存原始 `atm_iv`**（与历史序列），iv_rank 由下游自算。推荐 (a) 以减少下游负担。

### T7 写入校验门禁（防再产出坏数据）—— 中
- 在落盘前加校验，任一不通过则告警/丢弃该快照：
  - `iv ∈ (0, 1000)`；
  - Greeks 非奇异：`|delta| ≤ 1.0001`、`0 ≤ gamma < 1e6`、`vega`/`theta` 有限；
  - `index_price > 0`（若 T3 已接）；
  - DVOL `∈ (0, 400)` 且非空；
  - `mid_price/bid_price/ask_price ≥ 0` 且 `ask ≥ bid`。

### T8（可选）补全 ETH funding_rate、个券期权 ticker（修 Q4/G5）
- deribit/binance funding_rate 补 ETH；options_ticker 若需个券期权逐笔，按 instrument 采集（当前仅永续）。

---

## 3. 输出数据契约（下游期望的字段）

> 仅列出**需要新增/修复**的字段，已有可信字段保持不变。

### 3.1 `options_greeks`（每期权合约快照，~5s）
新增列：
| 字段 | 类型 | 来源 |
|------|------|------|
| `bid_size` | float | get_order_book 顶档买量 |
| `ask_size` | float | get_order_book 顶档卖量 |
| `open_interest` | float | get_summary |
| `volume_24h` | float | get_summary.stats.volume |
| `mark_iv` | float | 交易所 mark_iv（即当前可信的 `iv`，建议显式重命名/保留双列避免歧义） |

修复列（T1）：`delta`/`gamma`/`vega`/`theta` 改为交易所直出值（或正确 BS 回退），通过第 4 节校验。

### 3.2 新增 `deribit/dvol/{BTC,ETH}.parquet`
| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | int64 ms | 快照时间 |
| `symbol` | str | `BTC`/`ETH` |
| `dvol` | float | Deribit DVOL 指数值 |

### 3.3 `mark_price`
修复：`index_price` 由 `get_index_price` 填充，**非 NULL**。

### 3.4 `vol_surface`（T6）
`iv_rank` 改为连续 0–100 百分位（或保留原始 atm_iv 由下游算）。

---

## 4. 验收标准（回传数据前必须全部通过）

重新采集至少 **3 个连续自然日**的 BTC（与 ETH）数据，运行以下检查并附报告：

1. **Greeks 正确性**：取每日 ATM call（|K−S|/S < 1%），断言 `0.4 ≤ delta ≤ 0.65`、`gamma > 1e-6`、`vega > 0`、`theta < 0`；并抽样用独立 BS（scipy）反算，`|delta_stored − delta_bs| < 0.02`。
2. **DVOL**：`deribit/dvol/BTC.parquet` 存在，`dvol ∈ (0, 400)`，每分钟至少 1 条。
3. **index_price**：`mark_price.index_price` 非空率 > 99%，且 `|index_price − underlying_price|/underlying_price < 1%`（同源同刻应接近）。
4. **订单簿深度**：流动性较好（ATM 附近）的合约 `bid_size>0 且 ask_size>0` 占比 > 90%。
5. **OI/volume**：`open_interest` 与 `volume_24h` 非空率 > 95%。
6. **iv_rank**：distinct 值数量 > 20（即连续，非 {40,60,80,100} 四个离散值）。
7. **校验门禁**：T7 的校验日志显示无"丢弃/告警"激增（坏数据率 < 1%）。

参考值（便于自查 BS 正确性）：S=K=100, T=1y, r=0.03, σ=0.30 的 ATM call → delta≈0.596、gamma≈1.06、vega≈37.6（每 1 整单位 σ）、theta<0。

---

## 5. 注意事项

- **不要破坏已有可信字段**：`iv`、`mid_price`/`bid_price`/`ask_price`、`underlying_price` 的语义与单位保持不变。
- **两套入口的取舍**：参考副本里有 `launch.py`（线程式，实际在跑，但依赖一个 `processors/` 包）与 `src/deribit_options_collector/`（更干净的 pipeline 包）。请在**实际产出数据的那个入口**里修复；若两者并存，建议以 `src/` 这套为唯一基线统一维护，避免漂移。
- **单位约定**：明确文档化 `iv` 为百分数（如 55.0=55%）、`T` 为年、Greeks 的 vega 是"每 1 整单位 σ"还是"每 1% σ"——下游依赖这些约定，请在回传时一并说明。
- **向后兼容**：新增列不要删除或重命名已有列；若重命名 `iv`→`mark_iv`，请保留 `iv` 别名至少一个周期。
- **时间对齐**：各源（greeks/vol_surface/mark/dvol）尽量共用统一快照时间戳，便于下游 asof 对齐（理想：同一 ms）。

---

## 附：下游去掉补丁的对照（修复后预期）

| 下游当前补丁 | 修复后 |
|---|---|
| 本地 BS Brent 重算 Greeks（每快照） | 直接用 stored greeks |
| atm_iv 代理 DVOL | 用 `deribit/dvol` 真值 |
| perp mark 代替交割指数 | 用 `index_price` TWAP |
| 无 size → 用 `min_price` 粗滤流动性 | 用 `bid_size/ask_size` + OI/volume |
| 自算 iv_rank | 用 vol_surface.iv_rank（连续） |

完成后请回传：① 新 schema 样例（前若干行）；② 第 4 节验收报告；③ 单位/约定说明文档。
