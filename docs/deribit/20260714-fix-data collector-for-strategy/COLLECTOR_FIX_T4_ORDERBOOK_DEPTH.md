# Deribit 期权数据采集器 — 追加修复：订单簿深度覆盖（T4）

> 本文档为 `COLLECTOR_DATA_FIX_PROMPT.md` 的**追加专项**，只针对上一轮修复后**唯一未达标**的一项：订单簿深度（bid_size/ask_size）覆盖。其余 T1/T2/T3/T5/T6/T7 已全部通过验收，无需再动。

---

## 0. 背景

上一轮修复后采集了 1 小时 sample（2026-07-15 02:50–03:50 UTC）回传验证。`options_greeks` 已新增 `bid_size/ask_size/open_interest/volume_24h/mark_iv` 等列，但**深度列覆盖严重不足**，未达验收的 ">90% 流动性合约两边报价" 标准。

下游策略用 bid/ask **价格**（全量都有）做成交模拟、用 OI 做流动性过滤，**当前不阻断**；但 bid/ask **深度（size）**稀疏会导致**滑点/成交量真实性不足**——无法判断某个行权价挂单量是否够吃下目标张数。本修复把深度从"采样 ~34 个"提升到"全量覆盖"。

---

## 1. 已验证症状（实测，非推测）

数据：`deribit/options_greeks/BTC_last1h.parquet`，单快照统计：

| 指标 | 实测 |
|------|------|
| 单快照 instrument 总数 | **590** |
| 有 `bid_size>0 且 ask_size>0` 的合约数 | **仅 ~34**（5.8% 行） |
| `bid_price==0` / `ask_price==0` 占比 | **0.0% / 0.0%**（价格全量都有） |
| 多快照采样，有 size 的合约数 | 稳定 34–36，**不随时间轮转覆盖全量** |
| 有 size 的合约交集/并集 | 28 / 42（小范围漂移，非系统轮转） |
| ATM±2% 区两边 size>0 占比 | 53%（验收要求 >90%） |
| 0-1DTE 合约两边 size>0 占比 | 17% |

**关键判读**：`bid_price/ask_price` 对全部 590 个合约都有值（0% 为空），只有 `size` 只有 34 个有值。说明价格来自 `get_book_summary`/ticker（批量、全量），而**深度（size）只对 ~34 个合约取到了**。

---

## 2. 根因（高置信）

`public/get_order_book` 是**单合约 REST 调用**，受 Deribit 限流（约 20 req/s）。在 5.6s 的 options 快照周期内，REST 只能轮询到 ~34 个合约的 order book，其余取不到 → 深度成了"固定小批采样"而非"全量"。价格之所以全量，是因为它走的是批量端点。

---

## 3. 修复方案（推荐 WebSocket，替代 REST 轮询）

Deribit 公共 WebSocket 提供**增量推送**的盘口通道，可一次性订阅全部期权合约，不受单合约 REST 限流：

### 方案 A（推荐）：订阅 `quote.{instrument}` 通道
- `quote.{instrument_name}` 每次推送**最优买/卖价 + 挂单量**（top-of-book 的 price+amount），正是 `bid_size/ask_size` 所需，且**最轻量**。
- 订阅：批量 `public/subscribe`，每批 ≤100 个 instrument，全量 ~600 合约分 ~6 批（Deribit 允许并发订阅批次）。
- 收到推送时，按 `instrument_name + timestamp` 落到对应期权快照行，更新 `bid_size/ask_size`。

### 方案 B：订阅 `book.{instrument}.100ms`（depth=1）
- 若还需要多档 L2 深度，订阅 `book.{instrument_name}.100ms`，参数 `depth=1` 取顶档；需多档则 `depth=N`。
- 比 `quote` 重，但能拿到多档（`bid_1/bid_2/...`）。

### 方案 C（兜底，若不便改 WS）：REST 轮询 + 标注
- 维持 REST `get_order_book`，但**显式标注"深度为采样"**：新增列 `depth_source ∈ {full, sampled, missing}`，并**轮转覆盖**（每个周期覆盖不同的 ~34 个，若干周期内拼全）。
- 下游据此知道哪些行的 size 可信。**不推荐**，仅作退路。

> 优先 **方案 A**：改动最小、满足下游对顶档 bid/ask size 的需求。

---

## 4. 输出契约（与现有 schema 兼容）

`options_greeks` 现有 `bid_size/ask_size` 列**保留不变**，要求改为由 WS `quote` 通道填充，使：

- 每个快照中，**所有有报价的合约**（`bid_price>0 或 ask_price>0`）的 `bid_size/ask_size` 均 > 0（允许单边为 0，但顶档有量时应非 0）；
- （可选）新增 `bid_iv/ask_iv` 由 WS 同步填充（当前 options_ticker 的 bid_iv/ask_iv 100% NULL，可顺带修）。

不新增文件，不动其它列。

---

## 5. 验收标准（重新采集 ≥1h 后全部通过）

1. 单快照内，`bid_price>0 或 ask_price>0` 的合约中，`bid_size>0 或 ask_size>0` 占比 **> 90%**（当前 5.8%）。
2. ATM±2% 区，两边 `bid_size>0 且 ask_size>0` 占比 **> 90%**（当前 53%）。
3. 0-1DTE 合约两边 size>0 占比 **> 70%**（当前 17%）。若真实市场确无盘口，记录该合约 `depth_source=missing`，不算失败。
4. `bid_size/ask_size` 与 Deribit 网页盘口同刻抽样核对，误差 < 5%。
5. WS 断线/重连有日志，重连后深度恢复。

---

## 6. 注意事项

- **不要影响价格列**：`bid_price/ask_price/mid_price` 当前全量可信，保持其来源（批量端点）不变；size 改由 WS 填充。
- **时间对齐**：WS 推送的 timestamp 与 options 快照周期对齐（同 ms 或最近），便于下游 asof 关联。
- **单位**：`bid_size/ask_size` 的单位（合约张数 vs 标的数量）请在回传时文档化——Deribit `quote.amount` 通常是**合约张数**（每张 BTC 期权 = 1 BTC 标的）。
- **优先级**：此项**非阻断**（下游用价格+OI 已可运行），但影响滑点真实性。若资源紧张，可排在"延长历史天数"之后。

---

## 附：验收脚本（供对方自查，Python）

```python
import pyarrow.parquet as pq, numpy as np
df = pq.read_table("deribit/options_greeks/BTC_last1h.parquet").to_pandas()
df = df[df.mid_price > 0]
snap = df.timestamp.median()
s = df[(df.timestamp > snap-10000) & (df.timestamp <= snap+10000)]
quoted = s[(s.bid_price>0) | (s.ask_price>0)]
both_size = s[(s.bid_size>0) & (s.ask_size>0)]
print("有报价合约数:", quoted.instrument_name.nunique())
print("两边size>0合约数:", both_size.instrument_name.nunique(),
      f"({len(both_size)/len(s)*100:.1f}%行, 要求>90%有报价即有size)")
atm = s[(s.strike-s.underlying_price).abs()/s.underlying_price < 0.02]
atm_both = atm[(atm.bid_size>0)&(atm.ask_size>0)]
print(f"ATM±2% 两边size>0: {len(atm_both)/max(len(atm),1)*100:.1f}% (要求>90%)")
```
