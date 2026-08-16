# 双核驱动 0-1DTE 策略 — 数据采集需求文档（Binance 获取路线）

> 本文档定义 `dual_core_0dte` 策略所需的最小数据集（字段 / 粒度 / 历史深度），并给出
> **从 Binance 免费历史数据**获取每一项的具体方法、端点与可行性结论。
>
> **使用场景**：先用 Binance 历史数据试跑策略回测，验证 alpha 后再决定是否购买/自采 Deribit 期权数据。

---

## 1. 策略对数据的依赖（一句话）

- **Alpha 择时核**：用期权 Greeks（theta/IV）+ 已实现波动率（RV）预测当日胜率。
- **Hybrid Kelly-VIX 仓位核**：用 Garman-Klass RV（永续 1min K 线）→ 蒙特卡洛 → 凯利仓位，再用 **DVOL 百分位**缩放。
- **结构腿**：0-1DTE **期权**（看跌价差 / 铁蝴蝶），需要期权链的 mid/bid/ask/深度/OI/IV。
- **结算**：到期按指数 30min TWAP 或 `estimated_delivery_price` 结算。

→ 因此数据分三层：**①期权链（核心，最贵）②标的+永续+DVOL（RV/结算/仓位）③宏观（利率）**。

---

## 2. 必需数据分级

### Tier 1 — 必需（缺一项策略跑不起来）

| # | 数据 | 最低粒度 | 策略用途 |
|---|------|---------|---------|
| 1 | **期权链快照**（mid/bid/ask + `mark_iv` + `open_interest` + bid/ask size） | 每日 08:00 UTC 1 张快照（研究最小）| 选 0-1DTE 合约、定价、算权利金、Greeks 反推、流动性过滤 |
| 2 | **BTC/ETH 标的价**（指数/现货） | 1 分钟 | moneyness、ITM/OTM 判定、**到期结算 TWAP** |
| 3 | **永续 1min OHLC** | 1 分钟连续 | **Garman-Klass 已实现波动率**（仓位核命脉）|
| 4 | **DVOL**（或可自算 ATM IV 百分位替代） | 日频收盘 | **DVOL-rank 凯利仓位缩放** |

### Tier 2 — 强烈建议（影响回测保真度）

| 数据 | 粒度 | 价值 |
|------|------|------|
| 期权 top-of-book 深度（bid_size/ask_size） | 进场快照 | 成交可行性过滤（避免选死合约）|
| 期权逐日 OI / volume | 日频 | 流动性排序 |

### Tier 3 — 免费 / 自算（不要花钱买）

| 数据 | 处理 |
|------|------|
| 无风险利率 `r` | **FRED 免费**（DGS10 / SOFR）|
| 波动率面 skew / butterfly / term_structure | 从期权链全 strike 自算 |
| `iv_rank` 百分位 | 滚动自算（策略已改用 DVOL 百分位）|
| 资金费率 funding | 仅作特征，权重低 |
| 多档 L2 订单簿 | **策略只需 top-of-book，不需要 L2** |
| 逐笔 trades | 策略固定时点进场，用不上 |

### 历史深度要求

- 最少 **60–90 天**连续：覆盖 20 天 Alpha 预热窗 + 30 天 DVOL-rank 窗 + OOS。
- 永续 1min K 线必须**无缺口连续**（GK RV 对缺失敏感）。

---

## 3. Binance 可行性矩阵（诚实结论）

| 策略需求 | Binance 能否提供 | 来源 | 判定 |
|----------|----------------|------|------|
| BTC/ETH 标的价（现货） | ✅ 完全可 | Spot K 线 | 直接用 |
| 永续 1min OHLC | ✅ 完全可 | USDM 永续 K 线 | 直接用（GK RV 原料）|
| 永续标记价 / 资金费率 | ✅ 完全可 | markPriceKlines / fundingRate | 直接用 |
| 到期结算价 | ✅ 可替代 | 1min 现货指数自算 TWAP | 直接用 |
| **DVOL** | ❌ 无等价物 | — | **必须自算 ATM IV 百分位替代** |
| **期权链（Deribit 同等）** | ⚠️ **结构性不同** | Binance Options（USDT 结算）| **见 §5 风险，非平替** |
| 无风险利率 | — | FRED（非 Binance）| 免费 |

> **关键认知**：Binance Options ≠ Deribit Options。详见 §5。Binance 能**完全免费**覆盖第 ②层
> （标的+RV+结算），但策略核心的**期权链**是最大风险点，需先用 §5 的方法做可行性验证。

---

## 4. Binance 获取方案

### 4.1 现货 + 永续（免费，可直接替代）— `data.binance.vision`

Binance 公开历史数据仓库，**无需 API key**，按日/月 zip 打包，适合批量回填。

**基础 URL**：`https://data.binance.vision`

| 数据 | 路径模板（每日）|
|------|----------------|
| 现货 K 线 | `/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{YYYY-MM-DD}.zip` |
| USDM 永续 K 线 | `/data/futures/um/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip` |
| 永续标记价 K 线 | `/data/futures/um/daily/markPriceKlines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip` |
| 永续指数价 K 线 | `/data/futures/um/daily/indexPriceKlines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip` |
| 永续 top-of-book tick | `/data/futures/um/daily/bookTicker/{SYMBOL}/{SYMBOL}-bookTicker-{DATE}.zip` |
| 资金费率 | `/data/futures/um/monthly/fundingRate/{SYMBOL}/{SYMBOL}-fundingRate-{YYYY-MM}.zip`（月文件）|

- `SYMBOL`：`BTCUSDT` / `ETHUSDT`
- `INTERVAL`：`1m`（GK RV 用）、`1h` 等
- 月文件把 `daily` 换成 `monthly`、日期换 `YYYY-MM`。
- K 线 zip 内为 CSV，列固定：`openTime, open, high, low, close, volume, closeTime, ...`。

**REST 备选**（单点查询，有权重限制）：
- 现货 K 线：`GET https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=...&limit=1000`
- 永续 K 线：`GET https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m`
- 资金费率：`GET https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT`

### 4.2 Binance Options（USDT 结算期权，部分替代）— `vapi.binance.com`

**Base URL**：`https://vapi.binance.com`（European 期权，标的 BTCUSDT/ETHUSDT）

| 用途 | 端点（参考，需对照官方文档核实最新版）|
|------|--------------------------------------|
| 期权合约规格（strike/expiry/标的）| `GET /vapi/v1/optionInfo` |
| 24h ticker（last/mark/bid/ask/qty/OI）| `GET /vapi/v1/ticker?underlying=BTCUSDT` |
| 标记价 + IV | `GET /vapi/v1/mark` |
| 订单簿（深度）| `GET /vapi/v1/depth?symbol=...` |
| 持仓量 OI | `GET /vapi/v1/option/openInterest?underlyingAsset=BTC` |
| **历史波动率/标记价序列**（含 IV）| `GET /vapi/v1/option/history?underlyingAsset=BTC&beginTime=...&endTime=...&period=1H` |
| 历史成交 | `GET /vapi/v1/historicalTrades?symbol=...&startTime=...&limit=...`（需 API key）|

> ⚠️ **Binance Options 不直接返回 Greeks**（delta/gamma/...）。需用 `mark` 返回的 IV + strike + T + 标的
> 自己用 BS 模型反推（项目内 `ares/backtest/strategies/black_scholes.py` 已有现成实现）。
>
> ⚠️ Binance Options **无 `data.binance.vision` 批量历史下载**，只能 REST 轮询，且历史深度/权重受限。
> 先用 `/vapi/v1/option/history` 探测能拿到多久的历史。

### 4.3 DVOL（Binance 无）→ 自算替代

```
DVOL_rank(t) = percentileofscore( 每日入场 ATM_IV 的近 30 天序列, 今日 ATM_IV )
ATM_IV       = 当日 08:00 UTC 快照中，最接近标的价格的 call/put 的 mark_iv
```

策略已设计为兼容此替代（DVOL-rank 窗 30 天）。只要拿到期权链 IV 序列即可自算。

### 4.4 无风险利率（非 Binance，免费）

FRED REST：`https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=...&file_type=json`
（或直接下载 CSV）。加密回测常用 `r=0`，长日期期权才需真实利率。

---

## 5. Binance Options 的结构性风险（务必先读）

策略原设计基于 **Deribit 反向（币本位）期权**。改用 Binance Options 需注意：

| 维度 | Deribit（原设计）| Binance Options |
|------|------------------|-----------------|
| 结算币种 | BTC（币本位）| **USDT**（现金交割）|
| 标的 | Deribit BTC 指数 | BTCUSDT 现货指数 |
| 行权方式 | European | European ✅ |
| 流动性 | 深，0-1DTE 丰富 | **明显更薄** → 权利金更薄、费用问题更严重 |
| Greeks | 交易所直出 | **需自算** |
| 历史数据 | 自采/厂商 | REST 轮询，深度受限 |

**payoff 公式需重写**：Deribit 币本位 `payoff_btc = max(0,K-S)/S`；Binance USDT 结算 `payoff_usdt = max(0,K-S)`。
策略 `structures.py` / `backtest.py` 的结算逻辑需相应分支。

**建议验证步骤**（按顺序，逐步投入）：
1. 先拉 **现货 + 永续 1min**（免费、确定能拿到），验证 **Alpha 核 + GK RV + 仓位核** 的纯波动率/择时部分能否工作。
2. 再用 `/vapi/v1/option/history` 探测 Binance Options 历史深度；若能覆盖 ≥60 天且 IV 可信，
   做期权腿回测（结算逻辑改 USDT）。
3. 若 Binance Options 历史不足或流动性太差 → 期权腿**仍需 Deribit**（自采或购买），
   仅标的/RV 部分用 Binance。

---

## 6. 文件落盘约定（对齐现有 collector 目录结构）

按现有 `data/options+futures_archive_*/` 目录命名，Binance 数据建议：

```
data/binance_archive_YYYYMMDD/
├── spot_klines/        BTCUSDT_YYYY-MM-DD.parquet   # 现货 1m/1h K
├── perp_klines/        BTCUSDT_YYYY-MM-DD.parquet   # USDM 永续 1m K（GK RV 原料）
├── mark_price/         BTCUSDT_YYYY-MM-DD.parquet   # 永续标记价 1m
├── funding_rate/       BTCUSDT_YYYY-MM-DD.parquet   # 资金费率
├── options_snapshot/   BTCUSDT_YYYY-MM-DD.parquet   # Binance Options 快照（08:00 UTC）
├── options_history/    BTCUSDT_YYYY-MM-DD.parquet   # /vapi/v1/option/history 轮询
└── index_price/        BTCUSDT_YYYY-MM-DD.parquet   # 现货/指数价（结算 TWAP）
```

- 每文件一日，列名与现有 collector 对齐（`timestamp/ms UTC`、`symbol`）。
- 期权快照列参考 Deribit：`timestamp, instrument_name, underlying_price, strike, expiry, time_to_expiry_years, option_type, mark_iv, delta, gamma, vega, theta, mid_price, bid_price, ask_price, bid_size, ask_size, open_interest, volume_24h`。

---

## 7. 批量下载脚本骨架（Python）

```python
import requests, zipfile, io, pandas as pd
from datetime import date, timedelta

VISION = "https://data.binance.vision"

def fetch_binance_klines(symbol, kind, interval, day):
    """kind: 'spot' | 'um'(usdm futures); interval: '1m'|'1h'; day: 'YYYY-MM-DD'"""
    if kind == "spot":
        url = f"{VISION}/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{day}.zip"
    else:  # usdm futures
        url = f"{VISION}/data/futures/um/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{day}.zip"
    r = requests.get(url, timeout=30)
    if r.status_code == 404:
        return None  # 当天无数据（未来日期/停盘）
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f, header=None)
    cols = ["openTime","open","high","low","close","volume","closeTime",
            "quoteVol","trades","takerBuyBase","takerBuyQuote","ignore"]
    df = df.iloc[:, :12]; df.columns = cols
    df["openTime"] = pd.to_datetime(df["openTime"], unit="ms", utc=True)
    return df

def fetch_options_snapshot(underlying="BTCUSDT"):
    """Binance Options 当前快照（REST）。历史需轮询 /vapi/v1/option/history。"""
    r = requests.get("https://vapi.binance.com/vapi/v1/ticker",
                     params={"underlying": underlying}, timeout=30)
    return pd.DataFrame(r.json())

if __name__ == "__main__":
    day = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    spot = fetch_binance_klines("BTCUSDT", "spot", "1m", day)
    perp = fetch_binance_klines("BTCUSDT", "um", "1m", day)
    print(spot.head(), perp.head())
```

> 拉取顺序建议：**①spot 1m + perp 1m（确定免费可拿到）→ ②funding/markPrice → ③options_history 探测 → ④optionInfo+ticker 全链快照**。

---

## 8. 路线图与决策点

```
[Step 1] 拉 Binance 现货+永续 60-90 天 1m 数据（免费，必成）
    └─ 验证 GK RV 计算、DVOL-rank 自算、Alpha 择时（用 ATM IV 当特征）
[Step 2] 探测 Binance Options /vapi/v1/option/history 历史深度
    ├─ 够 60 天且 IV 可信 → [Step 3a] 全 Binance 回测（结算改 USDT）
    └─ 不足/流动性差    → [Step 3b] 期权腿转 Deribit（自采或购买厂商）
[Step 3a/3b] 跑完整 dual_core_0dte 回测 → 报告 → go/no-go
```

---

## 9. 参考

- 策略设计与数据铁律：`docs/strategy_plan/20260714-gemini-3papers/双核驱动：实盘0-1DTE期权量化交易策略.md`
- 现有 Deribit 采集结构：`docs/data/DATA_QUALITY_VERIFICATION_RUNBOOK.md`
- BS/Greeks 现成实现：`ares/backtest/strategies/black_scholes.py`
- Binance 公开数据：<https://data.binance.vision>
- Binance Options API（核实端点）：<https://binance-docs.github.io/apidocs/voptions/en/>
- FRED：`DGS10` / `DGS30` / `SOFR`
