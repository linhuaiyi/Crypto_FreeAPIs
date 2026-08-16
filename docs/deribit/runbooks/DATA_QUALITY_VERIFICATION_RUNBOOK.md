# Data Quality Verification Runbook

## 1. Overview

本手册用于核验 `deribit-options-data-collector/data/` 下所有 parquet 数据的完整性、鲁棒性和策略适用性。

### Data Inventory

```
deribit-options-data-collector/data/
│
├── binance/                              ← Binance 数据 (REST 轮询)
│   ├── spot_price/      BTCUSDT, ETHUSDT       现货价格 (1s)
│   ├── mark_price/      BTCUSDT, ETHUSDT       标记价格 (30s, USDM 合约)
│   ├── funding_rate/    BTCUSDT                资金费率 (8h, USDM 合约)
│   └── basis/           BTC_USDT, ETH_USDT     基差 (10s, 现货 vs 合约)
│
├── deribit/                              ← Deribit 数据 (WS + REST)
│   ├── options_greeks/  BTC, ETH                期权链 Greeks (5s, 全合约)
│   ├── options_ticker/  BTC-PERPETUAL, ETH-PERPETUAL  永续合约 tick (WS)
│   ├── vol_surface/     BTC, ETH                波动率面 (10s)
│   ├── mark_price/      BTC-PERPETUAL, ETH-PERPETUAL  标记价格 (30s)
│   ├── funding_rate/    BTC-PERPETUAL           资金费率 (8h)
│   └── margin_params/   BTC, ETH                保证金参数 (24h)
│
├── hyperliquid/                          ← Hyperliquid 数据 (REST)
│   └── funding_rate/    BTC                    资金费率 (8h)
│
└── fred/                                 ← FRED 宏观数据 (REST)
    └── risk_free_rate/  USD                   无风险利率 (24h, DGS1MO~DGS30)
```

#### Deribit 合约数据说明

Deribit 采集的数据覆盖**期权**和**永续合约**两大类：

| 合约类型 | 采集内容 | 数据类型 |
|---------|---------|---------|
| **永续合约** (BTC-PERPETUAL, ETH-PERPETUAL) | 实时 bid/ask tick | `options_ticker` |
| **永续合约** | 标记价格 + 基差 | `mark_price` |
| **永续合约** | 资金费率 | `funding_rate` |
| **期权合约** (全行权价/到期日) | Delta, Gamma, Vega, Theta, IV 等 | `options_greeks` |
| **期权+永续** | ATM IV, 25d Skew/Butterfly, IV Rank | `vol_surface

### File Naming Convention

```
{exchange}/{data_type}/{symbol}_{YYYY-MM-DD}.parquet
```

---

## 2. Automated Verification (Recommended)

项目已有完整的自动化验证脚本 `scripts/verify_collected_data.py`，可一次性完成所有检查。

### 2.1 Basic Usage

```bash
cd d:/WORKSPACE/DataFetch/Crypto/FreeAPIs

# Verify data only (no log analysis)
python scripts/verify_collected_data.py --no-log

# Verify data + analyze collector log
python scripts/verify_collected_data.py

# Custom paths
python scripts/verify_collected_data.py --data-dir ./deribit-options-data-collector/data --log ./deribit-options-data-collector/logs/collector.log
```

### 2.2 Script Capabilities

`verify_collected_data.py` 自动执行以下 10 项检查：

| Section | Check | Description |
|---------|-------|-------------|
| 1. Log Analysis | Collector log | Scan for ERROR/WARNING, Greeks cycles, vol surface builds |
| 2. Source Verification | File existence | Auto-discover all `{exchange}/{type}/{symbol}_{date}.parquet`, check row counts, schemas, nulls |
| 3. Time Continuity | Timestamp gaps | Streaming timestamp analysis: monotonic order, duplicate detection, max gap reporting |
| 4. Coverage Analysis | Fill rate | Compute expected vs actual row counts based on known collection intervals |
| 5. Date Boundary | Timestamp vs filename | Verify each file only contains data from its expected date (UTC) |
| 6. Value Range Sanity | Price/IV/funding/spread | Non-positive prices, IV out of range, extreme funding rates, negative spreads |
| 7. Cross-Source Consistency | Binance vs Deribit | Mark price comparison, Greeks IV coverage per day |
| 8. Strategy Matrix | Strategy readiness | Cross-check data sources against strategy requirements (Short Strangle, etc.) |
| 9. Summary | Overall status | OK/WARNING/ERROR/MISSING counts per source |
| 10. File Inventory | Full file list | Every parquet file with row count and size |

### 2.3 Expected Intervals

脚本内置了各数据类型的预期采集频率：

| Source | Interval |
|--------|----------|
| binance/spot_price | 1s |
| binance/mark_price | 30s |
| binance/basis | 10s |
| binance/funding_rate | 8h |
| deribit/options_greeks | 5s (multi-instrument) |
| deribit/options_ticker | 1s |
| deribit/vol_surface | 10s |
| deribit/mark_price | 30s |
| deribit/funding_rate | 8h |
| deribit/margin_params | 24h |
| hyperliquid/funding_rate | 8h |
| fred/risk_free_rate | 24h |

---

## 3. Manual Checks

> **Note**: Sections 5-7 of the automated script now cover date boundary checks, value range sanity, and cross-source consistency. The following Python snippets are kept for ad-hoc / targeted investigation outside the automated flow.

### 3.1 Schema Verification

| Data Type | Required Columns |
|-----------|-----------------|
| `options_greeks` | timestamp, instrument_name, exchange, underlying_price, strike, expiry, time_to_expiry_years, option_type, iv, delta, gamma, vega, theta, rho, mid_price |
| `options_ticker` | timestamp, instrument_name, exchange, source, bid_price, ask_price, bid_size, ask_size, mid_price, spread |
| `vol_surface` | timestamp, symbol, atm_iv, skew_25d, butterfly_25d, iv_rank, quality |
| `mark_price` | timestamp, exchange, symbol, mark_price, index_price, basis |
| `funding_rate` | timestamp, exchange, symbol, funding_rate, mark_price, index_price |
| `margin_params` | timestamp, instrument_name, exchange, instrument_type, initial_margin_rate, maintenance_margin_rate |
| `spot_price` | timestamp, exchange, symbol, price, bid_price, ask_price |
| `basis` | timestamp, symbol, basis_type, spot_price, perp_price, basis, basis_pct, annualized_basis |

### 3.2 Date Boundary Check

Each `{symbol}_{date}.parquet` should only contain data from that date (UTC).

```python
import os, pyarrow.parquet as pq
from datetime import datetime, timezone

DATA_DIR = "deribit-options-data-collector/data"
for root, dirs, files in os.walk(DATA_DIR):
    for f in sorted(files):
        if not f.endswith(".parquet"): continue
        path = os.path.join(root, f)
        expected_date = f.split("_")[-1].replace(".parquet", "")
        pf = pq.read_table(path)
        ts = pf.column("timestamp").to_pylist()
        for t in [ts[0], ts[-1]]:
            actual = datetime.fromtimestamp(t / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if actual != expected_date:
                print(f"MISMATCH: {path} has {actual}, expected {expected_date}")
                break
```

### 3.3 Value Range Sanity

```python
import os, pyarrow.parquet as pq

DATA_DIR = "deribit-options-data-collector/data"
for root, dirs, files in os.walk(DATA_DIR):
    for f in sorted(files):
        if not f.endswith(".parquet"): continue
        path = os.path.join(root, f)
        rel = os.path.relpath(path, DATA_DIR).replace("\\", "/")
        df = pq.read_table(path).to_pandas()

        for col in ["mark_price", "price", "mid_price", "bid_price", "ask_price", "underlying_price"]:
            if col in df.columns and (df[col] <= 0).any():
                print(f"NON-POSITIVE {col}: {rel} -> {(df[col] <= 0).sum()} rows")

        if "iv" in df.columns:
            bad = ((df["iv"] <= 0) | (df["iv"] > 1000)).sum()
            if bad: print(f"IV OUT OF RANGE: {rel} -> {bad} rows")

        if "funding_rate" in df.columns and (df["funding_rate"].abs() > 0.1).any():
            print(f"EXTREME FUNDING: {rel} -> {(df['funding_rate'].abs() > 0.1).sum()} rows >10%")

        if "spread" in df.columns and (df["spread"] < 0).any():
            print(f"NEGATIVE SPREAD: {rel} -> {(df['spread'] < 0).sum()} rows")
```

---

## 4. Cross-Source Consistency

> **Note**: Cross-source mark price comparison and Greeks coverage analysis are now automated in Section 7 of `verify_collected_data.py`. The following snippets are for ad-hoc investigation.

### 4.1 Mark Price Consistency

Binance and Deribit mark prices for BTC should track closely.

```python
import pyarrow.parquet as pq

DATA = "deribit-options-data-collector/data"
date = "2026-05-15"

binance = pq.read_table(f"{DATA}/binance/mark_price/BTCUSDT_{date}.parquet").to_pandas()
deribit = pq.read_table(f"{DATA}/deribit/mark_price/BTC-PERPETUAL_{date}.parquet").to_pandas()

binance["ts_min"] = (binance["timestamp"] // 60000) * 60000
deribit["ts_min"] = (deribit["timestamp"] // 60000) * 60000

merged = binance.merge(deribit, on="ts_min", suffixes=("_binance", "_deribit"))
merged["pct_diff"] = abs(merged["mark_price_binance"] - merged["mark_price_deribit"]) / merged["mark_price_binance"] * 100
print(f"Samples: {len(merged)}, Mean diff: {merged['pct_diff'].mean():.3f}%, Max diff: {merged['pct_diff'].max():.3f}%")
```

### 4.2 Greeks Coverage per Day

```python
import os, pyarrow.parquet as pq

DATA = "deribit-options-data-collector/data"
for f in sorted(os.listdir(f"{DATA}/deribit/options_greeks")):
    if not f.endswith(".parquet"): continue
    df = pq.read_table(f"{DATA}/deribit/options_greeks/{f}").to_pandas()
    iv_pct = (1 - df["iv"].isna().mean()) * 100
    print(f"{f}: {df['instrument_name'].nunique()} instruments, {len(df):,} rows, iv_coverage={iv_pct:.1f}%")
```

---

## 5. Strategy Readiness

### 5.1 Short Strangle Requirements

| Data | Source | Frequency | Check |
|------|--------|-----------|-------|
| Options chain (greeks) | Deribit | ~5s | All strikes/expiries present per snapshot |
| ATM IV + skew | Deribit vol_surface | ~10s | atm_iv > 0, quality == "good" |
| Mark price | Deribit | 30s | Non-null, reasonable range |
| Funding rate | Binance + Deribit | 8h | At least 1 record per 8h window |
| Risk-free rate | FRED | Daily | Rate > 0, reasonable range (0-15%) |

### 5.2 Strategy Validation

```python
import os, pyarrow.parquet as pq

DATA = "deribit-options-data-collector/data"

def validate_short_strangle(date_str):
    checks = {}

    greeks_path = f"{DATA}/deribit/options_greeks/BTC_{date_str}.parquet"
    if os.path.exists(greeks_path):
        df = pq.read_table(greeks_path).to_pandas()
        checks["greeks"] = {"rows": len(df), "instruments": df["instrument_name"].nunique(),
            "iv_coverage": f"{(1-df['iv'].isna().mean())*100:.1f}%",
            "ok": len(df) > 100 and df["iv"].notna().mean() > 0.5}
    else:
        checks["greeks"] = {"ok": False, "error": "missing"}

    vol_path = f"{DATA}/deribit/vol_surface/BTC_{date_str}.parquet"
    checks["vol_surface"] = {"ok": os.path.exists(vol_path) and pq.read_table(vol_path).num_rows > 100} if os.path.exists(vol_path) else {"ok": False}

    mp_path = f"{DATA}/deribit/mark_price/BTC-PERPETUAL_{date_str}.parquet"
    checks["mark_price"] = {"ok": os.path.exists(mp_path) and pq.read_table(mp_path).num_rows > 100} if os.path.exists(mp_path) else {"ok": False}

    fr_d = f"{DATA}/deribit/funding_rate/BTC-PERPETUAL_{date_str}.parquet"
    fr_b = f"{DATA}/binance/funding_rate/BTCUSDT_{date_str}.parquet"
    checks["funding_rate"] = {"ok": os.path.exists(fr_d) or os.path.exists(fr_b)}

    rfr = f"{DATA}/fred/risk_free_rate/USD_{date_str}.parquet"
    checks["risk_free_rate"] = {"ok": os.path.exists(rfr)}

    all_ok = all(v.get("ok", False) for v in checks.values())
    status = "PASS" if all_ok else "FAIL"
    print(f"[{status}] {date_str}: {checks}")
    return all_ok
```

---

## 6. Common Issues

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Missing dates | Server downtime or pull not executed | Check server logs, re-pull with `bash scripts/pull_data.sh YYYY-MM-DD` |
| Empty parquet (0 rows) | API returned no data | Check API status, normal for funding_rate (only 1-3 rows/day) |
| Null IV values | No mark_iv from Deribit for illiquid strikes | Filter out in strategy, expected for far OTM |
| Null mark_price in funding_rate | Binance funding_rate API doesn't return mark_price | Expected behavior, use separate mark_price source |
| Negative spread | Bid/ask data race condition | Filter spread >= 0 in strategy |
| Large price divergence | Different quote sources/timing | Use tolerance threshold (0.5-1%) |
| Greeks missing after restart | Buffer not flushed before shutdown | Check flush interval config (300s default) |
| Coverage < 50% | First day partial data (started mid-day) | Expected for first day of collection |
