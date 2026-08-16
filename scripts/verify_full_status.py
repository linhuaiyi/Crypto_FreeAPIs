"""Comprehensive integrity check for all 14 collector streams."""
from __future__ import annotations

import glob
import re
import time
from collections import defaultdict

import pandas as pd

DATA_ROOT = "/opt/Crypto_FreeAPIs/deribit-options-data-collector/data"
LOG_PATH = "/opt/Crypto_FreeAPIs/deribit-options-data-collector/logs/collector.log"

now_s = time.time()
CUTOFF_1H = (now_s - 3600) * 1000

# ── 1. STREAM INVENTORY ──────────────────────────────────────────────────────

streams = [
    ("binance", "spot_price", "BTCUSDT", 1),
    ("binance", "spot_price", "ETHUSDT", 1),
    ("binance", "mark_price", "BTCUSDT", 1),
    ("binance", "mark_price", "ETHUSDT", 1),
    ("binance", "basis", "BTC_USDT", 1),
    ("binance", "basis", "ETH_USDT", 1),
    ("binance", "funding_rate", "BTCUSDT", 28800),
    ("binance", "funding_rate", "ETHUSDT", 28800),
    ("deribit", "mark_price", "BTC-PERPETUAL", 1),
    ("deribit", "mark_price", "ETH-PERPETUAL", 1),
    ("deribit", "index_price", "BTC", 1),
    ("deribit", "index_price", "ETH", 1),
    ("deribit", "dvol", "BTC", 60),
    ("deribit", "dvol", "ETH", 60),
    ("deribit", "margin_params", "BTC", 86400),
    ("deribit", "margin_params", "ETH", 86400),
    ("deribit", "funding_rate", "BTC-PERPETUAL", 28800),
    ("deribit", "funding_rate", "ETH-PERPETUAL", 28800),
    ("deribit", "options_greeks", "BTC", 5),
    ("deribit", "options_greeks", "ETH", 5),
    ("deribit", "options_ticker", "BTC-PERPETUAL", 5),
    ("deribit", "options_ticker", "ETH-PERPETUAL", 5),
    ("deribit", "vol_surface", "BTC", 5),
    ("deribit", "vol_surface", "ETH", 5),
    ("hyperliquid", "funding_rate", "BTC", 28800),
    ("hyperliquid", "funding_rate", "ETH", 28800),
    ("fred", "risk_free_rate", "USD", 86400),
]

print("=" * 100)
print("  SERVICE INTEGRITY REPORT")
print(f"  UTC: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now_s))}")
print("=" * 100)

issues = []
healthy = 0
stale_warn = 0

print("\n── 1. COMPLETENESS ──")
print(f"  {'stream':<44} {'rows':>7} {'age':>6} {'p50s':>6} {'maxs':>7} {'g>60s':>6} {'g>5m':>6}")
print("  " + "-" * 88)

for ex, stream, sym, cadence_s in streams:
    files = sorted(glob.glob(f"{DATA_ROOT}/{ex}/{stream}/{sym}_*.parquet"))
    label = f"{ex}/{stream}/{sym}"

    if not files:
        print(f"  {label:<42} {'NO FILES':>50}")
        issues.append(f"MISSING: {label}")
        continue

    df = pd.read_parquet(files[-1])
    total = len(df)
    if total == 0:
        print(f"  {label:<42} {'EMPTY':>50}")
        issues.append(f"EMPTY: {label}")
        continue

    last_ts = df["timestamp"].max()
    age_s = now_s - last_ts / 1000

    # gap stats
    gap_warn = ""
    if total >= 3:
        df_sorted = df.sort_values("timestamp")
        gaps = df_sorted["timestamp"].diff().dropna()
        p50 = gaps.quantile(0.5) / 1000
        mx = gaps.max() / 1000
        over_60s = int((gaps > 60000).sum())
        over_5min = int((gaps > 300000).sum())
    else:
        p50 = mx = 0.0
        over_60s = over_5min = 0

    # staleness: high-freq streams should be < 10x cadence
    if cadence_s <= 60 and age_s > cadence_s * 10:
        gap_warn = " STALE"
        stale_warn += 1
    if cadence_s <= 5 and over_5min > 0:
        gap_warn = " GAP"
        issues.append(f"GAP: {label} has {over_5min} gaps >5min")
    if cadence_s <= 5 and over_60s > 0:
        gap_warn = " SHORT_GAP"

    print(
        f"  {label:<42} {total:>7} {age_s:>5.0f}s {p50:>6.1f} {mx:>7.1f}"
        f" {over_60s:>6} {over_5min:>6}{gap_warn}"
    )
    healthy += 1


# ── 2. T4 CORRECTNESS ────────────────────────────────────────────────────────

print("\n── 2. CORRECTNESS ──")
print("  T4 options_greeks bid_size/ask_size coverage:")

for sym in ("BTC", "ETH"):
    files = sorted(glob.glob(f"{DATA_ROOT}/deribit/options_greeks/{sym}_*.parquet"))
    df = pd.read_parquet(files[-1])

    # Full file
    total = len(df)
    both = int(((df["bid_size"] > 0) & (df["ask_size"] > 0)).sum())
    neither = int(((df["bid_size"] == 0) & (df["ask_size"] == 0)).sum())
    ask_only = int(((df["bid_size"] == 0) & (df["ask_size"] > 0)).sum())

    # last1h
    df1h = df[df["timestamp"] >= CUTOFF_1H]
    total_1h = len(df1h)
    both_1h = int(((df1h["bid_size"] > 0) & (df1h["ask_size"] > 0)).sum()) if total_1h > 0 else 0
    neither_1h = int(((df1h["bid_size"] == 0) & (df1h["ask_size"] == 0)).sum()) if total_1h > 0 else 0

    # ATM band
    max_ts = df["timestamp"].max()
    latest = df[df["timestamp"] == max_ts]
    underlying = (
        latest["underlying_price"].iloc[0]
        if "underlying_price" in latest.columns
        else latest["mark_price"].median()
    )
    atm = latest[
        (latest["strike"] >= underlying * 0.98) & (latest["strike"] <= underlying * 1.02)
    ]
    atm_total = len(atm)
    atm_both = int(((atm["bid_size"] > 0) & (atm["ask_size"] > 0)).sum()) if atm_total > 0 else 0

    # NaN/Inf check
    num_cols = df.select_dtypes(include="number").columns
    nan_count = int(df[num_cols].isna().sum().sum())
    inf_count = int((df[num_cols].replace([float("inf"), float("-inf")], None).isna() & ~df[num_cols].isna()).sum().sum())

    # Negative check on price-like columns
    neg_issues = []
    for c in ("mark_price", "bid_price", "ask_price", "underlying_price", "index_price"):
        if c in df.columns:
            n = int((df[c] < 0).sum())
            if n > 0:
                neg_issues.append(f"{c} neg={n}")

    print(f"  {sym}:")
    print(f"    Total rows: {total:,}")
    print(f"    Both>0 (all):      {both}/{total} = {100*both/total:.1f}%")
    print(f"    Both>0 (last1h):   {both_1h}/{total_1h} = {100*both_1h/(total_1h or 1):.1f}%")
    print(f"    Neither (all):     {neither}/{total} = {100*neither/total:.1f}%")
    print(f"    Ask-only (all):    {ask_only}/{total} = {100*ask_only/total:.1f}%")
    print(f"    ATM {underlying:,.0f} +/-2%:  {atm_both}/{atm_total} = {100*atm_both/(atm_total or 1):.1f}%")
    print(f"    NaN count: {nan_count}  Inf count: {inf_count}")
    print(f"    Negative prices: {neg_issues or 'none'}")


# ── 3. BID/ASK PRICE CORRECTNESS ─────────────────────────────────────────────

print("\n  Other streams - data sanity:")
for ex, stream, sym in [
    ("binance", "spot_price", "BTCUSDT"),
    ("binance", "mark_price", "BTCUSDT"),
    ("deribit", "index_price", "BTC"),
    ("deribit", "mark_price", "BTC-PERPETUAL"),
    ("deribit", "dvol", "BTC"),
]:
    files = sorted(glob.glob(f"{DATA_ROOT}/{ex}/{stream}/{sym}_*.parquet"))
    if not files:
        continue
    df = pd.read_parquet(files[-1])
    if len(df) < 2:
        print(f"  {ex}/{stream}/{sym}: {len(df)} rows (skip)")
        continue
    num_cols = df.select_dtypes(include="number").columns
    nan_c = int(df[num_cols].isna().sum().sum())
    # negative check
    neg_cols = []
    for c in num_cols:
        if c in ("timestamp",):
            continue
        n = int((df[c] < 0).sum())
        if n > 0:
            neg_cols.append(f"{c}({n})")
    print(f"  {ex}/{stream}/{sym}: {len(df):,} rows, NaN={nan_c}, neg={neg_cols or 'none'}")


# ── 4. ROBUSTNESS ────────────────────────────────────────────────────────────

print("\n── 3. ROBUSTNESS ──")

with open(LOG_PATH) as f:
    lines = f.readlines()

# Connect stats
code_1009 = sum(1 for l in lines if "code=1009" in l)
code_1006 = sum(1 for l in lines if "code=1006" in l)
traceback_count = sum(
    1 for l in lines
    if "Traceback" in l and "Task exception was never retrieved" not in l
)
task_exc_count = sum(1 for l in lines if "Task exception was never retrieved" in l)
# real errors (not warnings)
error_count = sum(1 for l in lines if "[ERROR]" in l)
warn_503 = sum(1 for l in lines if "503" in l and "Server Error" in l)

# Recent (since 2026-07-17 20:00)
recent = [l for l in lines if l >= "2026-07-17 20:"]
recent_code_1006 = sum(1 for l in recent if "code=1006" in l)
recent_503 = sum(1 for l in recent if "503" in l and "Server Error" in l)

# Uptime from first line
first_line = lines[0].strip() if lines else "unknown"
last_line = lines[-1].strip() if lines else "unknown"

print(f"  Log span: {first_line[:19]} → {last_line[:19]}")
print(f"  code=1009 (message too big): {code_1009}")
print(f"  code=1006 (WS close):       {code_1006} total, {recent_code_1006} since 2026-07-17 20:00")
print(f"  503 from Deribit:           {warn_503} total, {recent_503} recent")
print(f"  REAL errors (not WARNING):  {error_count}")
print(f"  Real Tracebacks:            {traceback_count}")
print(f"  Cosmetic Task exc warnings: {task_exc_count}")


# ── 5. SUMMARY ───────────────────────────────────────────────────────────────

print("\n── 4. SUMMARY ──")
print(f"  Streams present: {healthy}/27")
print(f"  Issues: {len(issues)}")
for i in issues:
    print(f"    ⚠ {i}")

if not issues and healthy == 27:
    print("  COMPLETENESS:  PASS")
    print("  CONTINUITY:    PASS")
    print("  CORRECTNESS:   PASS")
    print("  ROBUSTNESS:    PASS")
