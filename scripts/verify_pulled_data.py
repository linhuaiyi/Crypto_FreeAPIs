"""Comprehensive verification of pulled production data (USDC-based).
Checks: completeness, correctness, reasonableness.
"""
from __future__ import annotations

import glob
import os
import sys
from collections import defaultdict

import pandas as pd

DATA_ROOT = sys.argv[1] if len(sys.argv) > 1 else "."

# ── Expected streams (post-USDC migration) ─────────────────────────────────────
# Derived from config_strategy.yaml + launch.py actual collectors
EXPECTED = {
    "deribit/spot_price":       ["BTC_USDC", "ETH_USDC", "SOL_USDC"],
    "deribit/mark_price":       ["BTC_USDC-PERPETUAL", "ETH_USDC-PERPETUAL", "SOL_USDC-PERPETUAL"],
    "deribit/index_price":      ["BTC_USDC", "ETH_USDC", "SOL_USDC"],
    "deribit/funding_rate":     ["BTC_USDC-PERPETUAL", "ETH_USDC-PERPETUAL", "SOL_USDC-PERPETUAL"],
    "deribit/basis":            ["BTC_USDC", "ETH_USDC", "SOL_USDC"],
    "deribit/options_greeks":   ["BTC_USDC", "ETH_USDC", "SOL_USDC"],
    "deribit/options_ticker":   ["BTC_USDC-PERPETUAL", "ETH_USDC-PERPETUAL", "SOL_USDC-PERPETUAL"],
    "deribit/vol_surface":      ["BTC_USDC", "ETH_USDC", "SOL_USDC"],
    "deribit/dvol":             ["BTC", "ETH"],
    "deribit/margin_params":    ["BTC", "ETH"],
    "binance/funding_rate":     ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "binance/mark_price":       ["BTCUSDT", "ETHUSDT"],
    "hyperliquid/funding_rate": ["BTC", "ETH", "SOL"],
    "fred/risk_free_rate":      ["USD"],
}

# Price sanity ranges (USDC-denominated)
PRICE_RANGES = {
    "BTC_USDC": (50000, 150000),
    "ETH_USDC": (1000, 8000),
    "SOL_USDC": (30, 300),
}

ISSUES = []
PASSES = []
OK = 0

def check(condition: bool, msg: str, severity: str = "WARN") -> None:
    global OK
    if condition:
        OK += 1
        PASSES.append(f"PASS: {msg}")
    else:
        ISSUES.append(f"[{severity}] {msg}")

# ── 0. Find all files ──────────────────────────────────────────────────────────
all_files = sorted(glob.glob(f"{DATA_ROOT}/**/*.parquet", recursive=True))
by_streamsym: dict[str, list[str]] = defaultdict(list)
for f in all_files:
    rel = os.path.relpath(f, DATA_ROOT).replace(os.sep, "/")
    parts = rel.split("/")
    if len(parts) < 3:
        continue
    stream = "/".join(parts[:-1])
    sym = parts[-1].split("_")[0]  # crude but works for most
    by_streamsym[stream].append(f)

print("=" * 90)
print("  PRODUCTION DATA VERIFICATION (USDC Migration)")
print("=" * 90)

# ── 1. COMPLETENESS ────────────────────────────────────────────────────────────
print("\n── 1. COMPLETENESS ──")

total_expected = sum(len(syms) for syms in EXPECTED.values())
found_count = 0

for stream, syms in sorted(EXPECTED.items()):
    files = by_streamsym.get(stream, [])
    present_syms = set()
    for f in files:
        fname = os.path.basename(f)
        for s in syms:
            if fname.startswith(s + "_"):
                present_syms.add(s)
    for s in syms:
        label = f"{stream}/{s}"
        if s in present_syms:
            found_count += 1
        else:
            print(f"  MISSING: {label}")
            ISSUES.append(f"MISSING: {label}")

check(found_count == total_expected,
      f"All {found_count}/{total_expected} expected streams present")

# ── 2. FILE STATS ──────────────────────────────────────────────────────────────
print("\n── 2. FILE STATS ──")
header = f"  {'stream':<48} {'files':>5} {'rows':>10} {'ts_range'}"
print(header)
print("  " + "-" * 86)

total_rows_all = 0
for stream in sorted(by_streamsym):
    fs = by_streamsym[stream]
    stream_rows = 0
    stream_mn = stream_mx = None
    for f in fs:
        try:
            t = pd.read_parquet(f, columns=["timestamp"])
            ts = t["timestamp"]
        except Exception as e:
            print(f"  ERR {f}: {e}")
            continue
        stream_rows += len(ts)
        mn = ts.min()
        mx = ts.max()
        if stream_mn is None or mn < stream_mn:
            stream_mn = mn
        if stream_mx is None or mx > stream_mx:
            stream_mx = mx

    from datetime import datetime, timezone
    first = datetime.fromtimestamp(stream_mn / 1000, tz=timezone.utc).strftime("%m-%d %H:%M") if stream_mn else "-"
    last = datetime.fromtimestamp(stream_mx / 1000, tz=timezone.utc).strftime("%m-%d %H:%M") if stream_mx else "-"
    ts_range = f"{first} → {last}"
    print(f"  {stream:<48} {len(fs):>5} {stream_rows:>10,}  {ts_range}")
    total_rows_all += stream_rows

print("  " + "-" * 86)
print(f"  TOTAL: {sum(len(v) for v in by_streamsym.values())} files, {total_rows_all:,} rows")

# ── 3. CORRECTNESS ─────────────────────────────────────────────────────────────
print("\n── 3. CORRECTNESS (field-level) ──")

# ── 3a. options_greeks: bid_size/ask_size coverage (T4 fix) ────────────────────
print("\n  [3a] options_greeks bid_size/ask_size coverage (T4):")
for sym in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
    files = sorted(glob.glob(f"{DATA_ROOT}/deribit/options_greeks/{sym}_*.parquet"))
    if not files:
        print(f"    {sym}: NO FILES")
        continue
    df = pd.read_parquet(files[-1])  # latest day
    total = len(df)
    both = int(((df["bid_size"] > 0) & (df["ask_size"] > 0)).sum())
    neither = int(((df["bid_size"] == 0) & (df["ask_size"] == 0)).sum())
    bid_only = int(((df["bid_size"] > 0) & (df["ask_size"] == 0)).sum())
    ask_only = int(((df["bid_size"] == 0) & (df["ask_size"] > 0)).sum())

    print(f"    {sym}:")
    print(f"      Rows: {total:,}")
    print(f"      Both>0:  {both}/{total} = {100*both/(total or 1):.1f}%")
    print(f"      Neither: {neither}/{total} = {100*neither/(total or 1):.1f}%")
    print(f"      Bid-only:{bid_only}/{total} = {100*bid_only/(total or 1):.1f}%")
    print(f"      Ask-only:{ask_only}/{total} = {100*ask_only/(total or 1):.1f}%")

    # ATM band check
    max_ts = df["timestamp"].max()
    latest = df[df["timestamp"] == max_ts]
    if "underlying_price" in latest.columns and len(latest) > 0:
        und = latest["underlying_price"].median()
    elif "mark_price" in latest.columns and len(latest) > 0:
        und = latest["mark_price"].median()
    else:
        und = None
    if und and und > 0:
        atm = latest[
            (latest["strike"] >= und * 0.98) & (latest["strike"] <= und * 1.02)
        ]
        atm_tot = len(atm)
        atm_both = int(((atm["bid_size"] > 0) & (atm["ask_size"] > 0)).sum()) if atm_tot > 0 else 0
        print(f"      ATM {und:,.0f} +/-2%: {atm_both}/{atm_tot} = {100*atm_both/(atm_tot or 1):.1f}%")
        check(atm_both / (atm_tot or 1) >= 0.90,
              f"{sym} ATM bid/ask coverage: {100*atm_both/(atm_tot or 1):.1f}% (target >=90%)")

    # NaN/Inf checks
    num_cols = df.select_dtypes(include="number").columns
    nan_total = int(df[num_cols].isna().sum().sum())
    print(f"      NaN total: {nan_total}")

    # Price sanity: no negatives on price columns
    for col in ("mark_price", "bid_price", "ask_price", "underlying_price", "index_price"):
        if col in df.columns:
            neg = int((df[col] < 0).sum())
            check(neg == 0, f"{sym} {col} no negatives: {neg}".replace(": 0", ": ok"))

# ── 3b. spot_price ──────────────────────────────────────────────────────────────
print("\n  [3b] spot_price:")
for sym in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
    files = sorted(glob.glob(f"{DATA_ROOT}/deribit/spot_price/{sym}_*.parquet"))
    if not files:
        print(f"    {sym}: NO FILES")
        continue
    df = pd.read_parquet(files[-1])
    lo, hi = PRICE_RANGES.get(sym, (0, 1e12))
    price_col = "price"
    vals = df[price_col]
    in_range = int(((vals >= lo) & (vals <= hi)).sum())
    print(f"    {sym}: {len(df):,} rows, price [{vals.min():.1f}, {vals.max():.1f}], "
          f"in_range={in_range}/{len(df)} ({100*in_range/(len(df) or 1):.1f}%)")
    check(in_range / (len(df) or 1) > 0.95,
          f"{sym} spot_price in expected range [{lo}, {hi}]: {100*in_range/(len(df) or 1):.1f}%")

# ── 3c. mark_price ──────────────────────────────────────────────────────────────
print("\n  [3c] mark_price:")
for sym in ("BTC_USDC-PERPETUAL", "ETH_USDC-PERPETUAL", "SOL_USDC-PERPETUAL"):
    files = sorted(glob.glob(f"{DATA_ROOT}/deribit/mark_price/{sym}_*.parquet"))
    if not files:
        print(f"    {sym}: NO FILES")
        continue
    df = pd.read_parquet(files[-1])
    base = sym.replace("-PERPETUAL", "")
    lo, hi = PRICE_RANGES.get(base, (0, 1e12))
    vals = df["mark_price"]
    in_range = int(((vals >= lo) & (vals <= hi)).sum())
    print(f"    {sym}: {len(df):,} rows, mark [{vals.min():.1f}, {vals.max():.1f}], "
          f"in_range={in_range}/{len(df)} ({100*in_range/(len(df) or 1):.1f}%)")
    check(in_range / (len(df) or 1) > 0.95,
          f"{sym} mark_price in range: {100*in_range/(len(df) or 1):.1f}%")

# ── 3d. index_price ─────────────────────────────────────────────────────────────
print("\n  [3d] index_price:")
for sym in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
    files = sorted(glob.glob(f"{DATA_ROOT}/deribit/index_price/{sym}_*.parquet"))
    if not files:
        continue
    df = pd.read_parquet(files[-1])
    lo, hi = PRICE_RANGES.get(sym, (0, 1e12))
    vals = df["index_price"]
    in_range = int(((vals >= lo) & (vals <= hi)).sum())
    # check for expected columns
    has_edp = "estimated_delivery_price" in df.columns
    print(f"    {sym}: {len(df):,} rows, index [{vals.min():.1f}, {vals.max():.1f}], "
          f"est_delivery={has_edp}")
    check(in_range / (len(df) or 1) > 0.95,
          f"{sym} index_price in range: {100*in_range/(len(df) or 1):.1f}%")
    check(has_edp, f"{sym} index_price has estimated_delivery_price")

# ── 3e. funding_rate ────────────────────────────────────────────────────────────
print("\n  [3e] funding_rate:")
for ex, syms in [("deribit", ["BTC_USDC-PERPETUAL", "ETH_USDC-PERPETUAL", "SOL_USDC-PERPETUAL"]),
                  ("binance", ["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
                  ("hyperliquid", ["BTC", "ETH", "SOL"])]:
    for sym in syms:
        files = sorted(glob.glob(f"{DATA_ROOT}/{ex}/funding_rate/{sym}_*.parquet"))
        if not files:
            print(f"    {ex}/{sym}: NO FILES")
            continue
        df = pd.read_parquet(files[-1])
        fr_col = "funding_rate" if "funding_rate" in df.columns else "rate"
        if fr_col not in df.columns:
            print(f"    {ex}/{sym}: {len(df)} rows, no funding_rate col")
            continue
        vals = df[fr_col]
        # funding rates typically in [-0.05, 0.05]
        in_range = int(((vals >= -0.1) & (vals <= 0.1)).sum())
        print(f"    {ex}/{sym}: {len(df)} rows, rate [{vals.min():.6f}, {vals.max():.6f}]")
        check(in_range / (len(df) or 1) > 0.95,
              f"{ex}/{sym} funding_rate in [-0.1, 0.1]: {100*in_range/(len(df) or 1):.1f}%")

# ── 3f. basis ───────────────────────────────────────────────────────────────────
print("\n  [3f] basis:")
for sym in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
    files = sorted(glob.glob(f"{DATA_ROOT}/deribit/basis/{sym}_*.parquet"))
    if not files:
        continue
    df = pd.read_parquet(files[-1])
    bp_col = "basis_bps" if "basis_bps" in df.columns else None
    if bp_col:
        vals = df[bp_col]
        print(f"    {sym}: {len(df)} rows, basis_bps [{vals.min():.2f}, {vals.max():.2f}]")
    else:
        print(f"    {sym}: {len(df)} rows, cols={list(df.columns)}")

# ── 3g. dvol ────────────────────────────────────────────────────────────────────
print("\n  [3g] dvol:")
for sym in ("BTC", "ETH"):
    files = sorted(glob.glob(f"{DATA_ROOT}/deribit/dvol/{sym}_*.parquet"))
    if not files:
        continue
    df = pd.read_parquet(files[-1])
    vals = df["dvol"]
    # DVOL typically 30-150
    print(f"    {sym}: {len(df)} rows, dvol [{vals.min():.1f}, {vals.max():.1f}]")

# ── 4. CROSS-STREAM CONSISTENCY ────────────────────────────────────────────────
print("\n── 4. CROSS-STREAM CONSISTENCY ──")

# spot vs index: should be highly correlated (same underlying)
for base in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
    spot_files = sorted(glob.glob(f"{DATA_ROOT}/deribit/spot_price/{base}_*.parquet"))
    idx_files = sorted(glob.glob(f"{DATA_ROOT}/deribit/index_price/{base}_*.parquet"))
    if not spot_files or not idx_files:
        continue
    spot_df = pd.read_parquet(spot_files[-1])
    idx_df = pd.read_parquet(idx_files[-1])
    spot_mean = spot_df["price"].mean()
    idx_mean = idx_df["index_price"].mean()
    diff_pct = abs(spot_mean - idx_mean) / idx_mean * 100
    print(f"  {base}: spot_avg={spot_mean:.2f}, index_avg={idx_mean:.2f}, diff={diff_pct:.4f}%")
    check(diff_pct < 2.0, f"{base} spot-index divergence < 2%: {diff_pct:.4f}%")

# spot vs mark_price: basis should be small
for base in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
    spot_files = glob.glob(f"{DATA_ROOT}/deribit/spot_price/{base}_*.parquet")
    mark_files = glob.glob(f"{DATA_ROOT}/deribit/mark_price/{base}-PERPETUAL_*.parquet")
    if not spot_files or not mark_files:
        continue
    spot_df = pd.read_parquet(spot_files[-1])
    mark_df = pd.read_parquet(mark_files[-1])
    spot_m = spot_df["price"].mean()
    mark_m = mark_df["mark_price"].mean()
    basis_bps = (mark_m - spot_m) / spot_m * 10000
    print(f"  {base}: spot={spot_m:.2f}, perp_mark={mark_m:.2f}, basis={basis_bps:.2f} bps")
    check(abs(basis_bps) < 500, f"{base} basis < 500 bps: {basis_bps:.2f}")  # USDC perp basis usually tiny

# ── 5. FRESHNESS ────────────────────────────────────────────────────────────────
print("\n── 5. FRESHNESS ──")
import time
now_s = time.time()

for stream, syms in sorted(EXPECTED.items()):
    for sym in syms:
        files = sorted(glob.glob(f"{DATA_ROOT}/{stream}/{sym}_*.parquet"))
        if not files:
            continue
        try:
            t = pd.read_parquet(files[-1], columns=["timestamp"])
            last_ts = t["timestamp"].max()
            age_min = (now_s - last_ts / 1000) / 60
            if age_min > 60:
                print(f"  STALE: {stream}/{sym}: last data {age_min:.0f} min ago")
            elif age_min > 30:
                print(f"  WARNING: {stream}/{sym}: last data {age_min:.0f} min ago")
        except Exception:
            pass

# ── 6. SUMMARY ──────────────────────────────────────────────────────────────────
print("\n" + "=" * 90)
print("  VERIFICATION SUMMARY")
print("=" * 90)
print(f"  PASSES: {len(PASSES)}")
print(f"  ISSUES: {len(ISSUES)}")
for i in ISSUES:
    print(f"    {i}")

if not ISSUES:
    print("\n  ALL CHECKS PASSED - Data is complete, correct, and reasonable.")
elif all("[WARN]" not in i for i in ISSUES):
    print("\n  Only WARN-level issues found. Data quality is acceptable.")
else:
    print(f"\n  Found {len(ISSUES)} issue(s). Review above.")

print("=" * 90)
