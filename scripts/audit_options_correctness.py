"""Validate option pricing / greeks correctness on latest snapshot."""
from __future__ import annotations

import glob
import time

import pandas as pd

DATA = "/opt/Crypto_FreeAPIs/deribit-options-data-collector/data/deribit"

print("=" * 80)
print("  OPTION DATA CORRECTNESS AUDIT")
print(f"  {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
print("=" * 80)

for sym in ("BTC", "ETH"):
    files = sorted(glob.glob(f"{DATA}/options_greeks/{sym}_*.parquet"))
    df = pd.read_parquet(files[-1])

    max_ts = df["timestamp"].max()
    snap = df[df["timestamp"] == max_ts].copy()
    n = len(snap)
    print(f"\n{'─'*80}")
    print(f"  {sym} latest snapshot: {n} instruments")
    print(f"  ts: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(max_ts/1000))}")

    # ── 1. Columns ──
    print(f"\n  ── 1. COLUMNS: {len(snap.columns)} total ──")
    print(f"  {sorted(snap.columns.tolist())}")

    # ── 2. Price ordering (bid ≤ mid ≤ ask) ──
    print(f"\n  ── 2. PRICE ORDERING (bid ≤ mid ≤ ask) ──")
    bid_le_mid = int((snap["bid_price"] <= snap["mid_price"] + 1e-9).sum())
    mid_le_ask = int((snap["mid_price"] <= snap["ask_price"] + 1e-9).sum())
    bid_le_ask = int((snap["bid_price"] <= snap["ask_price"] + 1e-9).sum())
    print(f"  bid ≤ mid: {bid_le_mid}/{n} = {100*bid_le_mid/n:.1f}%")
    print(f"  mid ≤ ask: {mid_le_ask}/{n} = {100*mid_le_ask/n:.1f}%")
    print(f"  bid ≤ ask:  {bid_le_ask}/{n} = {100*bid_le_ask/n:.1f}%")
    violations = snap[
        (snap["bid_price"] > snap["mid_price"])
        | (snap["mid_price"] > snap["ask_price"])
    ]
    if len(violations):
        print(f"  VIOLATIONS: {len(violations)}")
        for _, r in violations.head(3).iterrows():
            print(f"    {r['instrument_name']} bid={r['bid_price']:.4f} mid={r['mid_price']:.4f} ask={r['ask_price']:.4f}")

    # ── 3. Greeks sanity ──
    print(f"\n  ── 3. GREEKS SANITY ──")
    calls = snap[snap["option_type"].isin(("call", "C"))]
    puts = snap[snap["option_type"].isin(("put", "P"))]

    call_delta_ok = int(((calls["delta"] >= -0.01) & (calls["delta"] <= 1.01)).sum())
    put_delta_ok = int(((puts["delta"] >= -1.01) & (puts["delta"] <= 0.01)).sum())
    call_delta_bad = calls[calls["delta"] < -0.01]
    put_delta_bad = puts[puts["delta"] > 0.01]

    print(f"  calls: {len(calls)}")
    print(f"    delta ∈ [-0.01, 1.01]: {call_delta_ok}/{len(calls)}", end="")
    if len(call_delta_bad):
        print(f"  BAD: {call_delta_bad[['instrument_name','delta']].head(3).to_dict('records')}")
    else:
        print()

    print(f"  puts:  {len(puts)}")
    print(f"    delta ∈ [-1.01, 0.01]: {put_delta_ok}/{len(puts)}", end="")
    if len(put_delta_bad):
        print(f"  BAD: {put_delta_bad[['instrument_name','delta']].head(3).to_dict('records')}")
    else:
        print()

    gamma_ok = int((snap["gamma"] >= 0).sum())
    gamma_bad = int((snap["gamma"] < 0).sum())
    print(f"  gamma ≥ 0:              {gamma_ok}/{n}", end="")
    if gamma_bad:
        print(f"  NEGATIVE: {gamma_bad}")
    else:
        print("  ✓")

    vega_ok = int((snap["vega"] >= 0).sum())
    vega_bad = int((snap["vega"] < 0).sum())
    print(f"  vega ≥ 0:               {vega_ok}/{n}", end="")
    if vega_bad:
        print(f"  NEGATIVE: {vega_bad}")
    else:
        print("  ✓")

    theta_neg_puts = int((puts["theta"] < 0).sum()) if len(puts) > 0 else 0
    print(f"  put theta < 0:          {theta_neg_puts}/{len(puts)} = {100*theta_neg_puts/max(1,len(puts)):.1f}%")

    # Rho sign: calls +, puts -
    rho_call_pos = int((calls["rho"] > 0).sum()) if len(calls) > 0 else 0
    rho_put_neg = int((puts["rho"] < 0).sum()) if len(puts) > 0 else 0
    print(f"  call rho > 0:           {rho_call_pos}/{len(calls)}")
    print(f"  put rho < 0:            {rho_put_neg}/{len(puts)}")

    # ── 4. IV sanity ──
    print(f"\n  ── 4. IV (mark_iv) SANITY ──")
    iv = snap["mark_iv"].dropna()
    lo, hi = iv.min(), iv.max()
    p50, p95, p99 = iv.quantile([0.5, 0.95, 0.99]).tolist()
    outliers = int(((iv > 5.0) | (iv < 0.01)).sum())  # >500% or <1%
    print(f"  range: {lo*100:.1f}% – {hi*100:.1f}%")
    print(f"  p50={p50*100:.1f}%  p95={p95*100:.1f}%  p99={p99*100:.1f}%")
    print(f"  outliers (>500% or <1%): {outliers}/{len(iv)}")
    if outliers:
        bad_iv = iv[(iv > 5.0) | (iv < 0.01)]
        print(f"  sample outliers: {bad_iv.head(5).tolist()}")

    # ── 5. Mid price consistency ──
    print(f"\n  ── 5. MID PRICE vs (BID+ASK)/2 ──")
    has_both = (snap["bid_price"] > 0) & (snap["ask_price"] > 0)
    n_both = int(has_both.sum())
    if n_both > 0:
        bid_ask_mid = (snap["bid_price"] + snap["ask_price"]) / 2
        diff = (snap["mid_price"] - bid_ask_mid).abs()
        within_1pct = int((diff <= snap["mid_price"] * 0.01 + 0.001).sum())
        within_5pct = int((diff <= snap["mid_price"] * 0.05 + 0.001).sum())
        print(f"  with both prices: {n_both}/{n}")
        print(f"  mid ≈ (bid+ask)/2 ±1%: {within_1pct}/{n_both} = {100*within_1pct/n_both:.1f}%")
        print(f"  mid ≈ (bid+ask)/2 ±5%: {within_5pct}/{n_both} = {100*within_5pct/n_both:.1f}%")
        bad = snap[has_both & (diff > snap["mid_price"] * 0.05 + 0.001)]
        if len(bad):
            print(f"  MISPRICED ({len(bad)}):")
            for _, r in bad.head(3).iterrows():
                c = (r["bid_price"] + r["ask_price"]) / 2
                print(f"    {r['instrument_name']} bid={r['bid_price']:.4f} ask={r['ask_price']:.4f} mid={r['mid_price']:.4f} computed={c:.4f}")

    # ── 6. Spread ──
    print(f"\n  ── 6. SPREAD STATS ──")
    spread_pct = ((snap["ask_price"] - snap["bid_price"]) / snap["ask_price"].replace(0, None)) * 100
    spread_clean = spread_pct[has_both & (spread_pct > 0)]
    if len(spread_clean) > 0:
        print(f"  spread% p50={spread_clean.quantile(0.5):.2f}% p95={spread_clean.quantile(0.95):.2f}% p99={spread_clean.quantile(0.99):.2f}%")
    crossed = int((snap["ask_price"] < snap["bid_price"]).sum())
    print(f"  crossed (ask<bid): {crossed}/{n}")

    # ── 7. ATM strike continuity ──
    print(f"\n  ── 7. UNDERLYING ──")
    ul = snap["underlying_price"].dropna()
    print(f"  mean={ul.mean():,.2f}  min={ul.min():,.2f}  max={ul.max():,.2f}  unique={ul.nunique()}")
    # check strike spacing around ATM
    atm_strikes = sorted(snap["strike"].unique())
    ul_val = ul.iloc[0]
    nearby = [s for s in atm_strikes if ul_val * 0.8 < s < ul_val * 1.2]
    gaps = [nearby[i+1] - nearby[i] for i in range(len(nearby)-1)]
    if gaps:
        print(f"  ATM strike gaps (min/max): {min(gaps):.0f}/{max(gaps):.0f}")

print(f"\n{'='*80}")
print("  AUDIT COMPLETE")
