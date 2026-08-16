#!/usr/bin/env python3
"""Per-field data validator for the Deribit options collector.

Runs against one UTC day of CLOSED parquet files (default: yesterday) and
checks every collected field against an explicit invariant (range, sign,
coverage, cross-field consistency). Designed to be cron'd daily so data
issues (a disconnected feed, a zeroed column, a schema drift) surface within
~1 day instead of waiting for a human to notice.

Exit code 0 = all checks passed; 1 = one or more FAILED (use for cron alerting).

Usage:
    python3 scripts/verify_fields.py [YYYY-MM-DD] [--data-dir ./data] [--alert]

If --alert is set and TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env vars are
present, failures are pushed to Telegram.
"""
from __future__ import annotations

import os
import sys
import glob
import json
import argparse
from datetime import datetime, timezone, timedelta

import pandas as pd
import numpy as np

try:
    import requests
except Exception:
    requests = None

DATA_DIR = os.environ.get("VERIFY_DATA_DIR", "./data")
FAIL_THRESHOLD = "FAIL"   # any row-level violation count >= this fails the field


# ---------------------------------------------------------------------------
# Check primitives
# ---------------------------------------------------------------------------

class Result:
    def __init__(self, stream, field, status, detail, n_violations=0, n_checked=0):
        self.stream = stream; self.field = field; self.status = status
        self.detail = detail; self.n_violations = n_violations; self.n_checked = n_checked

    def __repr__(self):
        return f"[{self.status:4}] {self.stream:18} {self.field:22} {self.detail}"


def _check_range(df, field, lo, hi, stream):
    if field not in df.columns:
        return Result(stream, field, "MISS", "列缺失")
    s = pd.to_numeric(df[field], errors="coerce")
    bad = ((s < lo) | (s > hi)).sum()
    status = "PASS" if bad == 0 else "FAIL"
    return Result(stream, field, status, f"range[{lo},{hi}] 违规 {bad}/{len(s)}", bad, len(s))


def _check_nonneg(df, field, stream):
    if field not in df.columns:
        return Result(stream, field, "MISS", "列缺失")
    s = pd.to_numeric(df[field], errors="coerce")
    bad = (s < 0).sum()
    status = "PASS" if bad == 0 else "FAIL"
    return Result(stream, field, status, f"<0 共 {bad}", bad, len(s))


def _check_coverage(df, field, stream, min_pct, subset_mask=None):
    """% of rows (in subset) where field > 0 — the zero-feed bug catcher."""
    if field not in df.columns:
        return Result(stream, field, "MISS", "列缺失")
    s = pd.to_numeric(df[field], errors="coerce")
    mask = subset_mask if subset_mask is not None else pd.Series([True] * len(df), index=df.index)
    sub = s[mask]
    if len(sub) == 0:
        return Result(stream, field, "SKIP", "子集为空")
    cov = (sub > 0).mean()
    status = "PASS" if cov >= min_pct else "FAIL"
    return Result(stream, field, status, f"非零 {cov:.1%} (≥{min_pct:.0%} 期望, {len(sub)}行)", 0, len(sub))


def _check_instrument_coverage(df, field, stream, min_pct, subset_mask=None):
    """% of DISTINCT instruments (in subset) with >=1 non-zero value.

    A robust 'did the feed deliver anything' signal: a working feed populates
    most near-term-ATM instruments at some snapshot; a dead feed (e.g. a wrong
    WS interval that delivers nothing) leaves 0 instruments with data. This is
    what catches a silently-zeroed column where row-coverage is naturally
    sparse (one-sided markets).
    """
    if field not in df.columns:
        return Result(stream, field, "MISS", "列缺失")
    mask = subset_mask if subset_mask is not None else pd.Series([True] * len(df), index=df.index)
    sub = df[mask]
    if len(sub) == 0:
        return Result(stream, field, "SKIP", "子集为空")
    s = pd.to_numeric(sub[field], errors="coerce")
    has_val = s.groupby(sub["instrument_name"]).apply(lambda x: (x > 0).any())
    cov = has_val.mean() if len(has_val) else 0.0
    status = "PASS" if cov >= min_pct else "FAIL"
    return Result(stream, field, status,
                  f"乐器覆盖 {cov:.0%} ({int(has_val.sum())}/{len(has_val)})", 0, len(has_val))


def _near_term_atm_mask(df, max_dte=7, atm_pct=0.15):
    """Rows that SHOULD have ticker-delivered fields (0-7 DTE, ±atm_pct USDC)."""
    parts = df["instrument_name"].astype(str).str.split("-")
    exp = parts.str[1]; strike = pd.to_numeric(parts.str[2], errors="coerce")
    base = parts.str[0]
    usdc = base.isin(["BTC_USDC", "ETH_USDC", "SOL_USDC"])
    d = pd.to_datetime(exp, format="%d%b%y", errors="coerce")
    today = pd.Timestamp(datetime.now(timezone.utc).date())
    dte = (d - today).dt.days
    u = pd.to_numeric(df.get("underlying_price"), errors="coerce")
    moneyness = (strike - u).abs() / u.where(u > 0)
    return usdc & dte.between(0, max_dte) & (moneyness <= atm_pct)


# ---------------------------------------------------------------------------
# Stream validators
# ---------------------------------------------------------------------------

def validate_options_greeks(data_dir, date_str):
    results = []
    base = os.path.join(data_dir, "deribit/options_greeks")
    for coin in ["BTC_USDC", "ETH_USDC", "SOL_USDC"]:
        files = sorted(glob.glob(os.path.join(base, f"{coin}_{date_str}.parquet")))
        if not files:
            results.append(Result("options_greeks", coin, "FAIL", "当日无文件", 0, 0))
            continue
        df = pd.concat([pd.read_parquet(f) for f in files])
        st = f"og.{coin}"
        results.append(Result(st, "rows", "PASS" if len(df) > 1000 else "FAIL", f"{len(df)} 行", 0, len(df)))
        for fld, lo, hi in [("underlying_price", 0, 1e9), ("strike", 0, 1e9),
                            ("mark_iv", 0, 210), ("iv", 0, 210),
                            ("delta", -1, 1), ("vega", -1e6, 1e9)]:
            results.append(_check_range(df, fld, lo, hi, st))
        results.append(_check_nonneg(df, "gamma", st))  # gamma>=0 (spikes near expiry)
        for fld in ["bid_price", "ask_price", "bid_size", "ask_size", "bid_iv", "ask_iv"]:
            results.append(_check_nonneg(df, fld, st))
        # COVERAGE — instrument-level bug catchers (would have caught the 1000ms zero-feed).
        # 5% threshold: passes genuinely-illiquid coins (SOL ~14%) but fails a
        # dead feed (0%). The mark_iv>200 clamp is allowed by the 210 bound.
        nt = _near_term_atm_mask(df)
        results.append(_check_instrument_coverage(df, "bid_iv", st, 0.05, nt))
        results.append(_check_instrument_coverage(df, "ask_iv", st, 0.05, nt))
        if "deribit_delta" in df.columns:
            results.append(_check_instrument_coverage(df, "deribit_delta", st, 0.05, nt))
        # cross-field: mark_iv within [bid_iv, ask_iv] where both > 0
        if {"bid_iv", "ask_iv", "mark_iv"} <= set(df.columns):
            b = pd.to_numeric(df["bid_iv"], errors="coerce")
            a = pd.to_numeric(df["ask_iv"], errors="coerce")
            m = pd.to_numeric(df["mark_iv"], errors="coerce")
            both = (b > 0) & (a > 0)
            in_range = ((m >= b) & (m <= a)) & both
            ratio = in_range.sum() / both.sum() if both.any() else 1.0
            results.append(Result(st, "mark∈[bid,ask]", "PASS" if ratio >= 0.80 else "FAIL",
                                  f"{ratio:.0%} (≥80%期望, {both.sum()}行)", 0, int(both.sum())))
    return results


def _simple_numeric_stream(data_dir, stream_path, date_str, fields, stream_name):
    """Generic validator for streams with simple numeric range checks."""
    results = []
    base = os.path.join(data_dir, stream_path)
    files = sorted(glob.glob(os.path.join(base, f"*_{date_str}.parquet")))
    if not files:
        return [Result(stream_name, "*", "FAIL", "当日无文件", 0, 0)]
    df = pd.concat([pd.read_parquet(f) for f in files])
    results.append(Result(stream_name, "rows", "PASS" if len(df) > 0 else "FAIL", f"{len(df)} 行", 0, len(df)))
    for fld, lo, hi in fields:
        results.append(_check_range(df, fld, lo, hi, stream_name))
    return results


def validate_all(data_dir, date_str):
    R = []
    R += validate_options_greeks(data_dir, date_str)
    R += _simple_numeric_stream(data_dir, "deribit/vol_surface", date_str,
                                [("atm_iv", 0, 200), ("iv_rank", 0, 100)], "vol_surface")
    R += _simple_numeric_stream(data_dir, "deribit/dvol", date_str, [("dvol", 0, 200)], "dvol")
    R += _simple_numeric_stream(data_dir, "deribit/basis", date_str,
                                [("spot_price", 0, 1e9), ("perp_price", 0, 1e9), ("basis_pct", -50, 50)], "basis")
    R += _simple_numeric_stream(data_dir, "deribit/funding_rate", date_str,
                                [("funding_rate", -1, 1), ("mark_price", 0, 1e9), ("index_price", 0, 1e9)], "funding_rate")
    R += _simple_numeric_stream(data_dir, "deribit/index_price", date_str,
                                [("index_price", 0, 1e9)], "index_price")
    R += _simple_numeric_stream(data_dir, "deribit/spot_price", date_str,
                                [("price", 0, 1e9)], "spot_price")
    R += _simple_numeric_stream(data_dir, "deribit/mark_price", date_str,
                                [("mark_price", 0, 1e9)], "mark_price")
    R += _simple_numeric_stream(data_dir, "binance/mark_price", date_str,
                                [("mark_price", 0, 1e9)], "binance.mark")
    R += _simple_numeric_stream(data_dir, "binance/funding_rate", date_str,
                                [("funding_rate", -1, 1)], "binance.funding")
    R += _simple_numeric_stream(data_dir, "hyperliquid/funding_rate", date_str,
                                [("funding_rate", -1, 1)], "hl.funding")
    R += _simple_numeric_stream(data_dir, "fred/risk_free_rate", date_str,
                                [("rate_annual", -1, 1)], "fred.risk_free")
    return R


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

def send_bark(failures, date_str):
    """Push failures to Bark (iOS). creds from BARK_KEY / BARK_SERVER env."""
    key = os.environ.get("BARK_KEY")
    server = os.environ.get("BARK_SERVER", "https://api.day.app")
    if not key or requests is None:
        return
    body_lines = [f"字段校验失败 {date_str}", f"共 {len(failures)} 项:"]
    for r in failures[:20]:
        body_lines.append(f"• [{r.stream}] {r.field}: {r.detail}")
    try:
        requests.post(f"{server}/{key}",
                      json={"title": "⚠️ 采集数据校验失败",
                            "body": "\n".join(body_lines),
                            "group": "verify_fields"},
                      timeout=15)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("date", nargs="?", help="YYYY-MM-DD (default: yesterday UTC)")
    ap.add_argument("--data-dir", default=DATA_DIR)
    ap.add_argument("--alert", action="store_true", help="Bark 推送失败项")
    args = ap.parse_args()

    if args.date:
        date_str = args.date
    else:
        date_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"=== 字段级数据校验 @ {date_str} (data_dir={args.data_dir}) ===")
    results = validate_all(args.data_dir, date_str)

    fails = [r for r in results if r.status == "FAIL"]
    misses = [r for r in results if r.status == "MISS"]
    passes = [r for r in results if r.status == "PASS"]

    for r in results:
        if r.status != "PASS":
            print(r)

    print("\n" + "=" * 60)
    print(f"  PASS={len(passes)}  FAIL={len(fails)}  MISS(列缺失)={len(misses)}")
    print("=" * 60)

    if args.alert and fails:
        send_bark(fails, date_str)

    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
