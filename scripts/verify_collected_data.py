"""
V3.0 Data Collection Verification Script.

Auto-discovers all flat-named parquet data under data/{exchange}/{data_type}/{symbol}_{date}.parquet,
verifies completeness/continuity/correctness via streaming (no OOM).

Usage:
    python scripts/verify_collected_data.py --data-dir ./deribit-options-data-collector/data
    python scripts/verify_collected_data.py --data-dir ./deribit-options-data-collector/data --log ./deribit-options-data-collector/logs/collector.log
    python scripts/verify_collected_data.py --data-dir ./deribit-options-data-collector/data --no-log
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from pipeline.strategy_configs import get_all_strategies
from utils import get_logger

logger = get_logger("DataVerify")

_LARGE_FILE_ROWS = 100_000
_V3_EXCHANGES = {"binance", "deribit", "hyperliquid", "fred"}


# ── Data classes ──

@dataclass
class FileCheck:
    path: str
    rows: int
    columns: List[str]
    ts_min: Optional[str] = None
    ts_max: Optional[str] = None
    size_kb: float = 0.0
    null_warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def warnings(self) -> List[str]:
        return self.null_warnings

    @property
    def status(self) -> str:
        if self.errors:
            return "ERROR"
        if self.rows == 0:
            return "WARNING"
        if self.null_warnings:
            return "WARNING"
        return "OK"


@dataclass
class SourceCheck:
    exchange: str
    data_type: str
    symbol: str
    files: List[FileCheck] = field(default_factory=list)
    total_rows: int = 0

    @property
    def status(self) -> str:
        if any(f.errors for f in self.files):
            return "ERROR"
        if self.total_rows == 0:
            return "MISSING"
        if any(f.null_warnings for f in self.files):
            return "WARNING"
        return "OK"

    @property
    def messages(self) -> List[str]:
        msgs = []
        for fc in self.files:
            msgs.extend(fc.null_warnings)
            msgs.extend(fc.errors)
        if not self.files:
            msgs.append("No parquet files found")
        return msgs


# ── File check (metadata + sampling, no full read) ──

def check_parquet_file(filepath: str) -> FileCheck:
    errors: List[str] = []
    null_warnings: List[str] = []

    if "1970-01-01" in filepath.replace("\\", "/"):
        errors.append("epoch-zero date in filename")

    size_kb = os.path.getsize(filepath) / 1024.0

    try:
        pf = pq.ParquetFile(filepath)
    except Exception as e:
        return FileCheck(path=filepath, rows=0, columns=[], size_kb=size_kb, errors=[str(e)])

    rows = pf.metadata.num_rows
    columns = list(pf.schema_arrow.names)

    ts_min_str, ts_max_str = None, None
    if "timestamp" in columns and rows > 0:
        try:
            first_rg = pf.read_row_group(0, columns=["timestamp"])
            ts_min_str = str(pd.to_datetime(first_rg.column("timestamp")[0].as_py(), unit="ms"))
            last_idx = pf.metadata.num_row_groups - 1
            last_rg = pf.read_row_group(last_idx, columns=["timestamp"])
            ts_max_str = str(pd.to_datetime(last_rg.column("timestamp")[-1].as_py(), unit="ms"))
        except Exception:
            pass

    if rows > 0:
        key_cols = ["mark_price", "funding_rate", "bid_price", "ask_price"]
        cols_to_check = [c for c in key_cols if c in columns]
        if cols_to_check:
            if rows <= _LARGE_FILE_ROWS:
                df = pf.read(columns=cols_to_check).to_pandas()
                for col in cols_to_check:
                    if df[col].isnull().sum() / len(df) > 0.5:
                        null_warnings.append(f"Column '{col}' has >50% nulls")
                del df
            else:
                rg_count = pf.metadata.num_row_groups
                sample_rgs = list({0, rg_count // 2, rg_count - 1})
                chunks = []
                for ri in sample_rgs:
                    try:
                        chunks.append(pf.read_row_group(ri, columns=cols_to_check).to_pandas())
                    except Exception:
                        continue
                if chunks:
                    sample = pd.concat(chunks, ignore_index=True)
                    for col in cols_to_check:
                        if sample[col].isnull().sum() / len(sample) > 0.5:
                            null_warnings.append(f"Column '{col}' has >50% nulls (sampled)")
                    del sample, chunks

    return FileCheck(
        path=filepath, rows=rows, columns=columns,
        ts_min=ts_min_str, ts_max=ts_max_str,
        size_kb=size_kb, null_warnings=null_warnings, errors=errors,
    )


# ── Streaming timestamp analysis ──

@dataclass
class _TsState:
    prev_ts: Optional[int] = None
    ts_min: Optional[int] = None
    ts_max: Optional[int] = None
    total_rows: int = 0
    max_gap: float = 0.0
    max_gap_at: Optional[int] = None
    monotonic: bool = True
    dup_count: int = 0
    unique_ts: int = 0

    def feed(self, ts: np.ndarray) -> None:
        if len(ts) == 0:
            return
        self.total_rows += len(ts)
        cmin, cmax = int(ts[0]), int(ts[-1])
        if self.ts_min is None or cmin < self.ts_min:
            self.ts_min = cmin
        if self.ts_max is None or cmax > self.ts_max:
            self.ts_max = cmax
        if self.prev_ts is not None:
            gap = cmin - self.prev_ts
            if gap < 0:
                self.monotonic = False
            elif gap > self.max_gap:
                self.max_gap = float(gap)
                self.max_gap_at = self.prev_ts
        if len(ts) > 1:
            diffs = np.diff(ts)
            if (diffs < 0).any():
                self.monotonic = False
            local_max = int(diffs.max())
            if local_max > self.max_gap:
                self.max_gap = float(local_max)
                self.max_gap_at = int(ts[int(np.argmax(diffs))])
            self.dup_count += int((diffs == 0).sum())
            self.unique_ts += int((diffs > 0).sum()) + 1
        else:
            self.unique_ts += 1
        self.prev_ts = cmax


def _stream_files(files: List[str]) -> _TsState:
    state = _TsState()
    for fp in files:
        try:
            pf = pq.ParquetFile(fp)
            if "timestamp" not in pf.schema_arrow.names:
                continue
            for ri in range(pf.metadata.num_row_groups):
                try:
                    tbl = pf.read_row_group(ri, columns=["timestamp"])
                    ts = np.array(tbl.column("timestamp").to_pylist(), dtype=np.int64)
                    ts.sort()
                    state.feed(ts)
                    del ts, tbl
                except Exception:
                    continue
        except Exception:
            continue
    return state


def check_continuity(files: List[str], expected_sec: float, tolerance: float = 3.0) -> List[str]:
    if not files:
        return ["No files found"]
    state = _stream_files(files)
    if state.total_rows == 0:
        return ["No timestamp data"]
    issues: List[str] = []
    expected_ms = expected_sec * 1000
    if state.max_gap > expected_ms * tolerance:
        gap_time = pd.to_datetime(state.max_gap_at, unit="ms") if state.max_gap_at else "unknown"
        issues.append(f"Max gap {state.max_gap/1000:.0f}s at {gap_time}")
    if not state.monotonic:
        issues.append("Timestamps not monotonically increasing")
    if state.dup_count > 0:
        issues.append(f"{state.dup_count} duplicate timestamps")
    return issues


def compute_coverage(files: List[str], expected_sec: float, is_multi_instrument: bool = False) -> Tuple[int, float, Optional[str], Optional[str], float]:
    if not files:
        return 0, 0.0, None, None, 0.0
    state = _stream_files(files)
    if state.total_rows == 0 or state.ts_min is None:
        return 0, 0.0, None, None, 0.0
    hours = (state.ts_max - state.ts_min) / (3_600_000)
    # For multi-instrument (greeks), use unique timestamps for expected-interval coverage
    intervals = state.unique_ts if is_multi_instrument else state.total_rows
    expected = hours * 3600 / expected_sec
    coverage = intervals / expected * 100 if expected > 0 else 0
    start = str(pd.to_datetime(state.ts_min, unit="ms"))
    end = str(pd.to_datetime(state.ts_max, unit="ms"))
    return state.total_rows, hours, start, end, coverage


# ── Auto-discovery of V3.0 sources ──

def discover_v3_sources(data_dir: str) -> Dict[Tuple[str, str, str], List[str]]:
    """Discover {exchange}/{data_type}/{symbol}_{date}.parquet → files mapping."""
    sources: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for exchange in _V3_EXCHANGES:
        base = os.path.join(data_dir, exchange)
        if not os.path.isdir(base):
            continue
        for dtype_dir in sorted(glob.glob(os.path.join(base, "*"))):
            if not os.path.isdir(dtype_dir):
                continue
            data_type = os.path.basename(dtype_dir)
            for pf in sorted(glob.glob(os.path.join(dtype_dir, "*.parquet"))):
                fname = os.path.basename(pf)
                # Extract symbol: filename is {symbol}_{YYYY-MM-DD}.parquet
                name_without_ext = os.path.splitext(fname)[0]
                # Find last _YYYY-MM-DD pattern
                parts = name_without_ext.rsplit("_", 1)
                if len(parts) == 2 and len(parts[1]) == 10 and parts[1][4] == "-":
                    symbol = parts[0]
                else:
                    symbol = name_without_ext
                sources[(exchange, data_type, symbol)].append(pf)
    return sources


# ── Log analysis ──

def verify_log_file(log_path: str) -> Dict:
    if not os.path.exists(log_path):
        return {"status": "NO_LOG", "errors": [], "warnings": []}

    errors, warnings = [], []
    greeks_stats: Dict[str, int] = {}
    vol_surface_stats: Dict[str, int] = {}

    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "WARNING" in line:
                warnings.append(line)
            elif "ERROR" in line or ("Exception" in line and "Traceback" not in line):
                errors.append(line)
            elif "computed" in line and "Greeks" in line:
                for cur in ["BTC", "ETH"]:
                    if f" {cur} " in line:
                        greeks_stats[cur] = greeks_stats.get(cur, 0) + 1
                        break
            elif "Surface built for" in line:
                for cur in ["BTC", "ETH"]:
                    if f"for {cur}:" in line:
                        vol_surface_stats[cur] = vol_surface_stats.get(cur, 0) + 1
                        break

    return {
        "status": "OK" if not errors else "HAS_ERRORS",
        "error_count": len(errors), "warning_count": len(warnings),
        "errors": errors[:20], "warnings": warnings[:20],
        "greeks_stats": greeks_stats, "vol_surface_stats": vol_surface_stats,
    }


# ── Expected intervals per source ──

_SOURCE_INTERVALS = {
    ("binance", "mark_price"): 30,
    ("deribit", "mark_price"): 30,
    ("binance", "spot_price"): 1,
    ("deribit", "options_ticker"): 1,
    ("deribit", "options_greeks"): 5,
    ("binance", "basis"): 10,
    ("deribit", "vol_surface"): 10,
    ("binance", "funding_rate"): 28800,
    ("deribit", "funding_rate"): 28800,
    ("hyperliquid", "funding_rate"): 28800,
    ("fred", "risk_free_rate"): 86400,
    ("deribit", "margin_params"): 86400,
}

_MULTI_INSTRUMENT = {("deribit", "options_greeks")}

_PRICE_COLS = ["mark_price", "price", "mid_price", "bid_price", "ask_price", "underlying_price"]


# ── Date boundary check (runbook 3.2) ──

def check_date_boundaries(
    sources: Dict[Tuple[str, str, str], List[str]],
) -> List[str]:
    """Verify each file only contains data from its expected date (UTC)."""
    issues: List[str] = []
    for (exchange, data_type, symbol), files in sorted(sources.items()):
        for fp in sorted(files):
            fname = os.path.basename(fp)
            parts = os.path.splitext(fname)[0].rsplit("_", 1)
            if len(parts) != 2 or len(parts[1]) != 10 or parts[1][4] != "-":
                continue
            expected_date = parts[1]
            try:
                pf = pq.ParquetFile(fp)
                if "timestamp" not in pf.schema_arrow.names or pf.metadata.num_rows == 0:
                    continue
                first_rg = pf.read_row_group(0, columns=["timestamp"])
                first_ts = first_rg.column("timestamp")[0].as_py()
                last_idx = pf.metadata.num_row_groups - 1
                last_rg = pf.read_row_group(last_idx, columns=["timestamp"])
                last_ts = last_rg.column("timestamp")[-1].as_py()
                from datetime import datetime, timezone
                for ts_val in [first_ts, last_ts]:
                    actual = datetime.fromtimestamp(ts_val / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                    if actual != expected_date:
                        rel = f"{exchange}/{data_type}/{fname}"
                        issues.append(f"DATE MISMATCH: {rel} expected {expected_date}, got {actual}")
                        break
            except Exception:
                pass
    return issues


# ── Value range sanity (runbook 3.3) ──

def check_value_ranges(
    sources: Dict[Tuple[str, str, str], List[str]],
) -> List[str]:
    """Check price positivity, IV bounds, extreme funding, negative spread."""
    issues: List[str] = []
    for (exchange, data_type, symbol), files in sorted(sources.items()):
        for fp in sorted(files):
            try:
                pf = pq.ParquetFile(fp)
                rows = pf.metadata.num_rows
                if rows == 0:
                    continue
                cols = set(pf.schema_arrow.names)

                # Decide which columns to read
                price_cols = [c for c in _PRICE_COLS if c in cols]
                iv_col = ["iv"] if "iv" in cols else []
                fr_col = ["funding_rate"] if "funding_rate" in cols else []
                sp_col = ["spread"] if "spread" in cols else []
                read_cols = price_cols + iv_col + fr_col + sp_col
                if not read_cols:
                    continue

                # Sample for large files
                if rows > _LARGE_FILE_ROWS:
                    rg_count = pf.metadata.num_row_groups
                    sample_rgs = list({0, rg_count // 2, rg_count - 1})
                    chunks = []
                    for ri in sample_rgs:
                        try:
                            chunks.append(pf.read_row_group(ri, columns=read_cols).to_pandas())
                        except Exception:
                            continue
                    if not chunks:
                        continue
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pf.read(columns=read_cols).to_pandas()

                rel = f"{exchange}/{data_type}/{os.path.basename(fp)}"
                suffix = " (sampled)" if rows > _LARGE_FILE_ROWS else ""

                for col in price_cols:
                    bad = (df[col] <= 0).sum()
                    if bad:
                        issues.append(f"NON-POSITIVE {col}: {rel} -> {bad} rows{suffix}")

                if "iv" in df.columns:
                    bad = ((df["iv"] <= 0) | (df["iv"] > 1000)).sum()
                    if bad:
                        issues.append(f"IV OUT OF RANGE: {rel} -> {bad} rows{suffix}")

                if "funding_rate" in df.columns:
                    bad = (df["funding_rate"].abs() > 0.1).sum()
                    if bad:
                        issues.append(f"EXTREME FUNDING: {rel} -> {bad} rows >10pct{suffix}")

                if "spread" in df.columns:
                    bad = (df["spread"] < 0).sum()
                    if bad:
                        issues.append(f"NEGATIVE SPREAD: {rel} -> {bad} rows{suffix}")

                del df
            except Exception:
                pass
    return issues


# ── Cross-source consistency (runbook 4.1) ──

def check_cross_source_consistency(
    sources: Dict[Tuple[str, str, str], List[str]],
) -> List[str]:
    """Compare Binance vs Deribit mark prices for BTC and ETH."""
    results: List[str] = []
    for base_sym in ["BTC", "ETH"]:
        binance_key = ("binance", "mark_price", f"{base_sym}USDT")
        deribit_key = ("deribit", "mark_price", f"{base_sym}-PERPETUAL")
        b_files = sources.get(binance_key, [])
        d_files = sources.get(deribit_key, [])
        if not b_files or not d_files:
            continue

        # Group files by date
        from collections import defaultdict as dd
        b_by_date: Dict[str, str] = {}
        d_by_date: Dict[str, str] = {}
        for f in b_files:
            parts = os.path.splitext(os.path.basename(f))[0].rsplit("_", 1)
            if len(parts) == 2:
                b_by_date[parts[1]] = f
        for f in d_files:
            parts = os.path.splitext(os.path.basename(f))[0].rsplit("_", 1)
            if len(parts) == 2:
                d_by_date[parts[1]] = f

        common_dates = sorted(set(b_by_date.keys()) & set(d_by_date.keys()))
        for date in common_dates:
            try:
                b_df = pq.read_table(b_by_date[date]).to_pandas()
                d_df = pq.read_table(d_by_date[date]).to_pandas()
                if len(b_df) == 0 or len(d_df) == 0:
                    continue
                b_df["ts_min"] = (b_df["timestamp"] // 60000) * 60000
                d_df["ts_min"] = (d_df["timestamp"] // 60000) * 60000
                merged = b_df[["ts_min", "mark_price"]].merge(
                    d_df[["ts_min", "mark_price"]], on="ts_min", suffixes=("_b", "_d"),
                )
                if len(merged) == 0:
                    continue
                merged["pct_diff"] = (
                    (merged["mark_price_b"] - merged["mark_price_d"]).abs()
                    / merged["mark_price_b"]
                    * 100
                )
                results.append(
                    f"{base_sym} {date}: {len(merged)} samples, "
                    f"mean_diff={merged['pct_diff'].mean():.3f}%, "
                    f"max_diff={merged['pct_diff'].max():.3f}%"
                )
                del b_df, d_df, merged
            except Exception:
                pass
    return results


# ── Greeks coverage per day (runbook 4.2) ──

def check_greeks_coverage(
    sources: Dict[Tuple[str, str, str], List[str]],
) -> List[str]:
    """Report instruments, rows, and IV coverage per greeks file."""
    results: List[str] = []
    for (exchange, data_type, symbol), files in sorted(sources.items()):
        if data_type != "options_greeks":
            continue
        for fp in sorted(files):
            try:
                pf = pq.ParquetFile(fp)
                rows = pf.metadata.num_rows
                if rows == 0:
                    continue
                cols = set(pf.schema_arrow.names)
                read_cols = []
                if "instrument_name" in cols:
                    read_cols.append("instrument_name")
                if "iv" in cols:
                    read_cols.append("iv")
                if not read_cols:
                    continue

                # Sample for large files
                if rows > _LARGE_FILE_ROWS:
                    rg_count = pf.metadata.num_row_groups
                    sample_rgs = list({0, rg_count // 2, rg_count - 1})
                    chunks = []
                    for ri in sample_rgs:
                        try:
                            chunks.append(pf.read_row_group(ri, columns=read_cols).to_pandas())
                        except Exception:
                            continue
                    if not chunks:
                        continue
                    df = pd.concat(chunks, ignore_index=True)
                    suffix = " (sampled)"
                else:
                    df = pf.read(columns=read_cols).to_pandas()
                    suffix = ""

                n_instruments = df["instrument_name"].nunique() if "instrument_name" in df.columns else 0
                iv_pct = (1 - df["iv"].isna().mean()) * 100 if "iv" in df.columns else 100.0
                results.append(
                    f"{os.path.basename(fp)}: {n_instruments} instruments, "
                    f"{len(df):,} rows{suffix}, iv_coverage={iv_pct:.1f}%"
                )
                del df
            except Exception:
                pass
    return results

def _sep(c="=", w=90): print(c * w)
def _sec(t): print(f"\n{'='*90}\n  {t}\n{'='*90}")
def _sub(t): print(f"\n  -- {t} --\n")
def _ic(s): return {"OK":"[OK]","WARNING":"[!!]","ERROR":"[XX]","MISSING":"[--]"}.get(s, f"[{s}]")


# ── Main report ──

def generate_report(data_dir: str, log_path: Optional[str], strategies: Dict) -> bool:
    all_ok = True
    counts = {"OK": 0, "WARNING": 0, "ERROR": 0, "MISSING": 0}

    # 1. Log
    _sec("1. COLLECTOR LOG ANALYSIS")
    if log_path:
        lr = verify_log_file(log_path)
        print(f"  Log: {log_path}")
        print(f"  Status: {lr['status']}  |  Errors: {lr.get('error_count',0)}  |  Warnings: {lr.get('warning_count',0)}")
        for label, items in [("Errors", lr.get("errors")), ("Warnings", lr.get("warnings"))]:
            if items:
                print(f"\n  {label} (first 10):")
                for x in items[:10]:
                    print(f"    {x}")
        if lr.get("greeks_stats"):
            print(f"\n  Greeks cycles: {dict(sorted(lr['greeks_stats'].items()))}")
        if lr.get("vol_surface_stats"):
            print(f"  Vol surface builds: {dict(sorted(lr['vol_surface_stats'].items()))}")
        if lr["status"] != "OK":
            all_ok = False
    else:
        print("  No log file specified")

    # 2. Auto-discover & verify V3 sources
    _sec("2. V3.0 DATA SOURCE VERIFICATION (auto-discovered)")
    sources = discover_v3_sources(data_dir)

    if not sources:
        print("  No V3.0 Hive-partitioned data found")
    else:
        for key in sorted(sources.keys()):
            exchange, data_type, symbol = key
            files = sources[key]
            fc_list = [check_parquet_file(f) for f in sorted(files)]
            total_rows = sum(fc.rows for fc in fc_list)
            sc = SourceCheck(exchange=exchange, data_type=data_type, symbol=symbol,
                             files=fc_list, total_rows=total_rows)

            counts[sc.status] = counts.get(sc.status, 0) + 1
            if sc.status != "OK":
                all_ok = False

            print(f"  {_ic(sc.status)} {exchange}/{data_type}/{symbol}: {total_rows:,} rows, {len(fc_list)} files")
            for fc in fc_list:
                fn = os.path.basename(fc.path)
                ts_info = f"{fc.ts_min} ~ {fc.ts_max}" if fc.ts_min else "N/A"
                print(f"       {fn}: {fc.rows:,} rows, {fc.size_kb:.1f}KB | {ts_info}")
                for w in fc.null_warnings:
                    print(f"         WARN: {w}")
                for e in fc.errors:
                    print(f"         ERR:  {e}")

    # 3. Time continuity (streaming)
    _sec("3. TIME CONTINUITY CHECKS")
    for (exchange, data_type, symbol), files in sorted(sources.items()):
        key = (exchange, data_type)
        interval = _SOURCE_INTERVALS.get(key)
        if interval is None:
            continue
        issues = check_continuity(files, interval)
        label = f"{exchange}/{data_type}/{symbol} (~{interval}s)"
        if not issues:
            print(f"  [OK] {label}")
        else:
            for iss in issues:
                print(f"  [!!] {label}: {iss}")
                all_ok = False

    # 4. Coverage (streaming)
    _sec("4. DATA COVERAGE ANALYSIS")
    for (exchange, data_type, symbol), files in sorted(sources.items()):
        key = (exchange, data_type)
        interval = _SOURCE_INTERVALS.get(key)
        if interval is None:
            continue
        multi = key in _MULTI_INSTRUMENT
        rows, hours, start, end, coverage = compute_coverage(files, interval, multi)
        if rows == 0:
            print(f"  [--] {exchange}/{data_type}/{symbol}: NO DATA")
            continue
        cov_st = "OK" if coverage > 50 else "LOW"
        print(
            f"  {_ic(cov_st)} {exchange}/{data_type}/{symbol}: "
            f"{rows:,} rows | {hours:.1f}h | coverage={coverage:.1f}%"
            + (" (multi-instrument)" if multi else "")
        )

    # 5. Date boundary check (runbook 3.2)
    _sec("5. DATE BOUNDARY CHECK")
    date_issues = check_date_boundaries(sources)
    if date_issues:
        for iss in date_issues:
            print(f"  [!!] {iss}")
            all_ok = False
    else:
        print("  [OK] All files have timestamps matching their filename dates")

    # 6. Value range sanity (runbook 3.3)
    _sec("6. VALUE RANGE SANITY")
    range_issues = check_value_ranges(sources)
    if range_issues:
        for iss in range_issues:
            print(f"  [!!] {iss}")
            all_ok = False
    else:
        print("  [OK] All value ranges within acceptable bounds")

    # 7. Cross-source consistency (runbook 4.1/4.2)
    _sec("7. CROSS-SOURCE CONSISTENCY")
    _sub("7a. Mark Price Consistency (Binance vs Deribit)")
    consistency = check_cross_source_consistency(sources)
    if consistency:
        for r in consistency:
            print(f"  {r}")
    else:
        print("  [--] No overlapping mark_price data for cross-exchange comparison")

    _sub("7b. Greeks Coverage per Day")
    greeks_cov = check_greeks_coverage(sources)
    if greeks_cov:
        for r in greeks_cov:
            print(f"  {r}")
    else:
        print("  [--] No options_greeks data found")

    # 8. Strategy completeness
    _sec("8. STRATEGY COMPLETENESS MATRIX")
    for strat in strategies.values():
        print(f"\n  [{strat.priority}] {strat.display_name} ({strat.name})")
        all_met = True
        for req in strat.requirements:
            for exchange in req.exchanges:
                for symbol in req.symbols:
                    key = (exchange, req.data_type, symbol)
                    if key in sources:
                        total = sum(check_parquet_file(f).rows for f in sources[key])
                        ic = _ic("OK")
                        info = f"{total:,} rows"
                    else:
                        ic = _ic("MISSING")
                        info = "N/A"
                        all_met = False
                    print(f"    {ic} {req.data_type:20s} | {exchange:12s} | {symbol:18s} | {info}")
        print(f"    Strategy status: {'ALL MET' if all_met else 'INCOMPLETE'}")

    # 9. Summary
    _sec("9. SUMMARY")
    total = sum(counts.values())
    print(f"  V3.0 sources: {total}")
    for st in ["OK", "WARNING", "ERROR", "MISSING"]:
        if counts.get(st, 0) > 0:
            print(f"    {st}: {counts[st]}")
    print(f"\n  {'ALL CHECKS PASSED' if all_ok else 'ISSUES FOUND — see details above'}")
    _sep()

    # 10. All V3 parquet files (metadata only)
    _sec("10. ALL V3 PARQUET FILES")
    for (exchange, data_type, symbol), files in sorted(sources.items()):
        for fp in sorted(files):
            rel = os.path.relpath(fp, data_dir).replace("\\", "/")
            try:
                n = pq.ParquetFile(fp).metadata.num_rows
                sz = os.path.getsize(fp) / 1024
                print(f"  {rel} ({n:,} rows, {sz:.1f}KB)")
            except Exception:
                print(f"  {rel} (READ ERROR)")

    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="V3.0 Data Collection Verification")
    parser.add_argument("--data-dir", default="./deribit-options-data-collector/data")
    parser.add_argument("--log", default="./deribit-options-data-collector/logs/collector.log")
    parser.add_argument("--no-log", action="store_true")
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    log_path = None if args.no_log else os.path.abspath(args.log)

    print(f"\n{'='*90}\n  V3.0 Data Collection Verification Report\n  Data: {data_dir}")
    if log_path:
        print(f"  Log:  {log_path}")
    print(f"{'='*90}")

    strategies = get_all_strategies()
    all_ok = generate_report(data_dir, log_path, strategies)
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
