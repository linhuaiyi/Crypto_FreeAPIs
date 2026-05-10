"""
V3.0 Data Collection Verification Script.

Auto-discovers all Hive-partitioned parquet data under data/{exchange}/{data_type}/date=*/{symbol}.parquet,
verifies completeness/continuity/correctness via streaming (no OOM).
Also reports legacy V1/V2 flat parquet files.

Usage:
    python scripts/verify_collected_data.py
    python scripts/verify_collected_data.py --data-dir ./data
    python scripts/verify_collected_data.py --log logs/collector.log
"""

from __future__ import annotations

import argparse
import glob
import os
import re
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
_V3_DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})$")


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

    if "date=1970" in filepath.replace("\\", "/"):
        errors.append("date=1970-01-01 partition — epoch-zero timestamp")

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
    """Discover {exchange}/{data_type}/date=*/{symbol}.parquet → files mapping."""
    sources: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for exchange in _V3_EXCHANGES:
        base = os.path.join(data_dir, exchange)
        if not os.path.isdir(base):
            continue
        for date_dir in sorted(glob.glob(os.path.join(base, "*", "date=*"))):
            if not _V3_DATE_RE.search(os.path.basename(date_dir)):
                continue
            data_type = os.path.basename(os.path.dirname(date_dir))
            for pf in glob.glob(os.path.join(date_dir, "*.parquet")):
                symbol = os.path.splitext(os.path.basename(pf))[0]
                sources[(exchange, data_type, symbol)].append(pf)
    return sources


# ── Log analysis ──

def verify_log_file(log_path: str) -> Dict:
    if not os.path.exists(log_path):
        return {"status": "NO_LOG", "errors": [], "warnings": []}

    errors, warnings = [], []
    greeks_stats: Dict[str, int] = {}
    vol_surface_stats: Dict[str, int] = {}
    date_1970_flushes: List[str] = []

    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "WARNING" in line:
                warnings.append(line)
            elif "ERROR" in line or ("Exception" in line and "Traceback" not in line):
                errors.append(line)
            elif "wrote" in line and "rows" in line:
                if "date=1970" in line:
                    date_1970_flushes.append(line)
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
        "date_1970_flushes": date_1970_flushes,
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


# ── Report helpers ──

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
        if lr.get("date_1970_flushes"):
            print(f"\n  ANOMALY: {len(lr['date_1970_flushes'])} epoch-zero flushes!")
            for x in lr["date_1970_flushes"][:5]:
                print(f"    {x}")
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
                dt = os.path.basename(os.path.dirname(fc.path))
                fn = os.path.basename(fc.path)
                ts_info = f"{fc.ts_min} ~ {fc.ts_max}" if fc.ts_min else "N/A"
                print(f"       {dt}/{fn}: {fc.rows:,} rows, {fc.size_kb:.1f}KB | {ts_info}")
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

    # 5. Strategy completeness
    _sec("5. STRATEGY COMPLETENESS MATRIX")
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

    # 6. Summary
    _sec("6. SUMMARY")
    total = sum(counts.values())
    print(f"  V3.0 sources: {total}")
    for st in ["OK", "WARNING", "ERROR", "MISSING"]:
        if counts.get(st, 0) > 0:
            print(f"    {st}: {counts[st]}")
    print(f"\n  {'ALL CHECKS PASSED' if all_ok else 'ISSUES FOUND — see details above'}")
    _sep()

    # 7. All V3 parquet files (metadata only)
    _sec("7. ALL V3 PARQUET FILES")
    for (exchange, data_type, symbol), files in sorted(sources.items()):
        for fp in sorted(files):
            rel = os.path.relpath(fp, data_dir).replace("\\", "/")
            try:
                n = pq.ParquetFile(fp).metadata.num_rows
                sz = os.path.getsize(fp) / 1024
                print(f"  {rel} ({n:,} rows, {sz:.1f}KB)")
            except Exception:
                print(f"  {rel} (READ ERROR)")

    # 8. Legacy V1/V2 data
    _sec("8. LEGACY V1/V2 DATA (not verified)")
    legacy_dirs = []
    for d in sorted(os.listdir(data_dir)):
        full = os.path.join(data_dir, d)
        if not os.path.isdir(full):
            continue
        # V3.0 dirs have Hive subdirs, legacy dirs have flat .parquet files
        has_hive = any(_V3_DATE_RE.search(os.path.basename(sd))
                       for sd in glob.glob(os.path.join(full, "*", "date=*")))
        flat_files = glob.glob(os.path.join(full, "*.parquet"))
        if flat_files:
            total_kb = sum(os.path.getsize(f) for f in flat_files) / 1024
            legacy_dirs.append((d, len(flat_files), total_kb))
    if legacy_dirs:
        for name, count, kb in legacy_dirs:
            print(f"  {name}/: {count} files, {kb:.0f}KB total")
    else:
        print("  (none)")

    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="V3.0 Data Collection Verification")
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--log", default="./logs/collector.log")
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
