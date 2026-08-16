"""Clean up pre-T4-v2 data: delete rows with timestamp < CUTOFF.

CUTOFF = 2026-07-16 05:19:48 UTC (T4 v2 restart moment).

For each parquet file in the 14 streams:
  - If all rows before CUTOFF: delete file
  - If file spans CUTOFF: filter and rewrite atomically (tmp + rename)
  - If all rows after CUTOFF: leave untouched

Also deletes any *.corrupted.* quarantine artifacts.
"""
from __future__ import annotations

import glob
import os
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

CUTOFF_MS = int(
    datetime(2026, 7, 16, 5, 19, 48, tzinfo=timezone.utc).timestamp() * 1000
)
DATA_DIR = "/opt/Crypto_FreeAPIs/deribit-options-data-collector/data"

STREAMS = [
    "binance/spot_price",
    "binance/mark_price",
    "binance/basis",
    "binance/funding_rate",
    "deribit/dvol",
    "deribit/funding_rate",
    "deribit/index_price",
    "deribit/margin_params",
    "deribit/mark_price",
    "deribit/options_greeks",
    "deribit/options_ticker",
    "deribit/vol_surface",
    "hyperliquid/funding_rate",
    "fred/risk_free_rate",
]


def classify_file(path: str) -> tuple[str, int, int, int]:
    """Return (action, total_rows, before_cutoff, after_cutoff).

    action ∈ {'delete', 'trim', 'keep'}.
    """
    t = pq.read_table(path, columns=["timestamp"])
    ts = t.column("timestamp").to_pylist()
    n = len(ts)
    if n == 0:
        return ("delete", 0, 0, 0)
    before = sum(1 for x in ts if x < CUTOFF_MS)
    after = n - before
    if after == 0:
        return ("delete", n, n, 0)
    if before == 0:
        return ("keep", n, 0, n)
    return ("trim", n, before, after)


def trim_file(path: str) -> int:
    """Rewrite file keeping only rows >= CUTOFF. Returns rows kept."""
    full = pq.read_table(path)
    if "timestamp" not in full.column_names:
        raise RuntimeError(f"no timestamp column in {path}")
    ts = full.column("timestamp").to_pylist()
    mask = pa.array([x >= CUTOFF_MS for x in ts])
    filtered = full.filter(mask)
    tmp = path + ".tmp.cleanup"
    pq.write_table(filtered, tmp, compression="snappy")
    os.replace(tmp, path)
    return filtered.num_rows


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    print(f"=== cleanup_pre_t4 (dry_run={dry_run}, cutoff_ms={CUTOFF_MS}) ===")
    print(f"cutoff UTC: {datetime.fromtimestamp(CUTOFF_MS/1000, tz=timezone.utc).isoformat()}")

    summary = {
        "delete": {"files": 0, "rows": 0},
        "trim": {"files": 0, "rows_before": 0, "rows_after": 0},
        "keep": {"files": 0, "rows": 0},
        "error": {"files": 0},
    }

    for stream in STREAMS:
        d = f"{DATA_DIR}/{stream}"
        if not os.path.isdir(d):
            print(f"  [MISSING DIR] {stream}")
            continue
        files = sorted(glob.glob(f"{d}/*.parquet"))
        for f in files:
            try:
                action, total, before, after = classify_file(f)
                label = os.path.relpath(f, DATA_DIR)
                if action == "delete":
                    summary["delete"]["files"] += 1
                    summary["delete"]["rows"] += total
                    print(f"  [DEL] {label}  rows={total:,}")
                    if not dry_run:
                        os.remove(f)
                elif action == "trim":
                    summary["trim"]["files"] += 1
                    summary["trim"]["rows_before"] += before
                    summary["trim"]["rows_after"] += after
                    print(f"  [TRIM] {label}  keep {after:,}/{total:,}")
                    if not dry_run:
                        trim_file(f)
                else:
                    summary["keep"]["files"] += 1
                    summary["keep"]["rows"] += total
                    print(f"  [KEEP] {label}  rows={total:,}")
            except Exception as e:
                summary["error"]["files"] += 1
                print(f"  [ERR] {f}: {type(e).__name__}: {e}")

    # Sweep corrupted artifacts
    print("\n=== corrupted artifacts ===")
    n_corrupt = 0
    for cf in glob.glob(f"{DATA_DIR}/**/*.corrupted.*", recursive=True):
        n_corrupt += 1
        sz = os.path.getsize(cf)
        print(f"  [DEL-CORRUPT] {os.path.relpath(cf, DATA_DIR)} size={sz:,}B")
        if not dry_run:
            os.remove(cf)
    if n_corrupt == 0:
        print("  (none)")

    print("\n=== summary ===")
    print(f"  delete: {summary['delete']['files']} files, {summary['delete']['rows']:,} rows")
    print(f"  trim:   {summary['trim']['files']} files, "
          f"{summary['trim']['rows_before']:,} → {summary['trim']['rows_after']:,} rows")
    print(f"  keep:   {summary['keep']['files']} files, {summary['keep']['rows']:,} rows")
    print(f"  error:  {summary['error']['files']} files")
    print(f"  corrupted artifacts removed: {n_corrupt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
