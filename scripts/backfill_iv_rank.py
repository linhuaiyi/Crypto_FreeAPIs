"""Back-fill the ``iv_rank`` column in historical vol_surface parquet files.

The collector wrote ``iv_rank=50.0`` for every tick before the
``IVRankTracker`` integration. This script recomputes the column for each
tick using ONLY the daily representative IVs of days strictly before the
tick's own UTC day — no look-ahead.

Procedure per symbol:
  1. Scan ``{data_dir}/{exchange}/{data_type}/{symbol}_*.parquet``.
  2. Read each past day's parquet, compute that day's representative IV
     (last valid ``atm_iv`` by ``timestamp``).
  3. For each past day D, build ``history`` = representative IVs of all
     days with date < D (capped at ``--lookback-days``), then recompute
     every tick's ``iv_rank`` via ``compute_rank_batch``.
  4. Atomic-write the updated parquet (temp file + ``os.replace``).

Idempotent: running twice yields identical files. Today's file is
skipped because the collector is still writing it.

Usage:
    python scripts/backfill_iv_rank.py                 # dry-run
    python scripts/backfill_iv_rank.py --execute       # write
    python scripts/backfill_iv_rank.py --symbols BTC,ETH
    python scripts/backfill_iv_rank.py --lookback 180
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from processors.iv_rank import compute_rank_batch, daily_representative_iv
from utils import get_logger

logger = get_logger("BackfillIVRank")

_DEFAULT_DATA_DIR = "./deribit-options-data-collector/data"
_COMPRESSION = "zstd"


def _scan_files(
    data_dir: str, exchange: str, data_type: str, symbol: str,
) -> List[Tuple[str, str]]:
    """Return ``[(date_str, file_path)]`` sorted by date; today excluded."""
    pattern = os.path.join(data_dir, exchange, data_type, f"{symbol}_*.parquet")
    files = sorted(glob.glob(pattern))
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    out: List[Tuple[str, str]] = []
    prefix = f"{symbol}_"
    suffix = ".parquet"
    for fp in files:
        fname = os.path.basename(fp)
        if not fname.startswith(prefix) or not fname.endswith(suffix):
            continue
        date_str = fname[len(prefix):-len(suffix)]
        if date_str >= today_str:
            continue
        out.append((date_str, fp))
    return out


def _compute_daily_representative_ivs(
    files: List[Tuple[str, str]],
) -> Dict[str, float]:
    """Read each day's parquet (timestamp + atm_iv) -> ``{date: daily_iv}``."""
    daily: Dict[str, float] = {}
    for date_str, fp in files:
        try:
            df = pd.read_parquet(fp, columns=["timestamp", "atm_iv"])
        except Exception as e:
            logger.warning("skip %s: %s", fp, e)
            continue
        iv = daily_representative_iv(df)
        if iv > 0:
            daily[date_str] = iv
    return daily


def _history_for_day(
    target_date: str,
    daily_ivs: Dict[str, float],
    lookback_days: int,
) -> pd.Series:
    """Return IVs of all days BEFORE ``target_date`` (no look-ahead).

    Capped at the most recent ``lookback_days`` entries.
    """
    prior = [(d, iv) for d, iv in daily_ivs.items() if d < target_date]
    prior.sort(key=lambda x: x[0])
    if len(prior) > lookback_days:
        prior = prior[-lookback_days:]
    return pd.Series([iv for _, iv in prior])


def backfill_symbol(
    data_dir: str,
    symbol: str,
    exchange: str = "deribit",
    data_type: str = "vol_surface",
    lookback_days: int = 252,
    execute: bool = False,
) -> Dict[str, object]:
    """Backfill iv_rank for one symbol. Returns stats dict."""
    files = _scan_files(data_dir, exchange, data_type, symbol)
    if not files:
        logger.warning("[%s] no parquet files found", symbol)
        return {"symbol": symbol, "files": 0, "ticks": 0, "changed": 0,
                "max_baseline": 0, "skipped": 0}

    daily_ivs = _compute_daily_representative_ivs(files)
    logger.info(
        "[%s] %d/%d days have valid daily representative IV",
        symbol, len(daily_ivs), len(files),
    )

    total_ticks = 0
    total_changed = 0
    skipped = 0
    max_baseline = 0

    for date_str, fp in files:
        try:
            df = pd.read_parquet(fp)
        except Exception as e:
            logger.warning("[%s] skip %s: %s", symbol, fp, e)
            skipped += 1
            continue

        if "atm_iv" not in df.columns or "timestamp" not in df.columns:
            logger.warning("[%s] %s missing required columns", symbol, fp)
            skipped += 1
            continue

        history = _history_for_day(date_str, daily_ivs, lookback_days)
        max_baseline = max(max_baseline, len(history))

        new_ranks = compute_rank_batch(df["atm_iv"].values, history)
        new_ranks = new_ranks.astype(np.float32)

        if "iv_rank" in df.columns:
            old = df["iv_rank"].to_numpy(dtype=np.float64)
            changed = int((old != new_ranks.astype(np.float64)).sum())
        else:
            changed = len(df)

        total_ticks += len(df)
        total_changed += changed

        action = "would update" if not execute else "updated"
        logger.info(
            "[%s] %s: %s %d/%d ticks (baseline=%d days, rank range %.1f~%.1f)",
            symbol, date_str, action, changed, len(df), len(history),
            float(new_ranks.min()), float(new_ranks.max()),
        )

        if execute:
            df = df.copy()
            df["iv_rank"] = new_ranks
            tmp_fp = fp + ".tmp"
            df.to_parquet(tmp_fp, index=False, compression=_COMPRESSION)
            os.replace(tmp_fp, fp)

    return {
        "symbol": symbol,
        "files": len(files),
        "ticks": total_ticks,
        "changed": total_changed,
        "max_baseline": max_baseline,
        "skipped": skipped,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Back-fill iv_rank in historical vol_surface parquet",
    )
    parser.add_argument(
        "--data-dir",
        default=_DEFAULT_DATA_DIR,
        help=f"data directory (default: {_DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--symbols",
        default="BTC,ETH",
        help="comma-separated symbols (default: BTC,ETH)",
    )
    parser.add_argument(
        "--exchange", default="deribit", help="exchange folder (default: deribit)",
    )
    parser.add_argument(
        "--data-type", default="vol_surface",
        help="data_type folder (default: vol_surface)",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=252,
        help="rolling window size in days (default: 252)",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="actually write files (default: dry-run)",
    )
    args = parser.parse_args()

    abs_data_dir = os.path.abspath(args.data_dir)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    mode = "EXECUTE" if args.execute else "DRY-RUN"

    print("=" * 60)
    print(f"  Backfill iv_rank  [{mode}]")
    print("=" * 60)
    print(f"  data_dir:    {abs_data_dir}")
    print(f"  symbols:     {symbols}")
    print(f"  lookback:    {args.lookback_days} days")
    print(f"  exchange:    {args.exchange}")
    print(f"  data_type:   {args.data_type}")
    print("=" * 60)

    results = []
    for sym in symbols:
        logger.info("── processing %s ──", sym)
        r = backfill_symbol(
            data_dir=abs_data_dir,
            symbol=sym,
            exchange=args.exchange,
            data_type=args.data_type,
            lookback_days=args.lookback_days,
            execute=args.execute,
        )
        results.append(r)

    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    print(f"  {'symbol':<8} {'files':>6} {'ticks':>10} {'changed':>10} "
          f"{'baseline':>10} {'skipped':>8}")
    for r in results:
        print(f"  {r['symbol']:<8} {r['files']:>6} {r['ticks']:>10,} "
              f"{r['changed']:>10,} {r['max_baseline']:>10} {r['skipped']:>8}")
    print("=" * 60)
    if not args.execute:
        print("  Dry-run only. Re-run with --execute to apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
