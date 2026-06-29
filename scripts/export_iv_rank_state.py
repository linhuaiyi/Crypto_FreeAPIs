"""Export IV rank history from local parquet to a state JSON file.

Used to seed a remote server's ``IVRankTracker`` when the server has no
local history (e.g., after ``pull_data.sh`` cleared its ``data/`` dir).
The exported JSON is loaded by ``IVRankTracker`` on bootstrap and the
server then continues appending new days on its own.

Procedure (one-time seed, or re-run whenever server state is lost):

    python scripts/export_iv_rank_state.py
    bash scripts/push_iv_rank_state.sh         # scp to server
    ssh ... "systemctl restart deribit-collector"  # restart to load

The state file is also written/read by the local collector (if running)
and by ``IVRankTracker`` in the live trading process, so this script is
the canonical way to (re)build it from raw parquet.

Usage:
    python scripts/export_iv_rank_state.py
    python scripts/export_iv_rank_state.py --symbols BTC,ETH
    python scripts/export_iv_rank_state.py --output-dir ./state
"""

from __future__ import annotations

import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from processors.iv_rank import IVRankTracker
from utils import get_logger

logger = get_logger("ExportIVRankState")

_DEFAULT_DATA_DIR = "./deribit-options-data-collector/data"
_DEFAULT_OUTPUT_DIR = "./deribit-options-data-collector/state"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export IV rank history to a state JSON",
    )
    parser.add_argument(
        "--data-dir",
        default=_DEFAULT_DATA_DIR,
        help=f"source data directory (default: {_DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--symbols",
        default="BTC,ETH",
        help="comma-separated symbols (default: BTC,ETH)",
    )
    parser.add_argument(
        "--output-dir",
        default=_DEFAULT_OUTPUT_DIR,
        help=f"output directory (default: {_DEFAULT_OUTPUT_DIR})",
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
    args = parser.parse_args()

    abs_data_dir = os.path.abspath(args.data_dir)
    abs_output_dir = os.path.abspath(args.output_dir)
    os.makedirs(abs_output_dir, exist_ok=True)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    print("=" * 60)
    print("  Export IV rank state")
    print("=" * 60)
    print(f"  data_dir:    {abs_data_dir}")
    print(f"  output_dir:  {abs_output_dir}")
    print(f"  symbols:     {symbols}")
    print(f"  lookback:    {args.lookback_days} days")
    print("=" * 60)

    for sym in symbols:
        state_file = os.path.join(abs_output_dir, f"iv_rank_{sym}.json")
        tracker = IVRankTracker(
            sym,
            lookback_days=args.lookback_days,
            state_file=state_file,
        )
        n = tracker.bootstrap_from_parquet(
            abs_data_dir, exchange=args.exchange, data_type=args.data_type,
        )
        tracker.save_state()
        size = os.path.getsize(state_file)
        print(f"  {sym}: {n} days -> {state_file} ({size} bytes)")
        logger.info("[%s] exported %d days to %s", sym, n, state_file)

    print("=" * 60)
    print("  Next: bash scripts/push_iv_rank_state.sh")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
