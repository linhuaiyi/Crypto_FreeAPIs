"""Post-restart verification: scan every parquet file, report per-stream stats."""
from __future__ import annotations

import glob
import os
import sys
from datetime import datetime, timezone

import pyarrow.parquet as pq

CUTOFF_MS = int(
    datetime(2026, 7, 16, 5, 19, 48, tzinfo=timezone.utc).timestamp() * 1000
)
DATA_DIR = sys.argv[1] if len(sys.argv) > 1 else "."


def main() -> int:
    files = sorted(glob.glob(f"{DATA_DIR}/**/*.parquet", recursive=True))
    by_stream: dict[str, list[str]] = {}
    for f in files:
        rel = os.path.relpath(f, DATA_DIR).replace(os.sep, "/")
        parts = rel.split("/")
        if len(parts) <= 1:
            continue
        stream = "/".join(parts[:-1])
        by_stream.setdefault(stream, []).append(f)

    header = (
        f"{'stream':<45} {'files':>5} {'rows':>10} {'pre-cut':>8} "
        f"{'first_ts':<27} {'last_ts':<27}"
    )
    print(header)
    print("-" * len(header))

    total_rows_all = 0
    total_precut_all = 0
    for stream in sorted(by_stream):
        fs = by_stream[stream]
        total_rows = 0
        pre_cut = 0
        mn = mx = None
        for f in fs:
            try:
                t = pq.read_table(f, columns=["timestamp"])
                ts = t.column("timestamp").to_pylist()
            except Exception as e:
                print(f"ERR {f}: {e}")
                continue
            total_rows += len(ts)
            for x in ts:
                if x < CUTOFF_MS:
                    pre_cut += 1
                if mn is None or x < mn:
                    mn = x
                if mx is None or x > mx:
                    mx = x
        first = (
            datetime.fromtimestamp(mn / 1000, tz=timezone.utc).isoformat()
            if mn
            else "-"
        )
        last = (
            datetime.fromtimestamp(mx / 1000, tz=timezone.utc).isoformat()
            if mx
            else "-"
        )
        flag = "  !!PRECUT" if pre_cut > 0 else ""
        print(
            f"{stream:<45} {len(fs):>5} {total_rows:>10,} {pre_cut:>8,} "
            f"{first:<27} {last:<27}{flag}"
        )
        total_rows_all += total_rows
        total_precut_all += pre_cut

    print("-" * len(header))
    print(f"TOTAL: {len(files)} files, {total_rows_all:,} rows, {total_precut_all:,} pre-cutoff")
    return 0


if __name__ == "__main__":
    sys.exit(main())
