"""
Cross-platform data pruning utility.

Scans DATA_DIR for Hive-partitioned directories (date=YYYY-MM-DD)
and removes partitions older than KEEP_DAYS.

Replaces the Bash-only prune_cloud_data.sh with a pathlib-based
implementation that runs on Windows, macOS, and Linux.

Usage:
    python scripts/prune_data.py                     # dry-run
    python scripts/prune_data.py --execute           # actually delete
    python scripts/prune_data.py --keep-days 7       # custom retention
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

DATE_DIR_PATTERN = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")


def find_date_partitions(data_dir: Path) -> List[Path]:
    """Find all date=YYYY-MM-DD directories under data_dir."""
    partitions: List[Path] = []
    for root, dirs, _files in os.walk(data_dir):
        for d in dirs:
            if DATE_DIR_PATTERN.match(d):
                partitions.append(Path(root) / d)
    return partitions


def prune_data(
    data_dir: str,
    keep_days: int = 14,
    execute: bool = False,
) -> Dict[str, object]:
    """Prune old Hive date partitions.

    Returns a dict with keys:
        scanned, deleted_dirs, deleted_files, freed_bytes, skipped, details
    Or 'error' if data_dir does not exist.
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        return {"error": f"Data directory not found: {data_dir}"}

    cutoff = datetime.now().date() - timedelta(days=keep_days)
    partitions = find_date_partitions(data_path)

    stats: Dict[str, object] = {
        "scanned": len(partitions),
        "deleted_dirs": 0,
        "deleted_files": 0,
        "freed_bytes": 0,
        "skipped": 0,
        "details": [],
    }

    for part_dir in sorted(partitions):
        match = DATE_DIR_PATTERN.match(part_dir.name)
        if not match:
            continue

        try:
            part_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue

        if part_date >= cutoff:
            stats["skipped"] = stats.get("skipped", 0) + 1  # type: ignore[assignment]
            continue

        dir_files = [f for f in part_dir.rglob("*") if f.is_file()]
        dir_size = sum(f.stat().st_size for f in dir_files)
        file_count = len(dir_files)

        rel = part_dir.relative_to(data_path)
        details_list = stats.get("details", [])
        details_list.append({  # type: ignore[union-attr]
            "path": str(rel),
            "date": match.group(1),
            "files": file_count,
            "size_mb": round(dir_size / 1024 / 1024, 2),
            "action": "DELETE" if execute else "WOULD_DELETE",
        })

        if execute:
            shutil.rmtree(part_dir)
            stats["deleted_dirs"] = stats.get("deleted_dirs", 0) + 1  # type: ignore[assignment]
            stats["deleted_files"] = stats.get("deleted_files", 0) + file_count  # type: ignore[assignment]
            stats["freed_bytes"] = stats.get("freed_bytes", 0) + dir_size  # type: ignore[assignment]

    # Clean up empty parent directories (bottom-up)
    if execute:
        for root, dirs, files in os.walk(data_path, topdown=False):
            root_path = Path(root)
            if root_path == data_path:
                continue
            if not dirs and not files:
                try:
                    root_path.rmdir()
                except OSError:
                    pass

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cross-platform Hive partition data pruning"
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("DATA_DIR", "./data"),
        help="Root data directory (default: $DATA_DIR or ./data)",
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=14,
        help="Retain partitions newer than this many days (default: 14)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete files (default: dry-run preview)",
    )
    args = parser.parse_args()

    stats = prune_data(args.data_dir, args.keep_days, args.execute)

    if "error" in stats:
        print(f"ERROR: {stats['error']}")
        sys.exit(1)

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    cutoff = datetime.now().date() - timedelta(days=args.keep_days)

    print(f"=== Data Prune ({mode}) ===")
    print(f"Data dir:   {args.data_dir}")
    print(f"Keep days:  {args.keep_days}")
    print(f"Cutoff:     {cutoff.isoformat()}")
    print(f"Scanned:    {stats['scanned']} date partitions")
    print(f"Skipped:    {stats['skipped']} (within retention)")
    print()

    details = stats.get("details", [])
    if details:
        print(f"{'Action':<14} {'Date':<12} {'Files':>6} {'Size':>10} Path")
        print("-" * 70)
        for d in details:
            print(
                f"{d['action']:<14} {d['date']:<12} {d['files']:>6} "
                f"{d['size_mb']:>8.1f}MB {d['path']}"
            )

    if args.execute:
        print(
            f"\nDeleted: {stats['deleted_dirs']} dirs, "
            f"{stats['deleted_files']} files, "
            f"{stats['freed_bytes'] / 1024 / 1024:.1f} MB freed"
        )
    else:
        print("\n(Dry-run: no files deleted. Use --execute to apply.)")


if __name__ == "__main__":
    main()
