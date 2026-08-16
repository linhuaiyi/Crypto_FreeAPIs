"""
tracemalloc-based memory diagnostics for the Deribit options collector.

Distinguishes two failure modes that BOTH show up as climbing RSS:

  1. Python-level leak: tracemalloc-tracked size climbs together with RSS.
     -> delta-vs-previous snapshot pinpoints the exact leaking file:line.
  2. Allocator retention (freed Python/C memory not returned to the OS):
     tracemalloc-tracked size stays FLAT while RSS keeps climbing.
     -> the fix is churn reduction (e.g. append-only writes) + malloc_trim,
        NOT a Python-object hunt.

Usage (in launch.py, near startup, after logging is configured):

    from utils.mem_trace import start as start_mem_trace
    start_mem_trace(interval_sec=600)   # snapshot every 10 min

Output: ``logs/mem_trace.log``. Each block records:
  - wall-clock + RSS (from /proc/self/status) + tracemalloc current/peak
  - ``RSS - tracked`` gap (positive => allocator-retained / C-level)
  - TOP-N by CURRENT size (lineno)
  - TOP-N by DELTA vs previous snapshot (lineno)  <- the leak fingerprint

Overhead: tracemalloc adds ~1.5x allocation cost; at a 10-minute snapshot
cadence the runtime impact is negligible. nframe=25 keeps tracebacks cheap.
"""
from __future__ import annotations

import os
import threading
import time
import tracemalloc
from typing import Optional

try:
    from utils import get_logger
    logger = get_logger("MemTrace")
except Exception:  # standalone-testable without the project's logger
    import logging
    logger = logging.getLogger("MemTrace")

try:
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except Exception:
    _HAS_PSUTIL = False

_started = False
_lock = threading.Lock()


def start(interval_sec: int = 600,
          top_n: int = 25,
          nframe: int = 25,
          log_path: Optional[str] = None,
          warmup_sec: int = 30) -> None:
    """Start tracemalloc + a daemon thread that snapshots every interval_sec.

    Idempotent: a second call is a no-op. Safe to call from main thread at
    startup. The worker thread is a daemon so it never blocks shutdown.
    """
    global _started
    with _lock:
        if _started:
            logger.warning("mem_trace already started, ignoring")
            return
        if not tracemalloc.is_tracing():
            tracemalloc.start(nframe)
        _started = True

    if log_path is None:
        log_path = os.path.join("logs", "mem_trace.log")
    parent = os.path.dirname(log_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    def _loop() -> None:
        prev = None
        time.sleep(warmup_sec)          # let startup allocations settle
        while True:
            try:
                cur = tracemalloc.take_snapshot()
                _write(cur, prev, top_n, log_path)
                prev = cur
            except Exception as e:  # never let the diagnostic kill the process
                logger.warning(f"mem_trace snapshot failed: {e}")
            time.sleep(interval_sec)

    t = threading.Thread(target=_loop, name="MemTrace", daemon=True)
    t.start()
    logger.info(
        f"mem_trace started (interval={interval_sec}s top={top_n} "
        f"nframe={nframe} -> {log_path})"
    )


def _rss_kb() -> Optional[int]:
    """Current process RSS in KiB. Prefers psutil, falls back to /proc."""
    if _HAS_PSUTIL:
        try:
            return int(psutil.Process().memory_info().rss / 1024)
        except Exception:
            pass
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        return None
    return None


def snapshot_now(top_n: int = 25,
                 log_path: Optional[str] = None,
                 prev=None):
    """Take one snapshot and write it. Exposed for tests and ad-hoc probes.

    Returns the snapshot so callers can pass it back as ``prev`` next time.
    """
    if not tracemalloc.is_tracing():
        tracemalloc.start(25)
    cur = tracemalloc.take_snapshot()
    _write(cur, prev, top_n, log_path or os.path.join("logs", "mem_trace.log"))
    return cur


def _write(cur, prev, top_n: int, log_path: str) -> None:
    rss = _rss_kb()
    cur_tracked, peak_tracked = tracemalloc.get_traced_memory()

    lines: list[str] = []
    lines.append("=" * 78)
    lines.append(f"snapshot @ {time.strftime('%Y-%m-%d %H:%M:%S')} (local)")
    if rss is not None:
        lines.append(f"RSS             : {rss:>12} KB")
    else:
        lines.append("RSS             :          n/a")
    lines.append(f"tracemalloc cur : {cur_tracked / 1024:>12.1f} KB")
    lines.append(f"tracemalloc peak: {peak_tracked / 1024:>12.1f} KB")
    if rss is not None:
        gap = rss - cur_tracked / 1024
        lines.append(
            f"RSS - tracked   : {gap:>12.1f} KB   "
            f"(large positive => allocator-retained / C-level, not Python objects)"
        )
    lines.append("")

    lines.append(f"--- TOP {top_n} by CURRENT size ---")
    for stat in cur.statistics("lineno")[:top_n]:
        lines.append(f"  {stat.size / 1024:>9.1f} KB  {stat.count:>6d}x  "
                     f"{_fmt(stat.traceback)}")

    if prev is not None:
        lines.append("")
        lines.append(f"--- TOP {top_n} by DELTA vs previous (size_diff) ---")
        diffs = cur.compare_to(prev, "lineno")
        shown = 0
        for stat in diffs:
            if stat.size_diff == 0 and stat.count_diff == 0:
                continue
            sign = "+" if stat.size_diff >= 0 else ""
            lines.append(
                f"  {sign}{stat.size_diff / 1024:>8.1f} KB  "
                f"(count {stat.count_diff:+d})  {_fmt(stat.traceback)}"
            )
            shown += 1
            if shown >= top_n:
                break

    lines.append("")
    with open(log_path, "a") as f:
        f.write("\n".join(lines) + "\n")


def _fmt(tb) -> str:
    """Compact one-line render of a traceback (uses its top frame)."""
    frames = tb
    if not frames:
        return "<unknown>"
    f0 = frames[0]
    return f"{f0.filename}:{f0.lineno}"
