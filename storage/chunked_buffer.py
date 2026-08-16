"""
Chunked buffer for batched Parquet writes with date-based file naming.

Memory-bounded accumulation with triple flush triggers:
  - Row count threshold (default: 100,000 rows)
  - Memory size threshold (default: 200 MB)
  - Time interval threshold (default: 5 minutes)

Output path convention:
  data/{exchange}/{data_type}/{symbol}_{date}.parquet

Memory strategy
---------------
Two layers of defense against allocator-retained RSS growth (previously the
process climbed to 2-3 GB resident after ~1 day even though live Python
objects stayed flat at ~16 MB, because the C-level allocators for pandas /
pyarrow / numpy never returned freed pages to the OS):

1. Buffered DataFrames are kept as a **list of small frames** per key and only
   concatenated once at flush time. This avoids the O(n^2) reallocation churn
   of re-concatenating the whole growing buffer on every append.

2. The on-disk Parquet file is written **append-only via a long-lived
   ``pyarrow.ParquetWriter``** kept open per (key, date). Each flush writes one
   row group of just the new rows. The previous implementation re-read the
   entire growing day-file, concatenated, de-duplicated and re-wrote it on
   EVERY flush — for options_greeks that meant re-reading a ~100 MB file 12
   times/hour, allocating and freeing hundreds of MB of pyarrow buffers that
   the allocator then retained. Append-only eliminates that re-read entirely
   in steady state.

Restart-replay safety: on the first flush for a (key, date) after process
start, if the target file already exists (restart mid-day), it is read ONCE,
merged with the new rows, de-duplicated, and used to open the writer. Thus the
expensive read happens at most once per restart per key, never per flush.

Crash safety: each ParquetWriter is closed on UTC date rollover and on
graceful shutdown (``stop_periodic_flush`` / ``close_all_writers``), which
finalizes the Parquet footer. A hard crash (SIGKILL / OOM) may leave the
current day-file without a footer; the next start quarantines it (``.corrupted``
suffix) and starts fresh, exactly as before. A SIGTERM handler in the launcher
should call ``close_all_writers`` for a clean exit.

After each flush we also call ``malloc_trim(0)`` + ``gc.collect()`` so glibc
returns whatever fastbin pages it can to the OS.
"""

import gc
import os
import time
import ctypes
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils import get_logger

logger = get_logger("ChunkedBuffer")

# ZSTD compression balances ratio and CPU cost on NVMe
_PARQUET_COMPRESSION = "zstd"


def _malloc_trim() -> None:
    """Return freed glibc fastbin pages to the OS. No-op if unavailable."""
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


class ChunkedBuffer:
    """Memory-bounded buffered writer with flat date-named Parquet output."""

    def __init__(
        self,
        data_dir: str = "./data",
        max_rows: int = 100_000,
        max_memory_mb: int = 200,
        flush_interval_sec: int = 300,
    ) -> None:
        self.data_dir = data_dir
        self.max_rows = max_rows
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.flush_interval_sec = flush_interval_sec

        # Per-key list of small DataFrames; concatenated only at flush.
        self._buffers: Dict[str, List[pd.DataFrame]] = {}
        # Incremental running totals (updated under _lock) for cheap flush checks.
        self._rows: Dict[str, int] = {}
        self._mem: Dict[str, int] = {}
        self._last_flush_time: Dict[str, float] = {}
        # Long-lived ParquetWriter per (key|date). Append-only in steady state.
        self._writers: Dict[str, "pq.ParquetWriter"] = {}
        self._writer_dates: Dict[str, str] = {}  # wk -> date_str (for rollover)
        self._lock = threading.RLock()
        self._timer: Optional[threading.Timer] = None

        os.makedirs(data_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API (unchanged contract)
    # ------------------------------------------------------------------

    def append(
        self,
        exchange: str,
        data_type: str,
        symbol: str,
        df: pd.DataFrame,
    ) -> int:
        """Append rows to buffer. Returns rows buffered, or rows written on flush.

        The frame is appended to a per-key list (O(1)); the expensive concat
        only happens once, at flush time.
        """
        if df.empty:
            return 0

        key = f"{exchange}/{data_type}/{symbol}"

        with self._lock:
            self._buffers.setdefault(key, []).append(df)
            rows = self._rows.get(key, 0) + len(df)
            mem = self._mem.get(key, 0) + int(df.memory_usage(deep=True).sum())
            self._rows[key] = rows
            self._mem[key] = mem

            if self._should_flush(key, rows, mem):
                flushed = self.flush(key)
                return flushed

            return rows

    def flush(self, key: Optional[str] = None) -> int:
        """Flush buffer(s) to Parquet. Returns total rows written."""
        total = 0

        with self._lock:
            keys_to_flush = [key] if key else list(self._buffers.keys())

            for k in keys_to_flush:
                parts = self._buffers.pop(k, None)
                self._rows.pop(k, None)
                self._mem.pop(k, None)
                self._last_flush_time.pop(k, None)
                if not parts:
                    continue
                df = pd.concat(parts, ignore_index=True)
                total += self._write_parquet(k, df)

        # Release allocator pages after the flush batch.
        gc.collect()
        _malloc_trim()

        return total

    def flush_all(self) -> int:
        """Flush all buffered data. Returns total rows written."""
        return self.flush()

    def get_buffer_stats(self) -> Dict[str, Dict[str, int]]:
        """Return current buffer sizes for monitoring."""
        with self._lock:
            stats = {}
            for key in self._buffers:
                stats[key] = {
                    "rows": self._rows.get(key, 0),
                    "memory_mb": round(self._mem.get(key, 0) / (1024 * 1024), 2),
                }
            return stats

    def start_periodic_flush(self) -> None:
        """Start a background timer for periodic flush."""
        self._schedule_timer()

    def stop_periodic_flush(self) -> None:
        """Stop the periodic flush timer, flush remaining data, finalize files."""
        if self._timer:
            self._timer.cancel()
            self._timer = None
        self.flush_all()
        self.close_all_writers()

    def close_all_writers(self) -> None:
        """Finalize every open ParquetWriter (writes the footer).

        Snapshots + clears the writer dict under the lock, then closes each
        writer OUTSIDE the lock. A slow ``writer.close()`` — or the periodic
        flush timer holding the lock — must not block graceful shutdown, which
        previously timed out and got force-killed, losing the day's
        open-writer data.
        """
        with self._lock:
            writers = list(self._writers.items())
            self._writers.clear()
            self._writer_dates.clear()
        for wk, writer in writers:
            try:
                writer.close()
                logger.info(f"finalized parquet writer {wk}")
            except Exception as e:
                logger.warning(f"writer close failed for {wk}: {e}")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _should_flush(self, key: str, rows: int, mem: int) -> bool:
        if rows >= self.max_rows:
            logger.info(f"[{key}] Flush triggered: row count {rows} >= {self.max_rows}")
            return True

        if mem >= self.max_memory_bytes:
            logger.info(f"[{key}] Flush triggered: memory {mem / 1024 / 1024:.1f} MB >= {self.max_memory_bytes / 1024 / 1024:.0f} MB")
            return True

        last = self._last_flush_time.get(key, 0)
        if last > 0 and (time.time() - last) >= self.flush_interval_sec:
            logger.info(f"[{key}] Flush triggered: interval >= {self.flush_interval_sec}s")
            return True

        return False

    def _write_parquet(self, key: str, df: pd.DataFrame) -> int:
        """Write DataFrame to date-named Parquet files, append-only.

        For each UTC date present in ``df``:
          - open a long-lived ParquetWriter on first encounter (handling the
            restart-replay merge if the file pre-exists), then
          - append subsequent flushes as new row groups.

        Output: data_dir/{exchange}/{data_type}/{symbol}_{date}.parquet
        """
        if df.empty:
            return 0

        if "timestamp" not in df.columns:
            logger.warning(f"[{key}] No timestamp column, cannot partition by date")
            return 0

        df = df.copy()
        df["_date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime("%Y-%m-%d")

        parts = key.split("/")
        # parts = [exchange, data_type, symbol]
        symbol = parts[-1]
        base_parts = parts[:-1]  # [exchange, data_type]
        dir_path = os.path.join(self.data_dir, *base_parts)
        os.makedirs(dir_path, exist_ok=True)

        total_written = 0
        for date_str, group in df.groupby("_date"):
            group = group.drop(columns=["_date"]).reset_index(drop=True)
            if group.empty:
                continue

            file_path = os.path.join(dir_path, f"{symbol}_{date_str}.parquet")
            wk = f"{key}|{date_str}"
            writer = self._writers.get(wk)

            if writer is None:
                # First write for this (key, date) in this process.
                group = self._merge_existing_if_any(key, file_path, group)
                table = pa.Table.from_pandas(group, preserve_index=False)
                # Open a fresh writer (truncates). The merged `group` already
                # incorporates any pre-existing on-disk rows, so truncation
                # loses nothing.
                writer = pq.ParquetWriter(
                    file_path, table.schema, compression=_PARQUET_COMPRESSION,
                )
                writer.write_table(table)
                self._writers[wk] = writer
                self._writer_dates[wk] = str(date_str)
                logger.info(
                    f"[{key}] {date_str}: opened writer, wrote {len(group)} rows "
                    f"to {file_path}"
                )
            else:
                table = _align_to_schema(group, writer.schema)
                writer.write_table(table)
                logger.info(
                    f"[{key}] {date_str}: appended {len(group)} rows to {file_path}"
                )

            total_written += len(group)

        self._last_flush_time[key] = time.time()
        return total_written

    def _merge_existing_if_any(
        self, key: str, file_path: str, group: pd.DataFrame,
    ) -> pd.DataFrame:
        """On restart mid-day, merge an existing day-file with new rows once.

        De-duplicates on timestamp (+ exchange/symbol/instrument_name when
        present), keeping the latest version of each row. If the existing file
        is corrupt (e.g. previous run was killed before the footer was
        written), quarantine it and start fresh.
        """
        if not os.path.exists(file_path):
            return group

        try:
            existing = pd.read_parquet(file_path)
        except Exception as read_err:
            quarantined = f"{file_path}.corrupted.{int(time.time())}"
            try:
                os.rename(file_path, quarantined)
                logger.warning(
                    f"[{key}] quarantined corrupted {file_path} -> "
                    f"{quarantined} (reason: {read_err})"
                )
            except OSError as rename_err:
                logger.error(
                    f"[{key}] failed to quarantine {file_path}: "
                    f"{rename_err}; starting fresh anyway"
                )
            return group

        if existing.empty:
            return group

        merged = pd.concat([existing, group], ignore_index=True)
        dedup_cols = ["timestamp"]
        for col in ["exchange", "symbol", "instrument_name"]:
            if col in merged.columns:
                dedup_cols.append(col)
        merged = merged.drop_duplicates(subset=dedup_cols, keep="last")
        merged = merged.sort_values("timestamp").reset_index(drop=True)
        # We have the merged data in memory; remove the stale file so the new
        # writer opens clean.
        try:
            os.remove(file_path)
        except OSError:
            pass
        return merged

    def _close_past_date_writers(self) -> None:
        """Finalize writers for dates that have rolled over (date < today UTC)."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._lock:
            for wk, date_str in list(self._writer_dates.items()):
                if date_str < today:
                    writer = self._writers.pop(wk, None)
                    self._writer_dates.pop(wk, None)
                    if writer is not None:
                        try:
                            writer.close()
                            logger.info(f"rolled over, finalized writer {wk}")
                        except Exception as e:
                            logger.warning(f"writer close failed for {wk}: {e}")

    def _schedule_timer(self) -> None:
        """Schedule periodic flush timer."""
        if self._timer:
            self._timer.cancel()

        self._timer = threading.Timer(self.flush_interval_sec, self._on_timer)
        self._timer.daemon = True
        self._timer.start()

    def _on_timer(self) -> None:
        """Timer callback: flush stale buffers, roll over past-date writers."""
        now = time.time()
        with self._lock:
            stale_keys = []
            for key, last_time in self._last_flush_time.items():
                if key in self._buffers and (now - last_time) >= self.flush_interval_sec:
                    stale_keys.append(key)
            # Also flush buffers that have never been flushed but have data
            for key in self._buffers:
                if key not in self._last_flush_time:
                    stale_keys.append(key)

        for key in stale_keys:
            self.flush(key)

        # Finalize yesterday's files shortly after UTC midnight.
        self._close_past_date_writers()

        self._schedule_timer()


def _align_to_schema(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Coerce ``df`` into a pyarrow Table matching ``schema`` exactly.

    Selects/reorders columns to ``schema.names`` (inserting all-null columns
    for any that are missing in ``df``), then casts each column to its target
    type. This keeps a long-lived ParquetWriter happy when individual flushes
    vary slightly in column set/order — the schema is frozen by the first
    write and later flushes are aligned to it.
    """
    names = list(schema.names)
    aligned = df.reindex(columns=names)
    table = pa.Table.from_pandas(aligned, preserve_index=False)
    # Cast field-by-field to the frozen writer schema.
    for i, field in enumerate(schema):
        col = table.column(i)
        if col.type != field.type:
            table = table.set_column(i, field.name, col.cast(field.type))
    return table
